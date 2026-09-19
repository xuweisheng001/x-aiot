package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// 打真实 NATS/Redis/TDengine：用 cell 99 与独立 durable，避免干扰运行中的 pipeline。
func TestWorkerEndToEnd(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := config.MustNATS()
	defer nc.Close()
	if err := config.EnsureStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	rdb := config.MustRedis()
	defer rdb.Close()
	td := tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())

	const cell = 99
	sn := fmt.Sprintf("ITSN%d", time.Now().UnixNano()%1_000_000)
	cfg := ConsumerConfig(cell)
	cfg.Durable = "pipeline-it-99"
	// 只消费本次发布的消息：IOT_UP 保留 72h，上一次 IT 留在 cell 99 的消息会让计数翻倍
	cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	cons, err := js.CreateOrUpdateConsumer(ctx, envelope.StreamUp, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer js.DeleteConsumer(context.Background(), envelope.StreamUp, cfg.Durable)

	pub := func(kind envelope.Kind, seq int64, payload string) {
		e := envelope.Envelope{PK: "LM_S1", SN: sn, Kind: kind, Seq: seq, RecvTs: time.Now().UnixMilli(), Payload: []byte(payload)}
		if _, err := js.Publish(ctx, envelope.Subject(kind, cell), e.Marshal()); err != nil {
			t.Fatal(err)
		}
	}
	base := time.Now().UnixMilli()
	pub(envelope.KindTelemetry, 1, fmt.Sprintf(`{"seq":1,"ts":%d,"work_state":1,"progress":10,"module_model":"LM40","job_feedback_optin":true}`, base)) // v1.1 属性透传
	pub(envelope.KindTelemetry, 2, fmt.Sprintf(`{"seq":2,"ts":%d,"work_state":2,"progress":20}`, base+1))
	pub(envelope.KindTelemetry, 2, fmt.Sprintf(`{"seq":2,"ts":%d,"work_state":2,"progress":20}`, base+1)) // dup
	pub(envelope.KindEvent, 3, fmt.Sprintf(`{"seq":3,"ts":%d,"code":"FLAME_DETECTED","msg":"x"}`, base+2))
	pub(envelope.KindCmdAck, 4, `{"seq":4,"cmd_id":"it-cmd-1","result":"ok"}`)
	pub(envelope.KindOTAProgress, 5, `{"seq":5,"phase":"success","pct":100}`)
	if _, err := js.Publish(ctx, envelope.Subject(envelope.KindTelemetry, cell), []byte("garbage")); err != nil {
		t.Fatal(err)
	}

	m := NewMetrics(AllMetricNames...)
	w := NewWorker(cell, cons, rdb, td, m, Config{BatchSize: 500, Window: 100 * time.Millisecond})
	wctx, wcancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && m.Get(MConsumed) < 7 {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // 让窗口刷出
	wcancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if m.Get(MConsumed) != 7 || m.Get(MPoison) != 1 || m.Get(MDup) != 1 || m.Get(MRowsWritten) != 2 ||
		m.Get(MEvents) != 1 || m.Get(MCmdAcks) != 1 || m.Get(MOTAAcked) != 1 || m.Get(MNak) != 0 {
		t.Fatalf("metrics: consumed=%d poison=%d dup=%d rows=%d events=%d cmdacks=%d ota=%d nak=%d",
			m.Get(MConsumed), m.Get(MPoison), m.Get(MDup), m.Get(MRowsWritten), m.Get(MEvents), m.Get(MCmdAcks), m.Get(MOTAAcked), m.Get(MNak))
	}
	rep, err := shadow.Read(ctx, rdb, sn)
	if err != nil || rep["seq"] != "2" || rep["progress"] != "20" || rep["last_event_code"] != "FLAME_DETECTED" {
		t.Fatalf("shadow=%v err=%v", rep, err)
	}
	// v1.1：属性只在 seq=1 帧携带，批内最新是 seq=2，仍必须进影子
	if rep["module_model"] != "LM40" || rep["job_feedback_optin"] != "1" && rep["job_feedback_optin"] != "true" {
		t.Fatalf("v1.1 extras missing in shadow: %v", rep)
	}
	if v, err := rdb.Get(ctx, "cmdres:it-cmd-1").Result(); err != nil || v == "" {
		t.Fatalf("cmdres: %q %v", v, err)
	}
	info, err := cons.Info(ctx)
	if err != nil || info.NumAckPending != 0 || info.NumPending != 0 {
		t.Fatalf("consumer not drained: %+v err=%v", info, err)
	}
	_ = jetstream.AckExplicitPolicy
}
