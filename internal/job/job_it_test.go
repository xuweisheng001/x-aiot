package job

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/cellmap"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
)

// 集成（IOT_IT）：影子 optin=true 的 JOB_START/JOB_DONE 经 JetStream 落 job_record 且 duration 正确；
// optin=false 不落库；先 202 暂存后合并；desired optin=false 后 purge 删除。
func TestIntegration_JobFlow(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()
	nc, js := config.MustNATS()
	defer nc.Close()
	if err := config.EnsureStreams(ctx, js); err != nil {
		t.Fatal(err)
	}

	suffix := time.Now().UnixNano()
	snYes, snNo := fmt.Sprintf("IT_JOB_Y_%d", suffix), fmt.Sprintf("IT_JOB_N_%d", suffix)
	cleanup := func() {
		for _, sn := range []string{snYes, snNo} {
			_, _ = db.Exec(context.Background(), `DELETE FROM iot_shard.job_record WHERE sn=$1`, sn)
			_, _ = db.Exec(context.Background(), `DELETE FROM iot_shard.job_feedback_pending WHERE sn=$1`, sn)
			_, _ = db.Exec(context.Background(), `DELETE FROM iot_shard.shadow_desired WHERE sn=$1`, sn)
			_ = rdb.Del(context.Background(), shadow.Key(sn)).Err()
		}
	}
	cleanup()
	defer cleanup()
	if err := rdb.HSet(ctx, shadow.Key(snYes), OptInField, "true").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, shadow.Key(snNo), OptInField, "false").Err(); err != nil {
		t.Fatal(err)
	}

	reader := ShadowReaderFunc(func(ctx context.Context, sn string) (map[string]string, error) { return shadow.Read(ctx, rdb, sn) })
	svc := NewService(&PGStore{Pool: db}, reader, NewMetrics())
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	// 用一次性 durable：常驻 job-svc 可能正在跑，同名 durable 会把测试消息抢走
	durable := fmt.Sprintf("job-it-%d", time.Now().UnixNano())
	defer func() { _ = js.DeleteConsumer(context.Background(), envelope.StreamUp, durable) }()
	go func() { _ = RunConsumerNamed(cctx, js, svc, durable) }()

	publish := func(sn, code string, f JobFields, ts int64) {
		p, _ := json.Marshal(EventPayload{Seq: ts, Ts: ts, Code: code, JobFields: f})
		e := envelope.Envelope{PK: "LM_S1", SN: sn, Kind: envelope.KindEvent, Seq: ts, RecvTs: ts, Payload: p}
		if _, err := js.Publish(ctx, envelope.Subject(envelope.KindEvent, cellmap.CellOf(sn, config.Cells())), e.Marshal()); err != nil {
			t.Fatal(err)
		}
	}
	waitFor := func(desc string, cond func() bool) {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("timeout waiting: %s", desc)
	}
	hash := strings.Repeat("cd", 32)

	// 1 先标记 → 202 暂存（job_record 未到）
	jobA := "aaaaaaaa-1111-4222-8333-444444444444"
	h := Routes(svc, "it")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobA+"/feedback", strings.NewReader(`{"rating":"good"}`))
	req.Header.Set(HeaderDeviceSN, snYes)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("pending feedback status %d: %s", rec.Code, rec.Body.String())
	}

	// 2 optin=true：START → DONE 30s 后
	start := time.Now().Add(-time.Minute).UnixMilli()
	fA := JobFields{JobID: jobA, MaterialID: "BASSWOOD_3MM", ParamsHash: hash}
	publish(snYes, CodeStart, fA, start)
	publish(snYes, CodeDone, fA, start+30_000)
	var outcome string
	var duration int
	waitFor("job_record done", func() bool {
		err := db.QueryRow(ctx, `SELECT outcome, duration_s FROM iot_shard.job_record WHERE job_id=$1 AND outcome IS NOT NULL`, jobA).Scan(&outcome, &duration)
		return err == nil
	})
	if outcome != OutcomeDone || duration != 30 {
		t.Fatalf("outcome=%s duration=%d", outcome, duration)
	}
	// 暂存已合并
	var rating string
	if err := db.QueryRow(ctx, `SELECT rating FROM iot_shard.job_feedback WHERE job_id=$1`, jobA).Scan(&rating); err != nil || rating != "good" {
		t.Fatalf("pending not merged: %v %q", err, rating)
	}
	var pend int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.job_feedback_pending WHERE job_id=$1`, jobA).Scan(&pend)
	if pend != 0 {
		t.Fatal("pending row should be deleted")
	}

	// 3 optin=false 的 SN 不落库（发一条后等消费者处理过 dropped 计数）
	jobB := "bbbbbbbb-1111-4222-8333-444444444444"
	before := svc.M.Get("dropped_optin_false")
	publish(snNo, CodeStart, JobFields{JobID: jobB}, start)
	waitFor("dropped_optin_false", func() bool { return svc.M.Get("dropped_optin_false") > before })
	var n int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.job_record WHERE sn=$1`, snNo).Scan(&n)
	if n != 0 {
		t.Fatalf("optin=false must not be recorded, got %d rows", n)
	}

	// 4 撤回：desired optin=false → purge 删除 snYes 全部数据
	if _, err := db.Exec(ctx, `INSERT INTO iot_shard.shadow_desired(sn, desired, version) VALUES ($1, '{"job_feedback_optin": false}'::jsonb, 1)
		ON CONFLICT (sn) DO UPDATE SET desired = shadow_desired.desired || EXCLUDED.desired`, snYes); err != nil {
		t.Fatal(err)
	}
	purged, err := svc.PurgeOnce(ctx)
	if err != nil || purged < 1 {
		t.Fatalf("purge: n=%d err=%v", purged, err)
	}
	_ = db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.job_record WHERE sn=$1`, snYes).Scan(&n)
	if n != 0 {
		t.Fatalf("records remain after purge: %d", n)
	}
	_ = db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.job_feedback WHERE job_id=$1`, jobA).Scan(&n)
	if n != 0 {
		t.Fatal("feedback should cascade-delete")
	}
	t.Logf("metrics: started=%d finished=%d merged=%d dropped_false=%d purged=%d",
		svc.M.Get("records_started"), svc.M.Get("records_finished"), svc.M.Get("pending_merged"), svc.M.Get("dropped_optin_false"), svc.M.Get("purged_rows"))
}
