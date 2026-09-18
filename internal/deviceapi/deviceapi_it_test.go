package deviceapi

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// 打真实 PG/NATS：desired 合并事务 + 审计消费者落表。
func TestPGStoreAndAuditIT(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := config.MustPG(ctx)
	defer pool.Close()
	st := NewPGStore(pool)
	sn := fmt.Sprintf("ITAPI%d", time.Now().UnixNano()%1_000_000)
	defer pool.Exec(context.Background(), `DELETE FROM iot_shard.shadow_desired WHERE sn=$1`, sn)

	if _, v, err := st.GetDesired(ctx, sn); err != nil || v != 0 {
		t.Fatalf("empty desired v=%d err=%v", v, err)
	}
	v, d, err := st.MergeDesired(ctx, sn, json.RawMessage(`{"a":1}`))
	if err != nil || v != 1 {
		t.Fatalf("first merge v=%d err=%v", v, err)
	}
	v, d, err = st.MergeDesired(ctx, sn, json.RawMessage(`{"b":"x","a":2}`))
	if err != nil || v != 2 {
		t.Fatalf("second merge v=%d err=%v", v, err)
	}
	var m map[string]any
	_ = json.Unmarshal(d, &m)
	if m["a"] != float64(2) || m["b"] != "x" {
		t.Fatalf("merged=%s", d)
	}

	nc, js := config.MustNATS()
	defer nc.Close()
	if err := config.EnsureStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	cmdID := NewUUIDv4()
	defer pool.Exec(context.Background(), `DELETE FROM iot_shard.cmd_audit WHERE cmd_id=$1`, cmdID)
	rec := AuditRecord{CmdID: cmdID, SN: sn, Action: "pause", Params: json.RawMessage(`{"k":1}`), Operator: "it", Source: "test", Result: "dispatched", CreatedAt: time.Now().UTC()}
	body, _ := json.Marshal(rec)
	if _, err := js.Publish(ctx, envelope.SubjectCmdAudit, body); err != nil {
		t.Fatal(err)
	}
	cctx, ccancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- RunAuditConsumer(cctx, js, st) }()
	var result string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(ctx, `SELECT result FROM iot_shard.cmd_audit WHERE cmd_id=$1`, cmdID).Scan(&result); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	ccancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if result != "dispatched" {
		t.Fatalf("audit row not found / result=%q", result)
	}
	if err := st.MarkAcked(ctx, cmdID, "ok"); err != nil {
		t.Fatal(err)
	}
	var acked *time.Time
	if err := pool.QueryRow(ctx, `SELECT acked_at FROM iot_shard.cmd_audit WHERE cmd_id=$1`, cmdID).Scan(&acked); err != nil || acked == nil {
		t.Fatalf("acked_at=%v err=%v", acked, err)
	}
}
