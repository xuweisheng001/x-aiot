package alarm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// 集成：事件 → open → notified，重复事件被 squelch，重复 ack 409，close 需 acked。
func TestIntegration_StateMachine(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()
	svc := NewService(db, rdb, NewMetrics())

	sn := fmt.Sprintf("IT_ALARM_%d", time.Now().UnixNano())
	_ = rdb.Del(ctx, SquelchKey(sn, "FLAME_DETECTED")).Err()
	defer db.Exec(context.Background(), `DELETE FROM iot_shard.alarm WHERE sn=$1`, sn)

	payload, _ := json.Marshal(map[string]any{"seq": 1, "ts": time.Now().UnixMilli(), "code": "FLAME_DETECTED"})
	env := &envelope.Envelope{PK: "LM_S1", SN: sn, Kind: envelope.KindEvent, Seq: 1, RecvTs: time.Now().UnixMilli(), Payload: payload}

	out, err := svc.HandleEvent(ctx, env)
	if err != nil || out != OutcomeNotified {
		t.Fatalf("first event: out=%v err=%v", out, err)
	}
	out, err = svc.HandleEvent(ctx, env)
	if err != nil || out != OutcomeSquelched {
		t.Fatalf("duplicate event: out=%v err=%v", out, err)
	}
	list, err := svc.List(ctx, "notified", 10)
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	for _, a := range list {
		if a.SN == sn {
			id = a.ID
		}
	}
	if id == 0 {
		t.Fatal("alarm not found in notified list")
	}
	if err := svc.Close(ctx, id, "tester"); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("close before ack should be illegal, got %v", err)
	}
	if err := svc.Ack(ctx, id); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := svc.Ack(ctx, id); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("second ack should be illegal, got %v", err)
	}
	if svc.M.Get("alarms_acked") != 1 {
		t.Fatalf("duplicate ack double counted: %d", svc.M.Get("alarms_acked"))
	}
	if err := svc.Close(ctx, id, "tester"); err != nil {
		t.Fatalf("close: %v", err)
	}
}

type fakeTD struct{ res *tdengine.Result }

func (f *fakeTD) Query(_ context.Context, _ string) (*tdengine.Result, error) { return f.res, nil }

// 集成：TDengine 有 FLAME 事件、PG 无告警 → 对账补录 1 条（绕过 squelch）；第二轮 missing=0。
func TestIntegration_Reconcile(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()
	svc := NewService(db, rdb, NewMetrics())
	sn := fmt.Sprintf("IT_RECON_%d", time.Now().UnixNano())
	defer db.Exec(context.Background(), `DELETE FROM iot_shard.alarm WHERE sn=$1`, sn)
	// 预置 squelch 键，证明补录绕过 squelch
	if err := rdb.Set(ctx, SquelchKey(sn, "FLAME_DETECTED"), 1, SquelchTTL).Err(); err != nil {
		t.Fatal(err)
	}
	evTs := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	td := &fakeTD{res: &tdengine.Result{
		ColumnMeta: [][]any{{"ts"}, {"sn"}, {"code"}},
		Data:       [][]any{{evTs.Format(time.RFC3339Nano), sn, "FLAME_DETECTED"}},
	}}
	r := NewReconciler(td, svc, 30*time.Minute, time.Second)
	rep, err := r.RunOnce(ctx)
	if err != nil || rep.Events != 1 || rep.Missing != 1 || rep.Created != 1 {
		t.Fatalf("first run rep=%+v err=%v", rep, err)
	}
	rep, err = r.RunOnce(ctx)
	if err != nil || rep.Missing != 0 || rep.Created != 0 {
		t.Fatalf("second run should be covered: rep=%+v err=%v", rep, err)
	}
	if svc.M.Get("reconcile_created") != 1 || svc.M.Get("reconcile_missing") != 1 {
		t.Fatalf("metrics created=%d missing=%d", svc.M.Get("reconcile_created"), svc.M.Get("reconcile_missing"))
	}
}
