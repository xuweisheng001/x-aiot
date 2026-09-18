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
