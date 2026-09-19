package health

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// 集成（IOT_IT）：真实 PG。冷却期内的两条已发提醒被对账抓出来，
// 且只报不改——记录必须原样留在库里（删掉只会掩盖问题）。
func TestIntegration_ReminderCooldownReconcile(t *testing.T) {
	db, ctx := itDB(t)
	sn := fmt.Sprintf("ITREM_%d", time.Now().UnixNano()%1e9)
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM iot_shard.health_reminder WHERE sn=$1`, sn)
	})

	now := time.Now().UTC()
	ins := func(part, level string, sentAt time.Time) int64 {
		var id int64
		if err := db.QueryRow(ctx, `INSERT INTO iot_shard.health_reminder(sn, part, level, health_at, sent_at) VALUES($1,$2,$3,50,$4) RETURNING id`,
			sn, part, level, sentAt).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	// module：两条相隔 1 小时（冷却 168h）→ 一条违规
	id1 := ins("module", "50", now.Add(-3*time.Hour))
	id2 := ins("module", "50", now.Add(-2*time.Hour))
	// filter：同一时刻但不同 part → 不与 module 互相影响
	ins("filter", "50", now.Add(-2*time.Hour))

	store := &PGStore{DB: db}
	rows, err := store.SentRemindersSince(ctx, now.Add(-DefaultReminderWindow))
	if err != nil {
		t.Fatal(err)
	}
	var mine []SentReminder
	for _, r := range rows {
		if r.SN == sn {
			mine = append(mine, r)
		}
	}
	if len(mine) != 3 {
		t.Fatalf("got %d sent reminders for %s, want 3", len(mine), sn)
	}
	viols := CooldownViolations(mine, DefaultCooldown)
	if len(viols) != 1 || viols[0].PrevID != id1 || viols[0].ID != id2 || viols[0].Part != "module" {
		t.Fatalf("violations=%+v want one (%d,%d) on module", viols, id1, id2)
	}

	m := NewMetrics()
	r := NewReminderReconciler(store, m, DefaultReminderWindow, DefaultCooldown)
	rep, err := r.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Checked < 3 || rep.Violations < 1 {
		t.Fatalf("report=%+v", rep)
	}
	if m.Get(MReminderReconcileRuns) != 1 || m.Get(MReminderCooldownViol) < 1 || m.Get(MReminderReconcileErrors) != 0 {
		t.Fatalf("metrics runs=%d violations=%d errors=%d",
			m.Get(MReminderReconcileRuns), m.Get(MReminderCooldownViol), m.Get(MReminderReconcileErrors))
	}
	// 只报不改
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.health_reminder WHERE sn=$1`, sn).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("对账不得改数据：剩 %d 行，应为 3", n)
	}
}
