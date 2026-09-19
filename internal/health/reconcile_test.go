package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

var remBase = time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

func sent(id int64, sn, part string, offset time.Duration) SentReminder {
	return SentReminder{ID: id, SN: sn, Part: part, Level: "50", SentAt: remBase.Add(offset)}
}

func TestCooldownViolations(t *testing.T) {
	const cd = 168 * time.Hour
	cases := []struct {
		name string
		rows []SentReminder
		cd   time.Duration
		want [][2]int64 // 期望的 (prev_id, id) 对
	}{
		{name: "空输入", cd: cd},
		{name: "单条不可能违规", rows: []SentReminder{sent(1, "A", "module", 0)}, cd: cd},
		{
			name: "间隔大于冷却期",
			rows: []SentReminder{sent(1, "A", "module", 0), sent(2, "A", "module", cd+time.Hour)},
			cd:   cd,
		},
		{
			name: "正好等于冷却期：不算违规",
			rows: []SentReminder{sent(1, "A", "module", 0), sent(2, "A", "module", cd)},
			cd:   cd,
		},
		{
			name: "差一秒：违规",
			rows: []SentReminder{sent(1, "A", "module", 0), sent(2, "A", "module", cd-time.Second)},
			cd:   cd,
			want: [][2]int64{{1, 2}},
		},
		{
			name: "同一毫秒两条（多副本同时发）",
			rows: []SentReminder{sent(1, "A", "module", time.Hour), sent(2, "A", "module", time.Hour)},
			cd:   cd,
			want: [][2]int64{{1, 2}},
		},
		{
			name: "跨 part 不相互影响",
			rows: []SentReminder{sent(1, "A", "module", 0), sent(2, "A", "filter", time.Minute)},
			cd:   cd,
		},
		{
			name: "跨 SN 不相互影响",
			rows: []SentReminder{sent(1, "A", "module", 0), sent(2, "B", "module", time.Minute)},
			cd:   cd,
		},
		{
			name: "乱序输入：先排序再判，结果与顺序无关",
			rows: []SentReminder{sent(3, "A", "module", 2*time.Hour), sent(1, "A", "module", 0), sent(2, "A", "module", time.Hour)},
			cd:   cd,
			want: [][2]int64{{1, 2}, {2, 3}}, // 连发三条 = 两次多余打扰
		},
		{
			name: "冷却期为 0：关掉对账（不报）",
			rows: []SentReminder{sent(1, "A", "module", 0), sent(2, "A", "module", time.Second)},
			cd:   0,
		},
		{
			name: "sent_at 为零值的行忽略",
			rows: []SentReminder{{ID: 1, SN: "A", Part: "module"}, sent(2, "A", "module", 0)},
			cd:   cd,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := CooldownViolations(c.rows, c.cd)
			if len(got) != len(c.want) {
				t.Fatalf("got %d violations %+v, want %d", len(got), got, len(c.want))
			}
			for i, w := range c.want {
				if got[i].PrevID != w[0] || got[i].ID != w[1] {
					t.Fatalf("violation[%d]=(%d,%d) want (%d,%d)", i, got[i].PrevID, got[i].ID, w[0], w[1])
				}
				if got[i].Gap != got[i].SentAt.Sub(got[i].PrevSentAt) {
					t.Fatalf("gap mismatch: %+v", got[i])
				}
			}
		})
	}
}

type fakeReminderStore struct {
	rows  []SentReminder
	err   error
	since time.Time
}

func (f *fakeReminderStore) SentRemindersSince(_ context.Context, since time.Time) ([]SentReminder, error) {
	f.since = since
	return f.rows, f.err
}

func TestReminderReconcilerRunOnce(t *testing.T) {
	st := &fakeReminderStore{rows: []SentReminder{
		sent(1, "A", "module", 0), sent(2, "A", "module", time.Hour), sent(3, "B", "module", 0),
	}}
	m := NewMetrics()
	r := NewReminderReconciler(st, m, 0, 0)
	now := remBase.Add(48 * time.Hour)
	r.Now = func() time.Time { return now }

	rep, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Checked != 3 || rep.Violations != 1 {
		t.Fatalf("report=%+v", rep)
	}
	if !st.since.Equal(now.Add(-DefaultReminderWindow)) {
		t.Fatalf("since=%v want %v", st.since, now.Add(-DefaultReminderWindow))
	}
	if r.Cooldown != DefaultCooldown || r.Window != DefaultReminderWindow {
		t.Fatalf("defaults: cooldown=%v window=%v", r.Cooldown, r.Window)
	}
	if m.Get(MReminderReconcileRuns) != 1 || m.Get(MReminderChecked) != 3 || m.Get(MReminderCooldownViol) != 1 {
		t.Fatalf("metrics runs=%d checked=%d violations=%d",
			m.Get(MReminderReconcileRuns), m.Get(MReminderChecked), m.Get(MReminderCooldownViol))
	}

	st.err = errors.New("pg down")
	if _, err := r.RunOnce(context.Background()); err == nil {
		t.Fatal("store error must surface")
	}
	if m.Get(MReminderReconcileErrors) != 1 {
		t.Fatalf("errors=%d", m.Get(MReminderReconcileErrors))
	}
}
