package health

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"
)

// 提醒冷却对账（INC-3-09 检测手段）：同一 (sn, part) 的两条已发出提醒之间至少隔一个冷却期。
//
// 冷却本身是在发送前查 LastSentAt 判的，进程内还有 runMu 串行化；
// 但多副本、批重跑、通知重试各自都能绕过这两道：它们都只保证「单进程单轮」不重复。
// 用户收到两条一模一样的换耗材提醒，不会有任何护栏报错——只有事后对已发记录做一次排序检查才看得见。
//
// 只报不改：提醒已经发出去了，删 health_reminder 行只会让下一次对账看不见问题，
// 而用户手机上的那两条通知不会因此消失。
const (
	DefaultReminderReconcileInterval = time.Hour
	DefaultReminderWindow            = 30 * 24 * time.Hour

	MReminderReconcileRuns   = "reminder_reconcile_runs"
	MReminderChecked         = "reminder_reconcile_checked"
	MReminderCooldownViol    = "reminder_cooldown_violations"
	MReminderReconcileErrors = "reminder_reconcile_errors"
)

var reminderMetricNames = []string{MReminderReconcileRuns, MReminderChecked, MReminderCooldownViol, MReminderReconcileErrors}

// SentReminder 是一条已发出的提醒（health_reminder 里 sent_at IS NOT NULL 的行）。
type SentReminder struct {
	ID     int64     `json:"id"`
	SN     string    `json:"sn"`
	Part   string    `json:"part"`
	Level  string    `json:"level"`
	SentAt time.Time `json:"sent_at"`
}

// Violation 是一对违反冷却的相邻提醒。
type Violation struct {
	SN         string        `json:"sn"`
	Part       string        `json:"part"`
	PrevID     int64         `json:"prev_id"`
	ID         int64         `json:"id"`
	PrevSentAt time.Time     `json:"prev_sent_at"`
	SentAt     time.Time     `json:"sent_at"`
	Gap        time.Duration `json:"gap"`
}

// CooldownViolations 纯函数：按 (sn, part) 分组、按 sent_at 升序，相邻间隔 < cooldown 即一条违规。
//
//   - 间隔正好等于 cooldown 不算违规（冷却期满即可再发）。
//   - 不同 part 互不影响（模块与滤芯各有各的冷却）。
//   - 输入乱序无所谓：内部排序（sent_at 相同按 id 定序，保证输出稳定）。
//   - 连发三条会报两条违规（1→2、2→3），因为那是两次多余的打扰。
func CooldownViolations(rows []SentReminder, cooldown time.Duration) []Violation {
	if cooldown <= 0 {
		return nil
	}
	type key struct{ sn, part string }
	groups := map[key][]SentReminder{}
	for _, r := range rows {
		if r.SentAt.IsZero() {
			continue
		}
		k := key{r.SN, r.Part}
		groups[k] = append(groups[k], r)
	}
	keys := make([]key, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].sn != keys[j].sn {
			return keys[i].sn < keys[j].sn
		}
		return keys[i].part < keys[j].part
	})
	var out []Violation
	for _, k := range keys {
		g := groups[k]
		sort.SliceStable(g, func(i, j int) bool {
			if !g[i].SentAt.Equal(g[j].SentAt) {
				return g[i].SentAt.Before(g[j].SentAt)
			}
			return g[i].ID < g[j].ID
		})
		for i := 1; i < len(g); i++ {
			gap := g[i].SentAt.Sub(g[i-1].SentAt)
			if gap < cooldown {
				out = append(out, Violation{SN: k.sn, Part: k.part, PrevID: g[i-1].ID, ID: g[i].ID,
					PrevSentAt: g[i-1].SentAt, SentAt: g[i].SentAt, Gap: gap})
			}
		}
	}
	return out
}

// ReminderStore 是对账依赖的 PG 子集（*PGStore 满足）。单独成接口，避免为一个只有对账用的查询
// 改动 Store 与它所有的 fake。
type ReminderStore interface {
	// SentRemindersSince 返回窗口内已发出的提醒（sent_at IS NOT NULL）。
	SentRemindersSince(ctx context.Context, since time.Time) ([]SentReminder, error)
}

func (s *PGStore) SentRemindersSince(ctx context.Context, since time.Time) ([]SentReminder, error) {
	rows, err := s.DB.Query(ctx, `
SELECT id, sn, part, level, sent_at
  FROM iot_shard.health_reminder
 WHERE sent_at IS NOT NULL AND sent_at >= $1
 ORDER BY sn, part, sent_at, id`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SentReminder
	for rows.Next() {
		var r SentReminder
		if err := rows.Scan(&r.ID, &r.SN, &r.Part, &r.Level, &r.SentAt); err != nil {
			return nil, err
		}
		r.SentAt = r.SentAt.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

// ReminderReconciler 周期性核对提醒冷却。
type ReminderReconciler struct {
	Store    ReminderStore
	M        *Metrics
	Window   time.Duration
	Cooldown time.Duration
	Now      func() time.Time
}

func NewReminderReconciler(st ReminderStore, m *Metrics, window, cooldown time.Duration) *ReminderReconciler {
	if m == nil {
		m = NewMetrics()
	}
	for _, k := range reminderMetricNames {
		m.Add(k, 0)
	}
	if window <= 0 {
		window = DefaultReminderWindow
	}
	if cooldown <= 0 {
		cooldown = DefaultCooldown
	}
	return &ReminderReconciler{Store: st, M: m, Window: window, Cooldown: cooldown, Now: time.Now}
}

// ReminderReport 是一轮对账的结果。
type ReminderReport struct {
	Checked    int `json:"checked"`
	Violations int `json:"violations"`
}

func (r *ReminderReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// RunOnce 执行一轮：查窗口内已发提醒 → CooldownViolations → 逐条 slog.Error。查询失败返回 err。
func (r *ReminderReconciler) RunOnce(ctx context.Context) (ReminderReport, error) {
	var rep ReminderReport
	if r.Store == nil {
		return rep, fmt.Errorf("reminder reconcile: not configured")
	}
	rows, err := r.Store.SentRemindersSince(ctx, r.now().Add(-r.Window))
	if err != nil {
		r.M.Inc(MReminderReconcileErrors)
		return rep, fmt.Errorf("reminder reconcile: query reminders: %w", err)
	}
	viols := CooldownViolations(rows, r.Cooldown)
	rep.Checked, rep.Violations = len(rows), len(viols)
	r.M.Inc(MReminderReconcileRuns)
	r.M.Add(MReminderChecked, int64(len(rows)))
	r.M.Add(MReminderCooldownViol, int64(len(viols)))
	for _, v := range viols {
		slog.Error("REMINDER COOLDOWN VIOLATION (duplicate notification reached the user)",
			"sn", v.SN, "part", v.Part, "prev_reminder_id", v.PrevID, "reminder_id", v.ID,
			"prev_sent_at", v.PrevSentAt, "sent_at", v.SentAt, "gap", v.Gap.String(), "cooldown", r.Cooldown.String())
	}
	slog.Info("health reminder reconcile", "checked", rep.Checked, "violations", rep.Violations,
		"window", r.Window.String(), "cooldown", r.Cooldown.String())
	return rep, nil
}

// Run 每 interval 跑一轮，直到 ctx 取消；启动即跑一轮。
func (r *ReminderReconciler) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultReminderReconcileInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("health reminder reconcile round failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
