package support

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

// 指令授权对账（INC-6-02 检测手段）：cmd_audit 里 source ∈ {support, agent} 的每一条指令，
// 在它自己的 created_at 时刻都必须有一条覆盖它的 support_grant。
//
// 为什么必须有对账：grantcheck 是 fail-closed 的护栏，但护栏只在 deviceapi 这条路径上。
// 有人绕过 grant（直连 MQTT、直写审计、新加的后台脚本、grantcheck 被误注释）时护栏不会报错，
// 只会「什么都没发生」。对账是唯一能把「绕过」变成可观测信号的东西。
//
// 判定口径：
//   - result='denied' 的审计行不算越权——那正是护栏拦下来的那一条，拦住了就不该再报一次。
//   - 已撤回的 grant 仍参与匹配，但只覆盖 revoked_at 之前的指令：撤回之前下的指令当时是合法的。
//   - 一条指令命中任意一条 grant 即合法（同 SN 可能有多张工单的 grant 交叠）。
const (
	DefaultAuditReconcileInterval = 5 * time.Minute
	DefaultAuditWindow            = 30 * time.Minute

	MAuditReconcileRuns = "audit_reconcile_runs"
	MAuditChecked       = "audit_checked"
	MAuditUnauthorized  = "audit_unauthorized"
	MAuditErrors        = "audit_reconcile_errors"
)

// auditMetricNames 在构造时预注册（Add 0），保证零违规时 /metrics 也输出这几行。
var auditMetricNames = []string{MAuditReconcileRuns, MAuditChecked, MAuditUnauthorized, MAuditErrors}

// AuditCmd 是 iot_shard.cmd_audit 中一条待核对的指令。
type AuditCmd struct {
	CmdID     string    `json:"cmd_id"`
	SN        string    `json:"sn"`
	Action    string    `json:"action"`
	Source    string    `json:"source"`
	Operator  string    `json:"operator"`
	Result    string    `json:"result"`
	CreatedAt time.Time `json:"created_at"`
}

// GrantWindow 是一条 grant 的有效时间窗（只取对账需要的列）。
type GrantWindow struct {
	GrantID   string     `json:"grant_id"`
	SN        string     `json:"sn"`
	GrantedAt time.Time  `json:"granted_at"`
	ExpiresAt time.Time  `json:"expires_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// Covers 纯函数：该 grant 是否覆盖 sn 在 t 时刻的一条指令。
// 区间取 [granted_at, expires_at)：正好等于 expires_at 的指令已经过期（与 Grant.Effective 的 After 同向）。
func (g GrantWindow) Covers(sn string, t time.Time) bool {
	if g.SN != sn || g.GrantedAt.IsZero() || g.ExpiresAt.IsZero() {
		return false
	}
	if t.Before(g.GrantedAt) || !t.Before(g.ExpiresAt) {
		return false
	}
	// 撤回：revoked_at 之后（含当刻）的指令不再被这条 grant 覆盖。
	if g.RevokedAt != nil && !t.Before(*g.RevokedAt) {
		return false
	}
	return true
}

// UnauthorizedCmds 纯函数：返回 cmds 中没有任何 grant 覆盖的指令。
// 输出按 created_at 升序稳定排序，与输入顺序无关（SQL 顺序变化不改变告警内容）。
func UnauthorizedCmds(cmds []AuditCmd, grants []GrantWindow) []AuditCmd {
	bySN := map[string][]GrantWindow{}
	for _, g := range grants {
		bySN[g.SN] = append(bySN[g.SN], g)
	}
	var out []AuditCmd
	for _, c := range cmds {
		covered := false
		for _, g := range bySN[c.SN] {
			if g.Covers(c.SN, c.CreatedAt) {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, c)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

// AuditStore 是对账依赖的 PG 子集（*PGStore 满足）。单独成接口，避免把只有对账用到的两个查询
// 塞进 Store —— handler / service 的 fake 不必为此改动。
type AuditStore interface {
	// AuditCmdsSince 返回窗口内 source ∈ {support, agent} 且未被拦下的指令。
	AuditCmdsSince(ctx context.Context, since time.Time) ([]AuditCmd, error)
	// GrantWindowsSince 返回可能覆盖窗口内指令的 grant（含已撤回的）。
	GrantWindowsSince(ctx context.Context, since time.Time) ([]GrantWindow, error)
}

func (s *PGStore) AuditCmdsSince(ctx context.Context, since time.Time) ([]AuditCmd, error) {
	rows, err := s.DB.Query(ctx, `
SELECT cmd_id, sn, action, source, operator, result, created_at
  FROM iot_shard.cmd_audit
 WHERE created_at >= $1 AND source = ANY($2) AND result <> 'denied'
 ORDER BY created_at`, since, []string{grantcheck.SourceSupport, grantcheck.SourceAgent})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditCmd
	for rows.Next() {
		var c AuditCmd
		if err := rows.Scan(&c.CmdID, &c.SN, &c.Action, &c.Source, &c.Operator, &c.Result, &c.CreatedAt); err != nil {
			return nil, err
		}
		c.CreatedAt = c.CreatedAt.UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *PGStore) GrantWindowsSince(ctx context.Context, since time.Time) ([]GrantWindow, error) {
	// expires_at >= since：窗口开始前批准、窗口内仍有效的 grant 也要取回。
	// status 含 revoked：撤回前下的指令仍然合法，由 Covers 用 revoked_at 判定。
	rows, err := s.DB.Query(ctx, `
SELECT grant_id, sn, granted_at, expires_at, revoked_at
  FROM iot_shard.support_grant
 WHERE granted_at IS NOT NULL AND expires_at IS NOT NULL AND expires_at >= $1
   AND status = ANY($2)`, since, []string{GrantGranted, GrantRevoked})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GrantWindow
	for rows.Next() {
		var g GrantWindow
		if err := rows.Scan(&g.GrantID, &g.SN, &g.GrantedAt, &g.ExpiresAt, &g.RevokedAt); err != nil {
			return nil, err
		}
		g.GrantedAt, g.ExpiresAt = g.GrantedAt.UTC(), g.ExpiresAt.UTC()
		out = append(out, g)
	}
	return out, rows.Err()
}

// AuditReconciler 周期性核对指令授权。只报不改：审计是事实记录，对账不得改写它。
type AuditReconciler struct {
	Store  AuditStore
	M      *Metrics
	Window time.Duration
	Now    func() time.Time
}

func NewAuditReconciler(st AuditStore, m *Metrics, window time.Duration) *AuditReconciler {
	if m == nil {
		m = NewMetrics()
	}
	for _, k := range auditMetricNames {
		m.Add(k, 0)
	}
	if window <= 0 {
		window = DefaultAuditWindow
	}
	return &AuditReconciler{Store: st, M: m, Window: window, Now: time.Now}
}

// AuditReport 是一轮对账的结果。
type AuditReport struct {
	Checked      int `json:"checked"`
	Grants       int `json:"grants"`
	Unauthorized int `json:"unauthorized"`
}

func (r *AuditReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// RunOnce 执行一轮：查审计 → 查 grant → 差集 → 逐条 slog.Error。查询失败返回 err（不吞）。
func (r *AuditReconciler) RunOnce(ctx context.Context) (AuditReport, error) {
	var rep AuditReport
	if r.Store == nil {
		return rep, ErrUnavailable
	}
	since := r.now().Add(-r.Window)
	cmds, err := r.Store.AuditCmdsSince(ctx, since)
	if err != nil {
		r.M.Inc(MAuditErrors)
		return rep, fmt.Errorf("audit reconcile: query cmd_audit: %w", err)
	}
	grants, err := r.Store.GrantWindowsSince(ctx, since)
	if err != nil {
		r.M.Inc(MAuditErrors)
		return rep, fmt.Errorf("audit reconcile: query grants: %w", err)
	}
	bad := UnauthorizedCmds(cmds, grants)
	rep.Checked, rep.Grants, rep.Unauthorized = len(cmds), len(grants), len(bad)
	r.M.Inc(MAuditReconcileRuns)
	r.M.Add(MAuditChecked, int64(len(cmds)))
	r.M.Add(MAuditUnauthorized, int64(len(bad)))
	for _, c := range bad {
		slog.Error("UNAUTHORIZED SUPPORT COMMAND (no grant covers it)", "cmd_id", c.CmdID, "sn", c.SN,
			"action", c.Action, "source", c.Source, "operator", c.Operator, "result", c.Result, "created_at", c.CreatedAt)
	}
	slog.Info("support audit reconcile", "checked", rep.Checked, "grants", rep.Grants,
		"unauthorized", rep.Unauthorized, "window", r.Window.String())
	return rep, nil
}

// Run 每 interval 跑一轮，直到 ctx 取消；启动即跑一轮。
func (r *AuditReconciler) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultAuditReconcileInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("support audit reconcile round failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
