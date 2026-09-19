package job

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"
)

// 反哺 opt-in 对账（INC-4-18，S1 合规）：job_record 里出现的每个 SN，
// 此刻的影子 reported.job_feedback_optin 都必须是 true。
//
// 为什么消费链路 fail-closed 了还要对账：落库时的判定只用了那一刻的影子。
// 影子晚到、consumer 旧版本、直连数据库的回填脚本、撤回时 purge 漏跑，都会留下「未同意却有数据」的行，
// 而这些行在链路上不会再被任何护栏碰到一次。对账是它们唯一的出口。
//
// 一条纪律：Redis 读失败的 SN 既不算违规也不删。
// 消费侧 fail-closed（读不到就丢）代价是少一条记录；删除侧 fail-closed 代价是删掉用户数据——
// 两个方向的「保守」是相反的，Redis 抖一下不能变成批量删库。
const (
	DefaultOptInReconcileInterval = time.Hour

	MOptInReconcileRuns   = "optin_reconcile_runs"
	MOptInChecked         = "optin_checked"
	MOptInViolations      = "optin_violations"
	MOptInPurged          = "optin_purged"
	MOptInPurgedRows      = "optin_purged_rows"
	MOptInShadowErrors    = "optin_shadow_errors"
	MOptInReconcileErrors = "optin_reconcile_errors"
)

// optinMetricNames 在构造时预注册（Add 0），保证零违规时 /metrics 也输出这几行。
var optinMetricNames = []string{MOptInReconcileRuns, MOptInChecked, MOptInViolations, MOptInPurged,
	MOptInPurgedRows, MOptInShadowErrors, MOptInReconcileErrors}

// ViolatingSNs 纯函数：返回「有加工数据但未同意」的 SN（升序稳定）。
//
//	OptInFalse / OptInMissing → 违规（缺失也算：v1.1 之前的设备不会上报该字段，没有同意就是没有同意）
//	OptInAllowed              → 合规
//	OptInErr                  → 判不了，既不违规也不删（由调用方单独计数）
func ViolatingSNs(snOptin map[string]OptInState) []string {
	var out []string
	for sn, st := range snOptin {
		if st == OptInFalse || st == OptInMissing {
			out = append(out, sn)
		}
	}
	sort.Strings(out)
	return out
}

// JobSNLister 是对账依赖的 PG 子集（*PGStore 满足）。单独成接口，避免为一个只有对账用的查询
// 改动 Store 与它所有的 fake。
type JobSNLister interface {
	// DistinctJobSNs 返回 job_record 中出现过的全部 SN。
	DistinctJobSNs(ctx context.Context) ([]string, error)
}

func (s *PGStore) DistinctJobSNs(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT sn FROM iot_shard.job_record ORDER BY sn`)
	if err != nil {
		return nil, fmt.Errorf("distinct job sns: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var sn string
		if err := rows.Scan(&sn); err != nil {
			return nil, err
		}
		out = append(out, sn)
	}
	return out, rows.Err()
}

// OptInReconciler 周期性核对加工记录的 opt-in 状态并删除未同意的数据。
type OptInReconciler struct {
	Svc *Service
	SNs JobSNLister
	Now func() time.Time
}

func NewOptInReconciler(svc *Service, sns JobSNLister) *OptInReconciler {
	if svc != nil && svc.M != nil {
		for _, k := range optinMetricNames {
			svc.M.Add(k, 0)
		}
	}
	return &OptInReconciler{Svc: svc, SNs: sns, Now: time.Now}
}

// OptInReport 是一轮对账的结果。
type OptInReport struct {
	Scanned      int   `json:"scanned"`
	Violations   int   `json:"violations"`
	Purged       int   `json:"purged"`
	PurgedRows   int64 `json:"purged_rows"`
	ShadowErrors int   `json:"shadow_errors"`
}

// RunOnce 执行一轮：distinct SN → 逐个读影子 → ViolatingSNs → PurgeSN。
// 列 SN 失败返回 err（不删任何东西）；单个 SN 的删除失败只计数并继续。
func (r *OptInReconciler) RunOnce(ctx context.Context) (OptInReport, error) {
	var rep OptInReport
	if r.Svc == nil || r.SNs == nil {
		return rep, fmt.Errorf("optin reconcile: not configured")
	}
	s := r.Svc
	s.M.Inc(MOptInReconcileRuns)
	sns, err := r.SNs.DistinctJobSNs(ctx)
	if err != nil {
		s.M.Inc(MOptInReconcileErrors)
		s.M.Inc("db_errors")
		return rep, fmt.Errorf("optin reconcile: list sns: %w", err)
	}
	rep.Scanned = len(sns)
	s.M.Add(MOptInChecked, int64(len(sns)))

	// 直接用 Shadow + DecideOptIn，不走 Service.optIn：那条路会打 dropped_optin_* 计数，
	// 那是消费链路的丢弃口径，对账不能污染它。
	states := make(map[string]OptInState, len(sns))
	for _, sn := range sns {
		rep0, err := s.Shadow.Read(ctx, sn)
		st := DecideOptIn(rep0, err)
		if st == OptInErr {
			rep.ShadowErrors++
			s.M.Inc(MOptInShadowErrors)
			slog.Warn("optin reconcile: shadow read failed, leaving data untouched", "sn", sn, "err", err)
			continue
		}
		states[sn] = st
	}

	bad := ViolatingSNs(states)
	rep.Violations = len(bad)
	s.M.Add(MOptInViolations, int64(len(bad)))
	for _, sn := range bad {
		slog.Error("OPTIN VIOLATION: job data present without consent", "sn", sn, "optin", states[sn].String())
		n, err := s.Store.PurgeSN(ctx, sn)
		if err != nil {
			s.M.Inc(MOptInReconcileErrors)
			s.M.Inc("db_errors")
			slog.Error("optin reconcile: purge failed", "sn", sn, "err", err)
			continue
		}
		rep.Purged++
		rep.PurgedRows += n
		s.M.Inc(MOptInPurged)
		s.M.Add(MOptInPurgedRows, n)
		slog.Info("optin reconcile: job data purged", "sn", sn, "rows", n, "optin", states[sn].String())
	}
	slog.Info("job optin reconcile", "scanned", rep.Scanned, "violations", rep.Violations,
		"purged", rep.Purged, "rows", rep.PurgedRows, "shadow_errors", rep.ShadowErrors)
	return rep, nil
}

// Run 每 interval 跑一轮，直到 ctx 取消；启动即跑一轮。
func (r *OptInReconciler) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultOptInReconcileInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("job optin reconcile round failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
