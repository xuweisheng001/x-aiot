package accessory

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// 配对归属对账（INC-2-06 / INC-2-12 检测手段）：配对时校验过 owner 一致，之后就再也没人看过。
// 主机转让、配件转让、解绑重绑之后，这条配对仍然有效，于是它变成一条越权通道：
// 旧主人的配件还能跟着新主人的主机联动，新主人的告警上下文里还挂着旧主人的设备。
//
// 处置是「停联动不解绑」：linkage_enabled=false。
// 解绑会丢掉 pair_key 与历史，用户看到的是设备凭空消失；停联动只是不再自动开关风机，
// 本地兜底仍在固件里，人工确认归属后 PATCH 回来即可。宁可不联动，也不越权。
const (
	DefaultOwnerReconcileInterval = 24 * time.Hour

	// 违规原因（reason）。
	ReasonOwnerMismatch = "owner_mismatch"
	ReasonHostUnbound   = "host_unbound"
	ReasonAccUnbound    = "acc_unbound"

	MOwnerReconcileRuns = "owner_reconcile_runs"
	MOwnerChecked       = "owner_checked"
	MOwnerDrift         = "owner_drift"
	MOwnerDisabled      = "owner_linkage_disabled"
	MOwnerErrors        = "owner_reconcile_errors"
)

var ownerMetricNames = []string{MOwnerReconcileRuns, MOwnerChecked, MOwnerDrift, MOwnerDisabled, MOwnerErrors}

// OwnerDrift 纯函数：给定配对两端的 owner（nil = 无有效绑定），判定是否越权及原因。
//
// 与配对时的 OwnersConsistent 不同：那里「任一方无绑定信息」是放行（模拟器 / 未绑定的新机，
// 由 BFF 兜底）；这里是拦截——一条已经存在的配对，如果有一端连主人都没有了，
// 说明设备被解绑或转让了，继续联动就是拿旧关系操作新主人的设备。
func OwnerDrift(hostOwner, accOwner *int64) (violation bool, reason string) {
	switch {
	case hostOwner == nil:
		return true, ReasonHostUnbound
	case accOwner == nil:
		return true, ReasonAccUnbound
	case *hostOwner != *accOwner:
		return true, ReasonOwnerMismatch
	}
	return false, ""
}

// firstOwner 取 owner 列表的代表值（nil = 无绑定）。
func firstOwner(owners []int64) *int64 {
	if len(owners) == 0 {
		return nil
	}
	u := owners[0]
	return &u
}

// OwnerReconciler 周期性核对全部有效配对的归属。
type OwnerReconciler struct {
	Store Store
	RDB   *redis.Client // 停联动后失效配对缓存；nil 时跳过（缓存 60 s 自愈）
	M     *Metrics
	Now   func() time.Time
}

func NewOwnerReconciler(st Store, rdb *redis.Client, m *Metrics) *OwnerReconciler {
	if m == nil {
		m = NewMetrics()
	}
	for _, k := range ownerMetricNames {
		m.Add(k, 0)
	}
	return &OwnerReconciler{Store: st, RDB: rdb, M: m, Now: time.Now}
}

// OwnerReport 是一轮对账的结果。
type OwnerReport struct {
	Checked  int            `json:"checked"`
	Drift    int            `json:"drift"`
	Disabled int            `json:"disabled"`
	Errors   int            `json:"errors"`
	Reasons  map[string]int `json:"reasons,omitempty"`
}

// RunOnce 遍历全部有效配对核对归属。列配对失败返回 err；单条配对的查询 / 更新失败只计数并继续
// （一条坏行不能让整轮对账停摆）。
func (r *OwnerReconciler) RunOnce(ctx context.Context) (OwnerReport, error) {
	rep := OwnerReport{Reasons: map[string]int{}}
	if r.Store == nil {
		return rep, fmt.Errorf("owner reconcile: not configured")
	}
	r.M.Inc(MOwnerReconcileRuns)
	pairings, err := r.Store.AllActivePairings(ctx)
	if err != nil {
		r.M.Inc(MOwnerErrors)
		r.M.Inc("db_errors")
		return rep, fmt.Errorf("owner reconcile: list pairings: %w", err)
	}
	rep.Checked = len(pairings)
	r.M.Add(MOwnerChecked, int64(len(pairings)))

	owners := map[string][]int64{}
	lookup := func(sn string) ([]int64, error) {
		if o, ok := owners[sn]; ok {
			return o, nil
		}
		o, err := r.Store.Owners(ctx, sn)
		if err != nil {
			return nil, err
		}
		owners[sn] = o
		return o, nil
	}

	for _, p := range pairings {
		ho, err := lookup(p.HostSN)
		if err != nil {
			rep.Errors++
			r.M.Inc(MOwnerErrors)
			r.M.Inc("db_errors")
			slog.Error("owner reconcile: host owners", "pairing_id", p.ID, "host_sn", p.HostSN, "err", err)
			continue
		}
		ao, err := lookup(p.AccSN)
		if err != nil {
			rep.Errors++
			r.M.Inc(MOwnerErrors)
			r.M.Inc("db_errors")
			slog.Error("owner reconcile: accessory owners", "pairing_id", p.ID, "acc_sn", p.AccSN, "err", err)
			continue
		}
		violation, reason := OwnerDrift(firstOwner(ho), firstOwner(ao))
		// 多 owner（家庭共享）：代表值不同但集合有交集仍算同一归属，沿用配对时的口径。
		if violation && reason == ReasonOwnerMismatch && OwnersConsistent(ho, ao) {
			violation = false
		}
		if !violation {
			continue
		}
		rep.Drift++
		rep.Reasons[reason]++
		r.M.Inc(MOwnerDrift)
		slog.Error("PAIRING OWNER DRIFT (linkage becomes a cross-owner channel)", "pairing_id", p.ID,
			"host_sn", p.HostSN, "acc_sn", p.AccSN, "reason", reason,
			"host_owners", ho, "acc_owners", ao, "linkage_enabled", p.LinkageEnabled)
		if !p.LinkageEnabled {
			continue // 已经停了，只报不重复写
		}
		if _, err := r.Store.UpdatePairing(ctx, p.ID, boolPtr(false), nil); err != nil {
			rep.Errors++
			r.M.Inc(MOwnerErrors)
			r.M.Inc("db_errors")
			slog.Error("owner reconcile: disable linkage failed", "pairing_id", p.ID, "err", err)
			continue
		}
		InvalidatePairings(ctx, r.RDB, p.HostSN)
		rep.Disabled++
		r.M.Inc(MOwnerDisabled)
		slog.Warn("linkage disabled by owner reconcile (unpair 不做：人工确认归属后 PATCH 恢复)",
			"pairing_id", p.ID, "host_sn", p.HostSN, "acc_sn", p.AccSN, "reason", reason)
	}
	slog.Info("accessory owner reconcile", "checked", rep.Checked, "drift", rep.Drift,
		"disabled", rep.Disabled, "errors", rep.Errors, "reasons", rep.Reasons)
	return rep, nil
}

func boolPtr(b bool) *bool { return &b }

// Run 每 interval 跑一轮，直到 ctx 取消；启动即跑一轮。
func (r *OwnerReconciler) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultOwnerReconcileInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("accessory owner reconcile round failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
