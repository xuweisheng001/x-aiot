package fleet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// ---- 告警推送目标（技术方案 BL5 §09.1 / §17.1「告警目标」） ----
//
// alarm-svc 推送前拉一次「这台设备该通知谁」：站点订阅用户（组织侧）+ 个人绑定用户（BL1 行为）。
// 只读、无租户中间件——调用方是 alarm-svc 自己，不是某个组织成员，也就没有 X-Org-Id / 角色可查。

// TargetSource 是目标用户的来源。
const (
	SourceOrg      = "org"
	SourcePersonal = "personal"
)

// AlarmTarget 是一个推送目标。
type AlarmTarget struct {
	UserID int64  `json:"user_id"`
	Role   string `json:"role,omitempty"` // 组织成员角色；个人绑定用户为空
	Source string `json:"source"`         // org | personal
}

// AlarmTargets 是 GET /internal/alarm-targets?sn= 的响应。
type AlarmTargets struct {
	SN      string        `json:"sn"`
	OrgID   int64         `json:"org_id"` // 0 = 设备不属于任何组织
	Targets []AlarmTarget `json:"targets"`
}

// MergeTargets 纯函数：站点订阅用户与个人绑定用户合并去重。
// 同一 user_id 同时是组织订阅者与个人绑定者时保留组织身份（带角色，便于推送侧分渠道）；
// 顺序稳定：先组织（按 user_id），后个人（按 user_id）。
func MergeTargets(org []AlarmTarget, personal []int64) []AlarmTarget {
	seen := make(map[int64]bool, len(org)+len(personal))
	out := make([]AlarmTarget, 0, len(org)+len(personal))
	sorted := append([]AlarmTarget(nil), org...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].UserID < sorted[j].UserID })
	for _, t := range sorted {
		if t.UserID <= 0 || seen[t.UserID] {
			continue
		}
		seen[t.UserID] = true
		t.Source = SourceOrg
		out = append(out, t)
	}
	ids := append([]int64(nil), personal...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, AlarmTarget{UserID: id, Source: SourcePersonal})
	}
	return out
}

// AlarmTargetStore 是告警目标查询用到的 Store 子集（窄接口 + 类型断言，主接口 store.go 不动）。
type AlarmTargetStore interface {
	DeviceOrgOf(ctx context.Context, sn string) (*DeviceOrg, error)
	SiteSubscribers(ctx context.Context, orgID, siteID int64) ([]AlarmTarget, error)
	PersonalUsers(ctx context.Context, sn string) ([]int64, error)
}

// SiteSubscribers 返回站点告警订阅用户（必须仍是在册成员：退出组织即不再收告警）。
func (s *PGStore) SiteSubscribers(ctx context.Context, orgID, siteID int64) ([]AlarmTarget, error) {
	rows, err := s.DB.Query(ctx, `
SELECT s.user_id, m.role FROM iot_shard.org_alarm_subscription s
  JOIN iot_shard.org_member m ON m.org_id = s.org_id AND m.user_id = s.user_id AND m.removed_at IS NULL
 WHERE s.org_id=$1 AND s.site_id=$2 ORDER BY s.user_id`, orgID, siteID)
	if err != nil {
		return nil, fmt.Errorf("site subscribers: %w", err)
	}
	defer rows.Close()
	var out []AlarmTarget
	for rows.Next() {
		var t AlarmTarget
		if err := rows.Scan(&t.UserID, &t.Role); err != nil {
			return nil, err
		}
		t.Source = SourceOrg
		out = append(out, t)
	}
	return out, rows.Err()
}

// PersonalUsers 返回设备的个人绑定用户（BL1 device_binding）。
func (s *PGStore) PersonalUsers(ctx context.Context, sn string) ([]int64, error) {
	rows, err := s.DB.Query(ctx, `SELECT user_id FROM iot_shard.device_binding WHERE sn=$1 AND unbound_at IS NULL ORDER BY user_id`, sn)
	if err != nil {
		return nil, fmt.Errorf("personal users: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// AlarmTargetsOf 查一台设备的推送目标：设备不属于任何组织时只返回个人绑定用户。
func (s *Service) AlarmTargetsOf(ctx context.Context, sn string) (*AlarmTargets, error) {
	st, ok := s.Store.(AlarmTargetStore)
	if !ok {
		return nil, fmt.Errorf("store does not support alarm targets")
	}
	if !snRe.MatchString(sn) {
		return nil, fmt.Errorf("%w: bad sn", ErrBadParam)
	}
	out := &AlarmTargets{SN: sn, Targets: []AlarmTarget{}}
	var orgTargets []AlarmTarget
	switch d, err := st.DeviceOrgOf(ctx, sn); {
	case err == nil:
		out.OrgID = d.OrgID
		if orgTargets, err = st.SiteSubscribers(ctx, d.OrgID, d.SiteID); err != nil {
			return nil, err
		}
	case errors.Is(err, ErrNotFound): // 个人设备：只有 BL1 的绑定用户
	default:
		return nil, err
	}
	personal, err := st.PersonalUsers(ctx, sn)
	if err != nil {
		return nil, err
	}
	if merged := MergeTargets(orgTargets, personal); len(merged) > 0 {
		out.Targets = merged
	}
	return out, nil
}

// registerAlarmTargets 挂 GET /internal/alarm-targets（root mux，**不经租户中间件**）。
func registerAlarmTargets(root *http.ServeMux, svc *Service) {
	root.HandleFunc("GET /internal/alarm-targets", func(w http.ResponseWriter, r *http.Request) {
		sn := r.URL.Query().Get("sn")
		if sn == "" {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "sn required")
			return
		}
		resp, err := svc.AlarmTargetsOf(r.Context(), sn)
		if err != nil {
			fail(w, err)
			return
		}
		svc.M.Inc(MAlarmTargetsServed)
		httpx.OK(w, resp)
	})
}
