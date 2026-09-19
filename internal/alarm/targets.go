package alarm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ---- 告警推送目标（技术方案 BL5 §09.1） ----
//
// 推送前问一次 fleet-svc「这台设备该通知谁」：组织设备返回站点订阅用户 + 个人绑定用户。
// 三条硬规矩：
//  1. 超时 500 ms，失败一律回退到个人绑定（BL1 行为）并计数 targets_fallback——告警不能因为
//     一个只读的辅助查询而延迟或丢失；
//  2. 未配置 IOT_FLEET_URL 时完全不查（默认行为与 BL1 一模一样）；
//  3. 状态机、squelch、升级逻辑一律不碰。

// TargetsTimeout 是查询 fleet-svc 的硬超时。
const TargetsTimeout = 500 * time.Millisecond

// Target 是一个推送目标（与 fleet-svc /internal/alarm-targets 的返回同形）。
type Target struct {
	UserID int64  `json:"user_id"`
	Role   string `json:"role,omitempty"`
	Source string `json:"source"` // org | personal
}

// TargetLookup 抽象目标查询；生产是 fleet-svc 的 HTTP 客户端，测试注入 fake。
type TargetLookup interface {
	Targets(ctx context.Context, sn string) ([]Target, error)
}

// HTTPTargets 调 fleet-svc GET /internal/alarm-targets?sn=。
type HTTPTargets struct {
	Base string
	HTTP *http.Client
}

// NewTargetLookup 按 fleet URL 造客户端；URL 为空返回 nil 接口（= 不查询，行为不变）。
func NewTargetLookup(base string) TargetLookup {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if base == "" {
		return nil
	}
	return &HTTPTargets{Base: base, HTTP: &http.Client{Timeout: TargetsTimeout}}
}

func (c *HTTPTargets) Targets(ctx context.Context, sn string) ([]Target, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.Base+"/internal/alarm-targets?sn="+url.QueryEscape(sn), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("alarm targets: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("alarm targets: status %d", resp.StatusCode)
	}
	var out struct {
		Data struct {
			SN      string   `json:"sn"`
			OrgID   int64    `json:"org_id"`
			Targets []Target `json:"targets"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("alarm targets decode: %w", err)
	}
	return out.Data.Targets, nil
}

// resolveTargets 查推送目标：未配置 → nil（跳过）；出错 / 超时 → nil 并计数回退。
// 返回 nil 表示「按原有的个人绑定推送」，调用方不需要区分「没配」与「查失败」。
func (s *Service) resolveTargets(ctx context.Context, sn string) []Target {
	if s == nil || s.Targets == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, TargetsTimeout)
	defer cancel()
	t, err := s.Targets.Targets(ctx, sn)
	if err != nil {
		s.M.Inc("targets_fallback")
		slog.Warn("alarm targets lookup failed, falling back to personal bindings", "sn", sn, "err", err)
		return nil
	}
	s.M.Inc("targets_resolved")
	return t
}
