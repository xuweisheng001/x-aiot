// Package grantcheck 是客服 / Agent 指令授权的校验（BL6 §4）。
//
// 独立成子包是为了让 deviceapi 只 import 这一个小包而不依赖整个 support 服务：
// deviceapi 在「白名单判定之后、审计发布之前」调用 Check，授权以 PG 查询为准，
// 不信任任何上游头的授权含义（X-Source 只用于推导来源，X-Grant-Id 只用于审计）。
package grantcheck

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// 来源常量。app / console 走用户或控制台身份，不需要 grant；support / agent 必须持有有效 grant。
const (
	SourceApp     = "app"
	SourceConsole = "console"
	SourceSupport = "support"
	SourceAgent   = "agent"

	// ActionSelfCheck 是 P0 唯一允许授权的动作，也是 agent 唯一允许的动作。
	ActionSelfCheck = "self_check"

	// HeaderSource / HeaderOperator / HeaderGrantID 由 BFF / xpilot 服务身份注入。
	HeaderSource   = "X-Source"
	HeaderOperator = "X-Operator"
	HeaderGrantID  = "X-Grant-Id"
)

// NeedsGrant 报告该来源是否必须持有有效 grant。
func NeedsGrant(source string) bool {
	return source == SourceSupport || source == SourceAgent
}

// AgentAllowed 报告 agent 来源是否允许该动作：只有 self_check。
func AgentAllowed(action string) bool { return action == ActionSelfCheck }

// SourceFromHeader 由服务身份头推导来源；缺失或非法一律 console。
// 请求体里的 source 字段不参与决策（返回 bodyIgnored=true 供调用方 WARN 一次）。
func SourceFromHeader(header, bodySource string) (source string, bodyIgnored bool) {
	h := strings.ToLower(strings.TrimSpace(header))
	switch h {
	case SourceApp, SourceSupport, SourceAgent, SourceConsole:
		source = h
	default:
		source = SourceConsole
	}
	return source, strings.TrimSpace(bodySource) != ""
}

// Decision 是一次授权判定的结果。
type Decision struct {
	OK      bool
	GrantID string
	Reason  string
}

// Querier 是 Check 依赖的最小 PG 子集（*pgxpool.Pool / pgx.Tx 满足）。
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Check 纯授权判定：
//   - 不需要 grant 的来源直接放行；
//   - agent 来源动作不是 self_check → 拒绝 "agent may only self_check"；
//   - support / agent 必须存在 status=granted、动作包含、未过期、未撤回的 grant，取最近一条。
//
// PG 出错 fail-closed（拒绝并返回 err）。
func Check(ctx context.Context, q Querier, sn, source, action string, now time.Time) (Decision, error) {
	if !NeedsGrant(source) {
		return Decision{OK: true}, nil
	}
	if source == SourceAgent && !AgentAllowed(action) {
		return Decision{Reason: "agent may only self_check"}, nil
	}
	if q == nil {
		return Decision{Reason: "grant checker not configured"}, nil
	}
	var id string
	err := q.QueryRow(ctx, `
SELECT grant_id FROM iot_shard.support_grant
 WHERE sn = $1 AND status = 'granted' AND $2 = ANY(actions)
   AND expires_at > $3 AND revoked_at IS NULL
 ORDER BY granted_at DESC LIMIT 1`, sn, action, now).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Decision{Reason: "no valid grant"}, nil
	}
	if err != nil {
		return Decision{Reason: "grant lookup failed"}, err
	}
	return Decision{OK: true, GrantID: strings.TrimSpace(id)}, nil
}
