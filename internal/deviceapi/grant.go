package deviceapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

// GrantChecker 是客服 / Agent 指令授权校验的抽象（BL6 §4）。
// 生产实现 PGGrantChecker 直查 PG（授权以库为准，不信任上游头）；测试用 fake。
type GrantChecker interface {
	Check(ctx context.Context, sn, source, action string, now time.Time) (grantcheck.Decision, error)
}

// PGGrantChecker 用 grantcheck 子包直查 iot_shard.support_grant。
type PGGrantChecker struct{ Q grantcheck.Querier }

func (p *PGGrantChecker) Check(ctx context.Context, sn, source, action string, now time.Time) (grantcheck.Decision, error) {
	return grantcheck.Check(ctx, p.Q, sn, source, action, now)
}

// GrantCheckTimeout 是授权查询上限；超时 fail-closed。
const GrantCheckTimeout = 2 * time.Second

var warnBodySourceOnce sync.Once

// resolveSource 由服务身份头推导来源；请求体 source 字段被忽略（只 WARN 一次，INC-6-02 / 6-07）。
func resolveSource(header, bodySource string) string {
	src, ignored := grantcheck.SourceFromHeader(header, bodySource)
	if ignored {
		warnBodySourceOnce.Do(func() {
			slog.Warn("cmd request body 'source' is ignored; source is derived from X-Source header only", "body_source", bodySource)
		})
	}
	return src
}

// authorize 在白名单之后、审计之前执行：不需要 grant 的来源直接放行；
// support / agent 必须有有效 grant；Checker 缺失或出错一律拒绝（fail-closed）。
func (s *Server) authorize(ctx context.Context, sn, source, action string) grantcheck.Decision {
	if !grantcheck.NeedsGrant(source) {
		return grantcheck.Decision{OK: true}
	}
	if s.Grants == nil {
		return grantcheck.Decision{Reason: "grant checker not configured"}
	}
	cctx, cancel := context.WithTimeout(ctx, GrantCheckTimeout)
	defer cancel()
	d, err := s.Grants.Check(cctx, sn, source, action, s.now())
	if err != nil {
		slog.Error("grant check", "sn", sn, "source", source, "action", action, "err", err)
		return grantcheck.Decision{Reason: "grant lookup failed"}
	}
	return d
}

// withGrantID 把 grant_id 合并进审计 params（cmd_audit 不改表结构，方案 §11.3）。
func withGrantID(params json.RawMessage, grantID string) json.RawMessage {
	if grantID == "" {
		return params
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(params, &m); err != nil || m == nil {
		m = map[string]json.RawMessage{}
	}
	gid, _ := json.Marshal(grantID)
	m["grant_id"] = gid
	out, _ := json.Marshal(m)
	return out
}
