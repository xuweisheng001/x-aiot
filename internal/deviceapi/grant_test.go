package deviceapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

// fakeGrants 模拟 grantcheck：agent 非自检一律拒绝（与真实规则一致）；其余按配置返回。
type fakeGrants struct {
	ok      bool
	grantID string
	calls   int
}

func (f *fakeGrants) Check(_ context.Context, _, source, action string, _ time.Time) (grantcheck.Decision, error) {
	f.calls++
	if source == grantcheck.SourceAgent && !grantcheck.AgentAllowed(action) {
		return grantcheck.Decision{Reason: "agent may only self_check"}, nil
	}
	if f.ok {
		return grantcheck.Decision{OK: true, GrantID: f.grantID}, nil
	}
	return grantcheck.Decision{Reason: "no valid grant"}, nil
}

// X-Source: support 且无 Checker（或无有效 grant）→ 403/10003，MQTT 零消息，留一条 denied 审计。
func TestPostCmdSupportWithoutGrantRefused(t *testing.T) {
	s, mq, js, _, _ := newTestServer() // Grants 为 nil：fail-closed
	h := s.Handler()
	rec := do(h, "POST", "/api/v1/devices/XT001/cmd", `{"action":"self_check"}`, map[string]string{grantcheck.HeaderSource: "support", grantcheck.HeaderOperator: "cs-01"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	code, _ := decode(t, rec)
	if code != 10003 || len(mq.msgs) != 0 {
		t.Fatalf("code=%d mqtt=%d", code, len(mq.msgs))
	}
	if len(js.data) != 1 {
		t.Fatalf("expected one denied audit, got %d", len(js.data))
	}
	var a AuditRecord
	_ = json.Unmarshal(js.data[0], &a)
	if a.Result != "denied" || a.Source != "support" || a.Operator != "cs-01" {
		t.Fatalf("denied audit=%+v", a)
	}

	// 有 Checker 但无有效 grant → 同样 403
	s2, mq2, _, _, _ := newTestServer()
	fg := &fakeGrants{ok: false}
	s2.Grants = fg
	rec = do(s2.Handler(), "POST", "/api/v1/devices/XT001/cmd", `{"action":"self_check"}`, map[string]string{grantcheck.HeaderSource: "support"})
	if rec.Code != http.StatusForbidden || len(mq2.msgs) != 0 || fg.calls != 1 {
		t.Fatalf("no grant: status=%d mqtt=%d calls=%d", rec.Code, len(mq2.msgs), fg.calls)
	}
}

// 有效 grant → 下发，审计 source=support 且 params 带 grant_id；审计先于指令。
func TestPostCmdSupportWithGrantDispatches(t *testing.T) {
	s, mq, js, _, _ := newTestServer()
	s.Grants = &fakeGrants{ok: true, grantID: "g-123"}
	rec := do(s.Handler(), "POST", "/api/v1/devices/XT001/cmd", `{"action":"self_check"}`, map[string]string{grantcheck.HeaderSource: "support", grantcheck.HeaderOperator: "cs-01"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(mq.msgs) != 1 || !strings.Contains(mq.msgs[0].payload, `"self_check"`) {
		t.Fatalf("mqtt=%+v", mq.msgs)
	}
	var a AuditRecord
	_ = json.Unmarshal(js.data[0], &a)
	if a.Source != "support" || a.Operator != "cs-01" || a.Result != "dispatched" || !strings.Contains(string(a.Params), `"grant_id":"g-123"`) {
		t.Fatalf("audit=%+v params=%s", a, string(a.Params))
	}
	if got := strings.Join(js.ord.ev, ","); got != "audit,mqtt" {
		t.Fatalf("order=%s", got)
	}
}

// agent 来源只允许 self_check：pause → 403，即使 grant 有效。
func TestPostCmdAgentOnlySelfCheck(t *testing.T) {
	s, mq, _, _, _ := newTestServer()
	s.Grants = &fakeGrants{ok: true, grantID: "g-1"}
	h := s.Handler()
	rec := do(h, "POST", "/api/v1/devices/XT001/cmd", `{"action":"pause"}`, map[string]string{grantcheck.HeaderSource: "agent"})
	if rec.Code != http.StatusForbidden || len(mq.msgs) != 0 {
		t.Fatalf("agent pause: status=%d mqtt=%d", rec.Code, len(mq.msgs))
	}
	rec = do(h, "POST", "/api/v1/devices/XT002/cmd", `{"action":"self_check"}`, map[string]string{grantcheck.HeaderSource: "agent"})
	if rec.Code != http.StatusOK || len(mq.msgs) != 1 {
		t.Fatalf("agent self_check: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

// 请求体里的 source=support 不能冒充：无 X-Source 头 → console，不需要 grant，照常下发且审计 source=console。
func TestPostCmdBodySourceIgnored(t *testing.T) {
	s, mq, js, _, _ := newTestServer()
	rec := do(s.Handler(), "POST", "/api/v1/devices/XT001/cmd", `{"action":"pause","source":"support","operator":"mallory"}`, nil)
	if rec.Code != http.StatusOK || len(mq.msgs) != 1 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var a AuditRecord
	_ = json.Unmarshal(js.data[0], &a)
	if a.Source != "console" {
		t.Fatalf("body source must be ignored, got %q", a.Source)
	}
}
