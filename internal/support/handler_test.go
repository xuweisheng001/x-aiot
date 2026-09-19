package support

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

func doReq(h http.Handler, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "10.0.0.1:1234"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func data(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var resp struct {
		Code int            `json:"code"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad json: %s", rec.Body.String())
	}
	return resp.Data
}

func TestHandlerGrantLifecycle(t *testing.T) {
	st := newMemStore(fixedNow)
	st.devices["SN1"] = DeviceInfo{ProductKey: "LM_S1"}
	st.owners["SN1"] = 42
	dev := &fakeDev{reported: map[string]string{"work_state": "0"}, ackAfter: 1}
	h := Routes(newSvc(st, dev, &fakeTD{}), "test")

	rec := doReq(h, http.MethodPost, "/api/v1/support/grants", `{"sn":"SN1","ticket_id":"T-1"}`, map[string]string{grantcheck.HeaderOperator: "cs-01"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("request grant %d %s", rec.Code, rec.Body.String())
	}
	id := data(t, rec)["grant_id"].(string)

	// 无 X-User-Id → 401；非 owner → 403
	if rec := doReq(h, http.MethodPost, "/api/v1/support/grants/"+id+"/confirm", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no user id: %d", rec.Code)
	}
	if rec := doReq(h, http.MethodPost, "/api/v1/support/grants/"+id+"/confirm", "", map[string]string{HeaderUserID: "7"}); rec.Code != http.StatusForbidden {
		t.Fatalf("non-owner: %d", rec.Code)
	}
	// 未确认前自检 → 403，deviceapi 未被调用
	if rec := doReq(h, http.MethodPost, "/api/v1/support/grants/"+id+"/self_check", "", map[string]string{grantcheck.HeaderOperator: "cs-01"}); rec.Code != http.StatusForbidden || len(dev.cmds) != 0 {
		t.Fatalf("self check before confirm: %d %v", rec.Code, dev.cmds)
	}
	rec = doReq(h, http.MethodPost, "/api/v1/support/grants/"+id+"/confirm", "", map[string]string{HeaderUserID: "42"})
	if rec.Code != http.StatusOK || data(t, rec)["status"] != GrantGranted {
		t.Fatalf("confirm: %d %s", rec.Code, rec.Body.String())
	}
	// 二次确认 → 409
	if rec := doReq(h, http.MethodPost, "/api/v1/support/grants/"+id+"/confirm", "", map[string]string{HeaderUserID: "42"}); rec.Code != http.StatusConflict {
		t.Fatalf("double confirm: %d", rec.Code)
	}
	// 客服自检需要 X-Operator
	if rec := doReq(h, http.MethodPost, "/api/v1/support/grants/"+id+"/self_check", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("operator required: %d", rec.Code)
	}
	rec = doReq(h, http.MethodPost, "/api/v1/support/grants/"+id+"/self_check", "", map[string]string{grantcheck.HeaderOperator: "cs-01"})
	if rec.Code != http.StatusOK || data(t, rec)["status"] != "acked" {
		t.Fatalf("self check: %d %s", rec.Code, rec.Body.String())
	}
	rec = doReq(h, http.MethodGet, "/api/v1/support/tickets/T-1/cmds", "", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"cmd_id":"cmd-1"`) {
		t.Fatalf("ticket cmds: %s", rec.Body.String())
	}
	rec = doReq(h, http.MethodPost, "/api/v1/support/grants/"+id+"/revoke", "", map[string]string{HeaderUserID: "42"})
	if rec.Code != http.StatusOK || data(t, rec)["status"] != GrantRevoked {
		t.Fatalf("revoke: %s", rec.Body.String())
	}
	if rec := doReq(h, http.MethodGet, "/api/v1/support/grants/nope", "", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown grant: %d", rec.Code)
	}
}

func TestHandlerBundlesAndAgent(t *testing.T) {
	st := newMemStore(fixedNow)
	st.devices["SN1"] = DeviceInfo{ProductKey: "LM_S1", FWVersion: "1.0"}
	dev := &fakeDev{reported: map[string]string{"work_state": "0", "user_id": "42"}}
	svc := newSvc(st, dev, &fakeTD{})
	h := Routes(svc, "test")

	rec := doReq(h, http.MethodPost, "/api/v1/support/bundles", `{"sn":"SN1","ticket_id":"T-2","user_note":"机器冒烟"}`, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("bundle: %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `"user_id"`) {
		t.Fatal("identity key leaked into bundle response")
	}
	id := data(t, rec)["bundle_id"].(string)
	if rec := doReq(h, http.MethodGet, "/api/v1/support/bundles/"+id, "", nil); rec.Code != http.StatusOK {
		t.Fatalf("get bundle: %d", rec.Code)
	}
	if rec := doReq(h, http.MethodGet, "/api/v1/devices/SN1/bundles", "", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), id) {
		t.Fatalf("list bundles: %s", rec.Body.String())
	}
	if rec := doReq(h, http.MethodPost, "/api/v1/support/bundles", `{"sn":"bad sn!"}`, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad sn: %d", rec.Code)
	}
	// 过期 → 410
	svc.Now = func() time.Time { return fixedNow.Add(BundleTTL + time.Hour) }
	if rec := doReq(h, http.MethodGet, "/api/v1/support/bundles/"+id, "", nil); rec.Code != http.StatusGone {
		t.Fatalf("expired bundle: %d", rec.Code)
	}
	svc.Now = func() time.Time { return fixedNow }

	// Agent：context 需要 X-Agent-Id；untrusted 标注；任何非自检指令路由层 403
	if rec := doReq(h, http.MethodGet, "/api/v1/agent/devices/SN1/context", "", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("agent id required: %d", rec.Code)
	}
	rec = doReq(h, http.MethodGet, "/api/v1/agent/devices/SN1/context", "", map[string]string{HeaderAgentID: "diag-bot"})
	if rec.Code != http.StatusOK || data(t, rec)["untrusted"] != true || !strings.Contains(rec.Body.String(), UntrustedOpen) {
		t.Fatalf("agent context: %d %s", rec.Code, rec.Body.String()[:200])
	}
	if rec := doReq(h, http.MethodPost, "/api/v1/agent/devices/SN1/cmd", `{"action":"pause"}`, map[string]string{HeaderAgentID: "diag-bot"}); rec.Code != http.StatusForbidden {
		t.Fatalf("agent pause must be 403: %d", rec.Code)
	}
	if rec := doReq(h, http.MethodPost, "/api/v1/agent/devices/SN1/self_check", `{"grant_id":"nope"}`, map[string]string{HeaderAgentID: "diag-bot"}); rec.Code != http.StatusNotFound {
		t.Fatalf("agent self_check with unknown grant: %d", rec.Code)
	}
	if svc.M.Get(MAgentRefused) < 1 {
		t.Fatal("agent refusal metric")
	}

	// 保修：响应无判定键
	rec = doReq(h, http.MethodGet, "/api/v1/support/warranty/SN1?ticket_id=T-2", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("warranty: %d %s", rec.Code, rec.Body.String())
	}
	for _, f := range ForbiddenWarrantyKeys {
		if strings.Contains(rec.Body.String(), `"`+f+`"`) {
			t.Fatalf("verdict key %q in response", f)
		}
	}
	// 字典：同人审批 403
	if rec := doReq(h, http.MethodPost, "/internal/support/dict", `{"code":"E_X","severity":"warn","cause":"c","steps":"s","created_by":"a","approved_by":"a"}`, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("dict same approver: %d", rec.Code)
	}
	if rec := doReq(h, http.MethodPost, "/internal/support/dict", `{"code":"E_X","severity":"warn","cause":"c","steps":"s","created_by":"a","approved_by":"b"}`, nil); rec.Code != http.StatusCreated {
		t.Fatalf("dict publish: %d %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, http.MethodGet, "/api/v1/support/dict/E_X", "", nil); rec.Code != http.StatusOK {
		t.Fatalf("dict get: %d", rec.Code)
	}
	if rec := doReq(h, http.MethodGet, "/healthz", "", nil); rec.Code != http.StatusOK {
		t.Fatal("healthz")
	}
	if rec := doReq(h, http.MethodGet, "/metrics", "", nil); !strings.Contains(rec.Body.String(), "support_bundles_generated") {
		t.Fatal("metrics")
	}
}
