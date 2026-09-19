package deviceapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// 全组合表驱动：来源 × work_state × lock。拒绝一律 403 / 10003。
func TestDecideJobStart(t *testing.T) {
	cases := []struct {
		source  string
		ws      int
		locked  bool
		allowed bool
		msg     string
	}{
		// fleet 来源：只有空闲且未锁才放行
		{"fleet", 0, false, true, ""},
		{"fleet", 0, true, false, "locked"},
		{"fleet", 2, false, false, "not idle"},
		{"fleet", 2, true, false, "locked"},
		{"fleet", -1, false, false, "not idle"}, // 影子缺失 → fail-closed
		{"fleet", -1, true, false, "locked"},
		// 非 fleet 来源：不管设备多空闲都拒绝
		{"app", 0, false, false, "X-Source: fleet"},
		{"app", 0, true, false, "X-Source: fleet"},
		{"app", 2, false, false, "X-Source: fleet"},
		{"app", 2, true, false, "X-Source: fleet"},
		{"app", -1, false, false, "X-Source: fleet"},
		{"app", -1, true, false, "X-Source: fleet"},
		{"", 0, false, false, "X-Source: fleet"},
		{"", 0, true, false, "X-Source: fleet"},
		{"", 2, false, false, "X-Source: fleet"},
		{"", 2, true, false, "X-Source: fleet"},
		{"", -1, false, false, "X-Source: fleet"},
		{"", -1, true, false, "X-Source: fleet"},
		// 头的大小写与空白不影响判定；支持类来源仍然不行
		{" FLEET ", 0, false, true, ""},
		{"support", 0, false, false, "X-Source: fleet"},
		{"fleet-scheduler", 0, false, false, "X-Source: fleet"},
	}
	for _, c := range cases {
		d := DecideJobStart(c.source, c.ws, c.locked)
		if d.Allowed != c.allowed {
			t.Errorf("DecideJobStart(%q,%d,%v).Allowed=%v want %v", c.source, c.ws, c.locked, d.Allowed, c.allowed)
			continue
		}
		if c.allowed {
			if d.HTTPStatus != http.StatusOK || d.BizCode != httpx.CodeOK {
				t.Errorf("DecideJobStart(%q,%d,%v)=%+v", c.source, c.ws, c.locked, d)
			}
			continue
		}
		if d.HTTPStatus != http.StatusForbidden || d.BizCode != httpx.CodeDenied || !strings.Contains(d.Msg, c.msg) {
			t.Errorf("DecideJobStart(%q,%d,%v)=%+v want 403/10003 containing %q", c.source, c.ws, c.locked, d, c.msg)
		}
	}
}

func TestShadowWorkStateAndLocked(t *testing.T) {
	cases := []struct {
		rep    map[string]string
		ws     int
		locked bool
	}{
		{map[string]string{}, -1, false},
		{map[string]string{"work_state": "0"}, 0, false},
		{map[string]string{"work_state": "2"}, 2, false},
		{map[string]string{"work_state": "2.0"}, 2, false},
		{map[string]string{"work_state": "abc"}, -1, false},
		{map[string]string{"work_state": "0", "lock_state": "1"}, 0, true},
		{map[string]string{"work_state": "0", "lock_state": "0"}, 0, false},
		{map[string]string{"work_state": "0", "lock": "true"}, 0, true},
		{map[string]string{"work_state": "0", "lock": "false"}, 0, false},
		{map[string]string{"work_state": "0", "lock": "maybe"}, 0, false},
	}
	for _, c := range cases {
		if got := ShadowWorkState(c.rep); got != c.ws {
			t.Errorf("ShadowWorkState(%v)=%d want %d", c.rep, got, c.ws)
		}
		if got := ShadowLocked(c.rep); got != c.locked {
			t.Errorf("ShadowLocked(%v)=%v want %v", c.rep, got, c.locked)
		}
	}
}

// postCmd 的 job_start 分支：影子未知（无 Redis）一律拒绝，且不产生审计与下行指令。
func TestPostCmdJobStartRefused(t *testing.T) {
	s, mq, js, _, _ := newTestServer()
	h := s.Handler()
	for i, hdr := range []map[string]string{nil, {"X-Source": "fleet"}, {"X-Source": "app"}} {
		sn := "XTJS" + string(rune('A'+i))
		rec := do(h, "POST", "/api/v1/devices/"+sn+"/cmd", `{"action":"job_start","params":{"job_id":"j1"}}`, hdr)
		code, _ := decode(t, rec)
		if rec.Code != http.StatusForbidden || code != httpx.CodeDenied {
			t.Fatalf("hdr=%v status=%d code=%d body=%s", hdr, rec.Code, code, rec.Body.String())
		}
	}
	if len(mq.msgs) != 0 || len(js.subj) != 0 {
		t.Fatalf("refused job_start must not publish: mqtt=%v audit=%v", mq.msgs, js.subj)
	}
	// 白名单本身放行 job_start（拒绝来自来源/空闲判定，而不是 400 未知动作）
	if d := DecideCmd("job_start"); !d.Allowed {
		t.Fatalf("job_start must be whitelisted: %+v", d)
	}
}

// 批量影子接口的参数校验（Redis 无关的分支）。
func TestPostShadowsValidation(t *testing.T) {
	s, _, _, _, _ := newTestServer()
	h := s.Handler()
	if rec := do(h, "POST", "/api/v1/devices/shadows", `not json`, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json: %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/v1/devices/shadows", `{"sns":[]}`, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty sns: %d", rec.Code)
	}
	var b strings.Builder
	b.WriteString(`{"sns":[`)
	for i := 0; i < MaxShadowBatch+1; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"SN"`)
	}
	b.WriteString(`]}`)
	rec := do(h, "POST", "/api/v1/devices/shadows", b.String(), nil)
	code, _ := decode(t, rec)
	if rec.Code != http.StatusBadRequest || code != httpx.CodeBadParam {
		t.Fatalf("over limit: status=%d code=%d", rec.Code, code)
	}
	// 上限内、无 Redis：返回空影子映射
	rec = do(h, "POST", "/api/v1/devices/shadows", `{"sns":["SN1","SN2","SN1"]}`, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"shadows"`) {
		t.Fatalf("ok case: %d %s", rec.Code, rec.Body.String())
	}
}
