package cf001

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { httpx.OK(w, "ok") })
}

// IP 维度：10 次/分钟，第 11 次 429 code 100012。
func TestIPLimiter_11thCallRateLimited(t *testing.T) {
	h := IPLimiter()(okHandler())
	for i := 1; i <= 12; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/oem/sign", strings.NewReader(`{}`))
		req.RemoteAddr = "10.0.0.7:5555"
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if i <= 10 && rr.Code != http.StatusOK {
			t.Fatalf("call %d: status %d", i, rr.Code)
		}
		if i > 10 {
			if rr.Code != http.StatusTooManyRequests || !strings.Contains(rr.Body.String(), `"code":100012`) {
				t.Fatalf("call %d: status %d body %s", i, rr.Code, rr.Body.String())
			}
		}
	}
	// 另一个 IP 不受影响
	req := httptest.NewRequest(http.MethodPost, "/api/v1/oem/sign", nil)
	req.RemoteAddr = "10.0.0.8:5555"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("other ip: %d", rr.Code)
	}
}

// 设备维度：Security 之后按 deviceId+path，rps 1 burst 2 → 同设备第 3 次 429。
func TestDeviceLimiter_3rdCallRateLimited(t *testing.T) {
	h := httpx.Chain(okHandler(), Security, DeviceLimiter())
	call := func(sn, header string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/oem/verify", strings.NewReader(`{"sn":"`+sn+`"}`))
		if header != "" {
			req.Header.Set("X-Device-Id", header)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	for i := 1; i <= 2; i++ {
		if rr := call("SN_A", ""); rr.Code != http.StatusOK {
			t.Fatalf("SN_A call %d: %d %s", i, rr.Code, rr.Body.String())
		}
	}
	if rr := call("SN_A", ""); rr.Code != http.StatusTooManyRequests || !strings.Contains(rr.Body.String(), `"code":100012`) {
		t.Fatalf("SN_A 3rd call: %d %s", rr.Code, rr.Body.String())
	}
	// header 与 body 是同一维度
	if rr := call("", "SN_A"); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("SN_A via header should share bucket: %d", rr.Code)
	}
	// 其它设备独立
	if rr := call("SN_B", ""); rr.Code != http.StatusOK {
		t.Fatalf("SN_B: %d", rr.Code)
	}
	// 没有身份 → Security 400
	if rr := call("", ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("no identity: %d", rr.Code)
	}
}

func TestSecurityRestoresBody(t *testing.T) {
	var seen string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b struct{ SN string }
		_ = httpx.Decode(r, &b)
		seen = b.SN
		httpx.OK(w, nil)
	})
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"sn":"DEV1"}`))
	rr := httptest.NewRecorder()
	Security(inner).ServeHTTP(rr, req)
	if seen != "DEV1" || req.Header.Get("X-Device-Id") != "DEV1" {
		t.Fatalf("seen=%q header=%q", seen, req.Header.Get("X-Device-Id"))
	}
}
