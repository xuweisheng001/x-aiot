package deviceapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDeviceAuthMissingID(t *testing.T) {
	h := DeviceAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(DeviceIDFrom(r.Context())))
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/x", nil)) // 无头、无路径 {sn}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rec.Code)
	}
	rec = httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/x", nil)
	req.Header.Set("X-Device-Id", "DEV1")
	h.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Body.String() != "DEV1" {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestDeviceRateKeyUsesPattern(t *testing.T) {
	mux := http.NewServeMux()
	var keys []string
	mux.Handle("POST /api/v1/devices/{sn}/cmd", DeviceAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys = append(keys, *DeviceRateKey(r))
	})))
	for _, sn := range []string{"A", "B"} {
		req := httptest.NewRequest("POST", "/api/v1/devices/"+sn+"/cmd", nil)
		req.Header.Set("X-Device-Id", "SAME")
		mux.ServeHTTP(httptest.NewRecorder(), req)
	}
	if len(keys) != 2 || keys[0] != keys[1] || keys[0] != "SAME|POST /api/v1/devices/{sn}/cmd" {
		t.Fatalf("keys=%v", keys)
	}
	if DeviceRateKey(httptest.NewRequest("GET", "/", nil)) != nil {
		t.Fatal("no device id → nil key")
	}
}
