package auth

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAuthCache(t *testing.T) {
	c := NewAuthCache()
	t0 := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	k := CacheKey("SIM00001", "fp1")
	if c.Recent(k, t0, time.Hour) {
		t.Fatal("empty cache must miss")
	}
	c.Remember(k, t0)
	if !c.Recent(k, t0.Add(59*time.Minute), time.Hour) {
		t.Fatal("within ttl must hit")
	}
	if c.Recent(k, t0.Add(61*time.Minute), time.Hour) {
		t.Fatal("past ttl must miss")
	}
	if c.Len() != 0 {
		t.Fatalf("expired entry should be lazily removed, len=%d", c.Len())
	}
	c.Remember("a|", t0)
	c.Remember("b|", t0.Add(30*time.Minute))
	if n := c.Sweep(t0.Add(90*time.Minute), time.Hour); n != 1 || c.Len() != 1 {
		t.Fatalf("sweep removed %d, len=%d", n, c.Len())
	}
	// 并发安全（-race）
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := CacheKey("SN", string(rune('A'+i%26)))
			c.Remember(key, t0)
			c.Recent(key, t0, time.Hour)
			c.Sweep(t0, time.Hour)
		}(i)
	}
	wg.Wait()
}

// fail-open 决策矩阵：只有「store 出错 + 开关开 + 缓存 TTL 内命中」才放行；明确拒绝永远拒绝。
func TestAuthenticator_FailOpenMatrix(t *testing.T) {
	t0 := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	good := func() *fakeStore {
		return &fakeStore{
			devices: map[string]string{"SIM00001": "activated", "SIM00002": "manufactured"},
			certs:   map[string][2]string{"fp1": {"SIM00001", "active"}, "fp2": {"SIM00001", "revoked"}},
		}
	}
	type step struct {
		client, fp string
		storeFail  bool
		at         time.Duration // 相对 t0
		wantOK     bool
		wantReason string
	}
	tests := []struct {
		name     string
		failOpen bool
		steps    []step
	}{
		{"store error, switch off → deny", false, []step{
			{"SIM00001", "fp1", false, 0, true, ""},
			{"SIM00001", "fp1", true, time.Minute, false, ReasonStoreError},
		}},
		{"store error, switch on, cache hit → allow", true, []step{
			{"SIM00001", "fp1", false, 0, true, ""},
			{"SIM00001", "fp1", true, time.Minute, true, ReasonFailOpen},
		}},
		{"store error, switch on, cache expired → deny", true, []step{
			{"SIM00001", "fp1", false, 0, true, ""},
			{"SIM00001", "fp1", true, 25 * time.Hour, false, ReasonStoreError},
		}},
		{"store error, switch on, never seen → deny", true, []step{
			{"SIM00001", "fp1", true, 0, false, ReasonStoreError},
		}},
		{"store error, switch on, different cert → deny", true, []step{
			{"SIM00001", "fp1", false, 0, true, ""},
			{"SIM00001", "fp9", true, time.Minute, false, ReasonStoreError},
		}},
		{"explicit deny (revoked cert) with cache hit → still deny", true, []step{
			{"SIM00001", "fp1", false, 0, true, ""},
			{"SIM00001", "fp2", false, time.Minute, false, "cert status revoked"},
		}},
		{"explicit deny (not activated) with cache hit → still deny", true, []step{
			{"SIM00002", "", false, 0, false, "device status manufactured"},
			{"SIM00002", "", false, time.Minute, false, "device status manufactured"},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := good()
			a := NewAuthenticator(st, tc.failOpen, DefaultCacheTTL, NewMetrics())
			for i, s := range tc.steps {
				st.fail = s.storeFail
				a.Now = func() time.Time { return t0.Add(s.at) }
				ok, reason, err := a.Authenticate(context.Background(), s.client, s.fp)
				if ok != s.wantOK {
					t.Fatalf("step %d: ok=%v reason=%q err=%v want ok=%v", i, ok, reason, err, s.wantOK)
				}
				if s.wantReason != "" && reason != s.wantReason {
					t.Fatalf("step %d: reason=%q want %q", i, reason, s.wantReason)
				}
				if s.storeFail && err == nil {
					t.Fatalf("step %d: store error must be surfaced via err even when fail-open allows", i)
				}
			}
		})
	}
}

func TestAuthenticator_Metrics(t *testing.T) {
	st := &fakeStore{devices: map[string]string{"SIM00001": "activated"}}
	m := NewMetrics()
	a := NewAuthenticator(st, true, time.Hour, m)
	_, _, _ = a.Authenticate(context.Background(), "SIM00001", "")
	_, _, _ = a.Authenticate(context.Background(), "SIM00009", "")
	st.fail = true
	_, _, _ = a.Authenticate(context.Background(), "SIM00001", "")
	if m.Get(MAuthAllow) != 2 || m.Get(MAuthDeny) != 1 || m.Get(MFailOpenAllowed) != 1 || m.Get(MStoreError) != 1 {
		t.Fatalf("allow=%d deny=%d failopen=%d storeerr=%d", m.Get(MAuthAllow), m.Get(MAuthDeny), m.Get(MFailOpenAllowed), m.Get(MStoreError))
	}
	if m.Get("deny_reason_device_not_found") != 1 {
		t.Fatalf("deny reason slug counter missing")
	}
	s := &Server{Store: st, Auth: a, M: m}
	h := s.Handler("auth-svc", "test")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{"auth_failopen_allowed 1", "auth_store_error 1", "deny_reason_device_not_found 1", "cache_size "} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestReasonSlug(t *testing.T) {
	for in, want := range map[string]string{
		"store error":                "store_error",
		"device status manufactured": "device_status_manufactured",
		"cert sn mismatch":           "cert_sn_mismatch",
		"device status a b c d":      "device_status_a", // 只取前 3 个词
		"":                           "unknown",
		"Cache-FailOpen":             "cache_failopen",
	} {
		if got := reasonSlug(in); got != want {
			t.Errorf("slug(%q)=%q want %q", in, got, want)
		}
	}
}
