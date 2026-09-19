package cf001

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

var fixedNow = time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

func TestCheckFreshness(t *testing.T) {
	w := 5 * time.Minute
	sec := func(d time.Duration) string { return strconv.FormatInt(fixedNow.Add(d).Unix(), 10) }
	ms := func(d time.Duration) string { return strconv.FormatInt(fixedNow.Add(d).UnixMilli(), 10) }
	tests := []struct {
		name, ts string
		wantErr  bool
	}{
		{"seconds now", sec(0), false},
		{"seconds 4m59s old", sec(-4*time.Minute - 59*time.Second), false},
		{"seconds 5m01s old → stale", sec(-5*time.Minute - time.Second), true},
		{"seconds 5m01s in future → stale", sec(5*time.Minute + time.Second), true},
		{"millis now", ms(0), false},
		{"millis 2m old", ms(-2 * time.Minute), false},
		{"millis 6m old → stale", ms(-6 * time.Minute), true},
		{"millis 1h future → stale", ms(time.Hour), true},
		{"not a number", "abc", true},
		{"negative", "-1", true},
		{"zero", "0", true},
		{"empty", "", true},
		{"14 digits (micros) → reject", "17583000000000000"[:14], true},
		{"whitespace tolerated", " " + sec(0) + " ", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckFreshness(tc.ts, fixedNow, w)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ts=%q err=%v wantErr=%v", tc.ts, err, tc.wantErr)
			}
			if err != nil && !errors.Is(err, ErrStale) {
				t.Fatalf("error must wrap ErrStale: %v", err)
			}
		})
	}
	// window<=0 回退默认 5 分钟
	if err := CheckFreshness(sec(-4*time.Minute), fixedNow, 0); err != nil {
		t.Fatal(err)
	}
}

func TestMemNonceStore(t *testing.T) {
	m := NewMemNonceStore()
	now := fixedNow
	m.Now = func() time.Time { return now }
	ctx := context.Background()
	if dup, _ := m.Seen(ctx, "n1", time.Minute); dup {
		t.Fatal("first use must not be dup")
	}
	if dup, _ := m.Seen(ctx, "n1", time.Minute); !dup {
		t.Fatal("second use must be dup")
	}
	now = now.Add(2 * time.Minute)
	if dup, _ := m.Seen(ctx, "n1", time.Minute); dup {
		t.Fatal("after ttl nonce may be reused")
	}
}

type fakeNonces struct {
	seen map[string]bool
	err  error
}

func (f *fakeNonces) Seen(_ context.Context, n string, _ time.Duration) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	if f.seen[n] {
		return true, nil
	}
	f.seen[n] = true
	return false, nil
}

// 构造一个没有数据库的 Service：所有断言都发生在 checkReplay 阻断之后、数据库之前。
func replayService(t *testing.T, nonces NonceStore) *Service {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return &Service{Key: key, Now: func() time.Time { return fixedNow }, Nonces: nonces, FreshnessWindow: 5 * time.Minute, C: &Counters{}}
}

func encPayload(t *testing.T, pub *rsa.PublicKey, digest, nonce string, ts time.Time) string {
	t.Helper()
	ct, err := EncryptOAEP(pub, []byte(fmt.Sprintf("%s|%s|%d", digest, nonce, ts.Unix())))
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(ct)
}

func TestCheckReplay(t *testing.T) {
	d := Digest("u", "m", "s", "x")
	t.Run("fresh then replay", func(t *testing.T) {
		svc := replayService(t, &fakeNonces{seen: map[string]bool{}})
		p := Payload{Digest: d, Nonce: "n-1", Timestamp: fixedNow.Unix()}
		if err := svc.checkReplay(context.Background(), p); err != nil {
			t.Fatalf("first: %v", err)
		}
		err := svc.checkReplay(context.Background(), p)
		if !errors.Is(err, ErrBadParam) || !strings.Contains(err.Error(), "replay") {
			t.Fatalf("second must be replay/ErrBadParam: %v", err)
		}
		if svc.C.Replay.Load() != 1 {
			t.Fatalf("replay counter=%d", svc.C.Replay.Load())
		}
	})
	t.Run("stale before nonce", func(t *testing.T) {
		fn := &fakeNonces{seen: map[string]bool{}}
		svc := replayService(t, fn)
		err := svc.checkReplay(context.Background(), Payload{Digest: d, Nonce: "n-2", Timestamp: fixedNow.Add(-time.Hour).Unix()})
		if !errors.Is(err, ErrBadParam) || !errors.Is(err, ErrStale) {
			t.Fatalf("want stale: %v", err)
		}
		if fn.seen["n-2"] {
			t.Fatal("stale request must not consume nonce")
		}
	})
	t.Run("nonce store error → unavailable (fail-closed)", func(t *testing.T) {
		svc := replayService(t, &fakeNonces{err: errors.New("redis down")})
		err := svc.checkReplay(context.Background(), Payload{Digest: d, Nonce: "n-3", Timestamp: fixedNow.Unix()})
		if !errors.Is(err, ErrUnavailable) || svc.C.NonceStoreErr.Load() != 1 {
			t.Fatalf("want ErrUnavailable + counter: %v", err)
		}
	})
	t.Run("no nonce store configured → unavailable", func(t *testing.T) {
		svc := replayService(t, nil)
		if err := svc.checkReplay(context.Background(), Payload{Digest: d, Nonce: "n", Timestamp: fixedNow.Unix()}); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("want ErrUnavailable: %v", err)
		}
	})
	t.Run("skip switch bypasses everything", func(t *testing.T) {
		svc := replayService(t, nil)
		svc.SkipFreshness = true
		if err := svc.checkReplay(context.Background(), Payload{Digest: d, Nonce: "n", Timestamp: 1}); err != nil || svc.C.SkippedFreshness.Load() != 1 {
			t.Fatalf("skip: err=%v counter=%d", err, svc.C.SkippedFreshness.Load())
		}
	})
}

// handler 层：重放与过期请求在 /sign 返回 400 code 10001，且不会触达数据库（Service.DB 为 nil 也不 panic）。
func TestSignHandler_RejectsReplayAndStale(t *testing.T) {
	fn := &fakeNonces{seen: map[string]bool{"used-nonce": true}}
	svc := replayService(t, fn)
	h := Routes(svc, "test")
	d := Digest("u", "m", "s", "x")
	post := func(payloadB64 string) *httptest.ResponseRecorder {
		body := fmt.Sprintf(`{"order_no":"PO-1","payload_b64":%q}`, payloadB64)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/oem/sign", strings.NewReader(body))
		req.RemoteAddr = "10.1.1.1:1"
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if rr := post(encPayload(t, &svc.Key.PublicKey, d, "used-nonce", fixedNow)); rr.Code != http.StatusBadRequest ||
		!strings.Contains(rr.Body.String(), `"code":10001`) || !strings.Contains(rr.Body.String(), "replay") {
		t.Fatalf("replay: %d %s", rr.Code, rr.Body.String())
	}
	if rr := post(encPayload(t, &svc.Key.PublicKey, d, "fresh-nonce", fixedNow.Add(-time.Hour))); rr.Code != http.StatusBadRequest ||
		!strings.Contains(rr.Body.String(), "stale") {
		t.Fatalf("stale: %d %s", rr.Code, rr.Body.String())
	}
	svc.Nonces = &fakeNonces{err: errors.New("redis down")}
	if rr := post(encPayload(t, &svc.Key.PublicKey, d, "n", fixedNow)); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("nonce store down must be 503: %d %s", rr.Code, rr.Body.String())
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rr.Body.String(), "sign_replay 1") || !strings.Contains(rr.Body.String(), "sign_stale 1") || !strings.Contains(rr.Body.String(), "sign_nonce_store_err 1") {
		t.Fatalf("metrics:\n%s", rr.Body.String())
	}
}

func TestLoadKeyStrict(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadKeyStrict(filepath.Join(dir, "missing.pem")); err == nil {
		t.Fatal("missing file must error in strict mode")
	}
	bad := filepath.Join(dir, "bad.pem")
	_ = os.WriteFile(bad, []byte("garbage"), 0o600)
	if _, err := LoadKeyStrict(bad); err == nil {
		t.Fatal("unparsable file must error in strict mode")
	}
	// 对照：宽松模式对缺失文件生成临时密钥，不报错
	if _, gen, err := LoadOrGenerateKey(filepath.Join(dir, "missing.pem")); err != nil || !gen {
		t.Fatalf("lenient fallback: gen=%v err=%v", gen, err)
	}
	// 合法密钥两种模式都能读
	k, _ := rsa.GenerateKey(rand.Reader, 2048)
	good := filepath.Join(dir, "good.pem")
	_ = os.WriteFile(good, pemPKCS1(k), 0o600)
	if _, err := LoadKeyStrict(good); err != nil {
		t.Fatal(err)
	}
}

// Redis SETNX 行为：同 nonce 第二次 dup，TTL 生效。
func TestIntegration_RedisNonceStore(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	rdb := config.MustRedis()
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st := &RedisNonceStore{RDB: rdb}
	n := fmt.Sprintf("it-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(context.Background(), NonceKey(n)) })
	if dup, err := st.Seen(ctx, n, 2*time.Second); err != nil || dup {
		t.Fatalf("first: dup=%v err=%v", dup, err)
	}
	if dup, err := st.Seen(ctx, n, 2*time.Second); err != nil || !dup {
		t.Fatalf("second: dup=%v err=%v", dup, err)
	}
	if ttl, _ := rdb.TTL(ctx, NonceKey(n)).Result(); ttl <= 0 || ttl > 2*time.Second {
		t.Fatalf("ttl=%s", ttl)
	}
	time.Sleep(2500 * time.Millisecond)
	if dup, err := st.Seen(ctx, n, time.Second); err != nil || dup {
		t.Fatalf("after expiry: dup=%v err=%v", dup, err)
	}
}

func pemPKCS1(k *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
}
