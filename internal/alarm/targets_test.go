package alarm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeLookup struct {
	targets []Target
	err     error
	delay   time.Duration
	calls   int
}

func (f *fakeLookup) Targets(ctx context.Context, _ string) ([]Target, error) {
	f.calls++
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return f.targets, f.err
}

// resolveTargets 的三种情形：正常返回、失败 / 超时回退、fleet 未配置。
func TestResolveTargets(t *testing.T) {
	t.Run("正常返回", func(t *testing.T) {
		svc := &Service{M: NewMetrics(), Targets: &fakeLookup{targets: []Target{
			{UserID: 1, Role: "org_admin", Source: "org"}, {UserID: 7, Source: "personal"}}}}
		got := svc.resolveTargets(context.Background(), "SN1")
		if len(got) != 2 || got[0].UserID != 1 || got[1].Source != "personal" {
			t.Fatalf("targets=%+v", got)
		}
		if svc.M.Get("targets_resolved") != 1 || svc.M.Get("targets_fallback") != 0 {
			t.Fatalf("resolved=%d fallback=%d", svc.M.Get("targets_resolved"), svc.M.Get("targets_fallback"))
		}
	})

	t.Run("出错回退个人绑定", func(t *testing.T) {
		svc := &Service{M: NewMetrics(), Targets: &fakeLookup{err: errors.New("boom")}}
		if got := svc.resolveTargets(context.Background(), "SN1"); got != nil {
			t.Fatalf("targets=%+v want nil", got)
		}
		if svc.M.Get("targets_fallback") != 1 {
			t.Fatalf("targets_fallback=%d", svc.M.Get("targets_fallback"))
		}
	})

	t.Run("超时回退且不拖慢告警", func(t *testing.T) {
		svc := &Service{M: NewMetrics(), Targets: &fakeLookup{delay: 3 * time.Second, targets: []Target{{UserID: 1}}}}
		start := time.Now()
		if got := svc.resolveTargets(context.Background(), "SN1"); got != nil {
			t.Fatalf("targets=%+v want nil", got)
		}
		if el := time.Since(start); el > 2*time.Second {
			t.Fatalf("resolveTargets blocked %s, must cap at %s", el, TargetsTimeout)
		}
		if svc.M.Get("targets_fallback") != 1 {
			t.Fatalf("targets_fallback=%d", svc.M.Get("targets_fallback"))
		}
	})

	t.Run("fleet 未配置则完全不查", func(t *testing.T) {
		svc := &Service{M: NewMetrics()}
		if got := svc.resolveTargets(context.Background(), "SN1"); got != nil {
			t.Fatalf("targets=%+v want nil", got)
		}
		if svc.M.Get("targets_fallback") != 0 || svc.M.Get("targets_resolved") != 0 {
			t.Fatal("未配置时不该产生任何计数（默认行为与 BL1 一致）")
		}
	})
}

// NewTargetLookup：URL 为空返回 nil 接口（不是「非 nil 接口包 nil 指针」，否则 resolveTargets 会照查不误）。
func TestNewTargetLookup(t *testing.T) {
	for _, base := range []string{"", "   "} {
		if lk := NewTargetLookup(base); lk != nil {
			t.Fatalf("NewTargetLookup(%q)=%v want nil", base, lk)
		}
	}
	lk := NewTargetLookup("http://127.0.0.1:8094/")
	c, ok := lk.(*HTTPTargets)
	if !ok || c.Base != "http://127.0.0.1:8094" || c.HTTP.Timeout != TargetsTimeout {
		t.Fatalf("client=%+v", lk)
	}
}

// HTTP 客户端：解析 fleet-svc 的 {code,data{targets}} 信封；非 200 与坏 json 都算失败（调用方回退）。
func TestHTTPTargets(t *testing.T) {
	var gotSN string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSN = r.URL.Query().Get("sn")
		switch gotSN {
		case "BAD":
			w.WriteHeader(http.StatusInternalServerError)
		case "GARBAGE":
			fmt.Fprint(w, "not json")
		default:
			fmt.Fprint(w, `{"code":0,"data":{"sn":"SN1","org_id":3,"targets":[{"user_id":5,"role":"teacher","source":"org"}]}}`)
		}
	}))
	defer srv.Close()
	c := NewTargetLookup(srv.URL)

	got, err := c.Targets(context.Background(), "SN1")
	if err != nil || len(got) != 1 || got[0].UserID != 5 || got[0].Role != "teacher" || got[0].Source != "org" {
		t.Fatalf("targets=%+v err=%v", got, err)
	}
	if gotSN != "SN1" {
		t.Fatalf("sn query=%q", gotSN)
	}
	if _, err := c.Targets(context.Background(), "BAD"); err == nil {
		t.Fatal("500 must be an error")
	}
	if _, err := c.Targets(context.Background(), "GARBAGE"); err == nil {
		t.Fatal("bad json must be an error")
	}
}
