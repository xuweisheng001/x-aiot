package grantcheck

import (
	"context"
	"testing"
	"time"
)

func TestSourceFromHeader(t *testing.T) {
	cases := []struct {
		header, body string
		want         string
		ignored      bool
	}{
		{"support", "", "support", false},
		{" Agent ", "", "agent", false},
		{"app", "support", "app", true},  // 请求体想冒充 support：忽略
		{"", "support", "console", true}, // 无头：console，体被忽略
		{"xpilot", "", "console", false}, // 非法头
		{"", "", "console", false},
	}
	for _, c := range cases {
		got, ign := SourceFromHeader(c.header, c.body)
		if got != c.want || ign != c.ignored {
			t.Errorf("SourceFromHeader(%q,%q)=(%s,%v) want (%s,%v)", c.header, c.body, got, ign, c.want, c.ignored)
		}
	}
}

func TestNeedsGrantAndAgentAllowed(t *testing.T) {
	for src, want := range map[string]bool{"app": false, "console": false, "support": true, "agent": true, "": false} {
		if NeedsGrant(src) != want {
			t.Errorf("NeedsGrant(%q)=%v", src, !want)
		}
	}
	if !AgentAllowed("self_check") || AgentAllowed("pause") || AgentAllowed("stop") || AgentAllowed("") {
		t.Fatal("agent must only self_check")
	}
}

// 无 PG 的分支：不需要 grant 直接放行；agent 非自检拒绝；support 无 checker 拒绝（fail-closed）。
func TestCheckWithoutStore(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	if d, err := Check(ctx, nil, "SN1", "app", "stop", now); err != nil || !d.OK {
		t.Fatalf("app must pass: %+v %v", d, err)
	}
	if d, _ := Check(ctx, nil, "SN1", "agent", "pause", now); d.OK || d.Reason != "agent may only self_check" {
		t.Fatalf("agent pause must be refused: %+v", d)
	}
	if d, _ := Check(ctx, nil, "SN1", "support", "self_check", now); d.OK || d.Reason != "grant checker not configured" {
		t.Fatalf("support without store must be refused: %+v", d)
	}
}
