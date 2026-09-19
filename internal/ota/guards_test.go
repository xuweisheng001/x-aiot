package ota

import (
	"testing"
	"time"
)

// 绝对数熔断：样本不足时比例规则永不触发，但 fail >= minAbsFail 即熔断；关闭（<=0）时与 ShouldFuse 完全一致。
func TestShouldFuseAbs(t *testing.T) {
	cases := []struct {
		name                   string
		ok, fail               int
		threshold              float64
		minSamples, minAbsFail int
		want                   bool
	}{
		{"below min samples, abs reached → fuse", 0, 5, 0.02, 50, 5, true},
		{"below min samples, abs not reached", 0, 4, 0.02, 50, 5, false},
		{"0.1% stage: 30 devices all failed, abs fuses", 0, 30, 0.02, 50, 5, true},
		{"many ok + 5 fail: ratio tiny but abs fuses", 10000, 5, 0.02, 50, 5, true},
		{"many ok + 4 fail: neither rule", 10000, 4, 0.02, 50, 5, false},
		{"abs disabled (0): 49 fails below min samples not fuse", 0, 49, 0.02, 50, 0, false},
		{"abs disabled (-1): same as ratio rule, fuse", 97, 3, 0.02, 50, -1, true},
		{"abs disabled: ratio equal threshold not fuse", 98, 2, 0.02, 50, 0, false},
		{"negative inputs", -1, 10, 0.02, 50, 5, false},
		{"zero everything", 0, 0, 0.02, 50, 5, false},
		{"minAbsFail 1: single failure fuses immediately", 0, 1, 0.02, 50, 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ShouldFuseAbs(c.ok, c.fail, c.threshold, c.minSamples, c.minAbsFail); got != c.want {
				t.Errorf("ShouldFuseAbs(%d,%d,%v,%d,%d)=%v want %v", c.ok, c.fail, c.threshold, c.minSamples, c.minAbsFail, got, c.want)
			}
		})
	}
	// 关闭绝对数时逐点与 ShouldFuse 一致
	for ok := 0; ok <= 120; ok += 7 {
		for fail := 0; fail <= 60; fail += 3 {
			if ShouldFuseAbs(ok, fail, 0.02, 50, 0) != ShouldFuse(ok, fail, 0.02, 50) {
				t.Fatalf("abs disabled must equal ShouldFuse at ok=%d fail=%d", ok, fail)
			}
		}
	}
}

func TestExpectedNextStage(t *testing.T) {
	cases := []struct {
		latest string
		exists bool
		want   string
		ok     bool
	}{
		{"", false, "0.1", true},
		{"50", false, "0.1", true}, // exists=false 时忽略 latest
		{"0.1", true, "1", true},
		{"1", true, "10", true},
		{"10", true, "50", true},
		{"50", true, "100", true},
		{"100", true, "", false},
		{"bogus", true, "", false},
	}
	for _, c := range cases {
		got, ok := ExpectedNextStage(c.latest, c.exists)
		if got != c.want || ok != c.ok {
			t.Errorf("ExpectedNextStage(%q,%v)=(%q,%v) want (%q,%v)", c.latest, c.exists, got, ok, c.want, c.ok)
		}
	}
}

func TestIsStale(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	older := 30 * time.Minute
	cases := []struct {
		name    string
		status  string
		updated time.Time
		older   time.Duration
		want    bool
	}{
		{"notified 31m ago", TaskNotified, now.Add(-31 * time.Minute), older, true},
		{"downloading 31m ago", TaskDownloading, now.Add(-31 * time.Minute), older, true},
		{"verifying 31m ago", TaskVerifying, now.Add(-31 * time.Minute), older, true},
		{"notified 29m ago", TaskNotified, now.Add(-29 * time.Minute), older, false},
		{"exactly at boundary not stale", TaskNotified, now.Add(-older), older, false},
		{"pending never stale (not yet dispatched)", TaskPending, now.Add(-24 * time.Hour), older, false},
		{"success never stale", TaskSuccess, now.Add(-24 * time.Hour), older, false},
		{"failed never stale", TaskFailed, now.Add(-24 * time.Hour), older, false},
		{"rolled_back never stale", TaskRolledBack, now.Add(-24 * time.Hour), older, false},
		{"olderThan 0 disables", TaskNotified, now.Add(-24 * time.Hour), 0, false},
		{"unknown status", "bogus", now.Add(-24 * time.Hour), older, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsStale(c.status, c.updated, now, c.older); got != c.want {
				t.Errorf("IsStale(%s)=%v want %v", c.status, got, c.want)
			}
		})
	}
	if len(StaleStatuses()) != 3 {
		t.Error("StaleStatuses size")
	}
}
