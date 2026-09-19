package probe

import (
	"testing"
	"time"
)

func TestVerdict(t *testing.T) {
	slo := 3 * time.Second
	cases := []struct {
		name    string
		found   bool
		latency time.Duration
		want    Status
	}{
		{"fast", true, 200 * time.Millisecond, StatusOK},
		{"exactly at SLO is still ok", true, slo, StatusOK},
		{"one ms over SLO", true, slo + time.Millisecond, StatusSlow},
		{"way over", true, 30 * time.Second, StatusSlow},
		{"never arrived", false, 0, StatusMissing},
		{"never arrived ignores latency", false, time.Millisecond, StatusMissing},
		// 设备时间比服务端超前时 notified_at - event_ts 为负；这是时钟问题不是链路问题，
		// 按 0 处理判 ok，否则每次时钟漂移都会误报成 slow
		{"negative latency from clock skew", true, -2 * time.Second, StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, reason := Verdict(c.found, c.latency, slo)
			if got != c.want {
				t.Fatalf("got %s (%s) want %s", got, reason, c.want)
			}
			if got != StatusOK && reason == "" {
				t.Fatal("non-ok verdict must carry a reason")
			}
			if got == StatusOK && reason != "" {
				t.Fatalf("ok verdict must not carry a reason: %q", reason)
			}
		})
	}
	if s, _ := Verdict(true, 4*time.Second, 0); s != StatusSlow {
		t.Fatalf("zero slo must fall back to %s: got %s", DefaultSLO, s)
	}
	if s, _ := Verdict(true, 2*time.Second, 0); s != StatusOK {
		t.Fatal("2s is inside the default 3s SLO")
	}
}

func TestFatalOnlyForMissing(t *testing.T) {
	for st, want := range map[Status]bool{
		StatusOK: false, StatusSlow: false, StatusError: false, StatusMissing: true,
	} {
		if got := (Result{Status: st}).Fatal(); got != want {
			t.Errorf("%s Fatal()=%v want %v", st, got, want)
		}
	}
}

// 探针间隔必须大于 squelch 窗口，否则第二发被聚合吞掉，会把正常聚合误报成漏告警。
func TestIntervalSane(t *testing.T) {
	cases := []struct {
		name              string
		interval, squelch time.Duration
		ok                bool
	}{
		{"10 min over 5 min squelch", 10 * time.Minute, 5 * time.Minute, true},
		{"equal to squelch is not enough", 5 * time.Minute, 5 * time.Minute, false},
		{"below squelch", time.Minute, 5 * time.Minute, false},
		{"no squelch configured", time.Minute, 0, true},
		{"zero interval", 0, 5 * time.Minute, false},
		{"negative interval", -time.Minute, 5 * time.Minute, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, why := IntervalSane(c.interval, c.squelch)
			if ok != c.ok {
				t.Fatalf("got %v (%s) want %v", ok, why, c.ok)
			}
			if !ok && why == "" {
				t.Fatal("rejection must explain itself")
			}
		})
	}
	// 默认配置本身必须是安全的，否则上线即误报
	if ok, why := IntervalSane(DefaultOptions().Interval, SquelchWindow); !ok {
		t.Fatalf("default interval must be safe: %s", why)
	}
}

func TestOptionsNormalise(t *testing.T) {
	var o Options
	o.normalise()
	d := DefaultOptions()
	if o.SN != d.SN || o.PK != d.PK || o.Code != d.Code || o.SLO != d.SLO || o.Interval != d.Interval {
		t.Fatalf("zero options must fall back to defaults: %+v", o)
	}
	if o.Timeout < 10*time.Second {
		t.Fatalf("timeout must leave room for a slow chain: %s", o.Timeout)
	}
	// 大 SLO 时超时要跟着放大，否则永远判 missing
	big := Options{SLO: 30 * time.Second}
	big.normalise()
	if big.Timeout != 60*time.Second {
		t.Fatalf("timeout should be 2x SLO, got %s", big.Timeout)
	}
}

func TestQuantile(t *testing.T) {
	m := NewMetrics()
	if m.Quantile(0.5) != 0 {
		t.Fatal("no samples yet")
	}
	for _, d := range []time.Duration{500, 100, 300, 200, 400} {
		m.Observe(d * time.Millisecond)
	}
	if got := m.Quantile(0.5); got != 300*time.Millisecond {
		t.Fatalf("p50=%s want 300ms", got)
	}
	if got := m.Quantile(1); got != 500*time.Millisecond {
		t.Fatalf("p100=%s want 500ms", got)
	}
	m.Observe(-time.Second) // 负值按 0 记，不污染分位
	if got := m.Quantile(0); got != 0 {
		t.Fatalf("p0=%s want 0", got)
	}
	// 样本上限：不能无限增长
	for i := 0; i < maxSamples*2; i++ {
		m.Observe(time.Millisecond)
	}
	if n := m.Get(MRuns); n != 0 {
		t.Fatal("Observe must not touch counters")
	}
	if len(m.lat) > maxSamples {
		t.Fatalf("samples grew to %d, cap is %d", len(m.lat), maxSamples)
	}
}

func TestEnvelopeAndPayloadShape(t *testing.T) {
	ts := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	p, err := eventPayload("FLAME_DETECTED", 7, ts)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"code":"FLAME_DETECTED"`, `"seq":7`, `"ts":1789812000000`} {
		if !contains(string(p), want) {
			t.Fatalf("payload %s missing %s", p, want)
		}
	}
	e, err := envelopeFor("LM_S1", "PROBE00001", "FLAME_DETECTED", 7, ts)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"sn":"PROBE00001"`, `"kind":"event"`, `"pk":"LM_S1"`} {
		if !contains(string(e), want) {
			t.Fatalf("envelope %s missing %s", e, want)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
