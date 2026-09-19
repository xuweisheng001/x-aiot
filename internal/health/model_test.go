package health

import (
	"math"
	"testing"
	"time"
)

var now = time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

// 输入缺失不得产出 0 分：这是 INC-3-02 的第一层根治，单测锁死。
func TestComputeHealthMissingInputsNeverZero(t *testing.T) {
	cfg := DefaultCfg()
	cases := []struct {
		name string
		in   Inputs
	}{
		{"no used hours", Inputs{HasUsed: false, Cfg: cfg, Now: now}},
		{"rated 0", Inputs{UsedWeighted: 100, HasUsed: true, Cfg: Cfg{Rated: 0}, Now: now}},
		{"rated negative", Inputs{UsedWeighted: 100, HasUsed: true, Cfg: Cfg{Rated: -1}, Now: now}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			score, eol, ok := ComputeHealth(c.in)
			if ok {
				t.Fatalf("missing input must not be ok, got score=%v", score)
			}
			if eol != nil {
				t.Fatal("no EOL prediction without inputs")
			}
		})
	}
	// 有输入时才给分
	score, _, ok := ComputeHealth(Inputs{UsedWeighted: 2000, HasUsed: true, Cfg: cfg, Now: now})
	if !ok || math.Abs(score-80) > 1e-9 {
		t.Fatalf("score=%v ok=%v want 80 true", score, ok)
	}
}

func TestWeightedHours(t *testing.T) {
	cfg := DefaultCfg() // wLow 1.0 wMid 1.3 wHigh 1.8
	cases := []struct {
		name               string
		delta, lo, mid, hi float64
		want               float64
		flag               HoursFlag
	}{
		{"all low", 1, 1, 0, 0, 1.0, FlagOK},
		{"all high", 0.5, 0, 0, 1, 0.9, FlagOK},
		{"mixed normalised", 1, 1, 1, 0, 1.15, FlagOK},
		{"shares unnormalised", 1, 2, 2, 0, 1.15, FlagOK},
		{"unknown shares fall back to 1.0", 0.4, 0, 0, 0, 0.4, FlagOK},
		{"negative delta is regress", -0.2, 1, 0, 0, 0, FlagRegress},
		{"delta over one hour clipped", 3, 1, 0, 0, 1.0, FlagClipped},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, flag := WeightedHours(c.delta, c.lo, c.mid, c.hi, cfg)
			if math.Abs(got-c.want) > 1e-9 || flag != c.flag {
				t.Fatalf("got %v/%q want %v/%q", got, flag, c.want, c.flag)
			}
		})
	}
	if _, f := WeightedHours(math.NaN(), 1, 0, 0, cfg); f != FlagRegress {
		t.Fatal("NaN delta must be treated as regress, never propagated into the score")
	}
}

func TestHealthPenaltyCap(t *testing.T) {
	cfg := DefaultCfg() // penalty 0.5/次，上限 10
	h, p := Health(0, 10000, 100, cfg)
	if p != MaxPenalty || math.Abs(h-90) > 1e-9 {
		t.Fatalf("penalty must cap at %v: h=%v p=%v", MaxPenalty, h, p)
	}
	if h, _ := Health(20000, 10000, 0, cfg); h != 0 {
		t.Fatalf("over-rated usage clamps to 0, got %v", h)
	}
	if h, _ := Health(0, 10000, -5, cfg); h != 100 {
		t.Fatalf("negative overtemp count must not raise health: %v", h)
	}
}

func TestMonotonic(t *testing.T) {
	cases := []struct {
		name       string
		prev, next float64
		swapped    bool
		want       float64
		kept       bool
	}{
		{"first ever", -1, 73, false, 73, false},
		{"decreasing accepted", 80, 70, false, 70, false},
		{"increase kept at prev", 70, 80, false, 70, true},
		{"increase allowed after swap", 70, 100, true, 100, false},
		{"equal", 70, 70, false, 70, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v, kept := Monotonic(c.prev, c.next, c.swapped)
			if v != c.want || kept != c.kept {
				t.Fatalf("got %v/%v want %v/%v", v, kept, c.want, c.kept)
			}
		})
	}
}

func TestDetectSwap(t *testing.T) {
	recent := now.Add(-time.Hour)
	old := now.Add(-48 * time.Hour)
	cases := []struct {
		name        string
		prev, cur   string
		hours       []float64
		marked      *time.Time
		moduleHours *float64
		wantSwap    bool
		wantReason  string
	}{
		{"model changed", "LM40", "LM20", []float64{500, 501}, nil, nil, true, SwapModelChange},
		{"same model no signal", "LM40", "LM40", []float64{500, 501, 502}, nil, nil, false, ""},
		{"hours reset two buckets", "", "", []float64{500, 1, 2}, nil, nil, true, SwapHoursReset},
		{"single dip is not enough", "", "", []float64{500, 1, 490}, nil, nil, false, ""},
		{"module hours halved", "", "", []float64{500}, nil, ptr(100.0), true, SwapModuleHours},
		{"module hours normal", "", "", []float64{500}, nil, ptr(480.0), false, ""},
		{"user marked with dip", "", "", []float64{500, 1}, &recent, nil, true, SwapUserMarked},
		{"user marked without dip", "", "", []float64{500, 501}, &recent, nil, false, ""},
		{"stale user mark ignored", "", "", []float64{500, 1}, &old, nil, false, ""},
		{"unknown model on one side", "", "LM40", []float64{500, 501}, nil, nil, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			swap, reason := DetectSwap(c.prev, c.cur, c.hours, c.marked, c.moduleHours, now)
			if swap != c.wantSwap || reason != c.wantReason {
				t.Fatalf("got %v/%q want %v/%q", swap, reason, c.wantSwap, c.wantReason)
			}
		})
	}
}

func TestPredictEOLAndRate(t *testing.T) {
	if eol := PredictEOL(20, 10000, 5, now); eol != nil {
		t.Fatal("health already at EOL threshold must not predict")
	}
	if eol := PredictEOL(70, 10000, 0, now); eol != nil {
		t.Fatal("no rate → no prediction")
	}
	// (70-20)/100 × 10000 = 5000 加权小时；每天 5 → 1000 天
	eol := PredictEOL(70, 10000, 5, now)
	if eol == nil {
		t.Fatal("expected prediction")
	}
	if days := eol.Sub(now).Hours() / 24; math.Abs(days-1000) > 0.5 {
		t.Fatalf("days=%v want 1000", days)
	}
	if eol := PredictEOL(70, 10000, 1e-9, now); eol != nil {
		t.Fatal("absurdly far prediction must be dropped, not shown to users")
	}
	if r := Rate30d(60, 3); r != 0 {
		t.Fatalf("under %v days must not predict: %v", MinPredictDays, r)
	}
	if r := Rate30d(60, 30); math.Abs(r-2) > 1e-9 {
		t.Fatalf("rate=%v want 2", r)
	}
}

func TestDecideReminder(t *testing.T) {
	past := now.Add(-8 * 24 * time.Hour)
	fresh := now.Add(-2 * time.Hour)
	cases := []struct {
		name   string
		in     ReminderInput
		action ReminderAction
		level  int
		reason string
	}{
		{"cross 80", ReminderInput{HealthPrev: 85, HealthNow: 79, Now: now}, ActionSend, 80, ""},
		{"no crossing", ReminderInput{HealthPrev: 85, HealthNow: 82, Now: now}, ActionNone, 0, ""},
		{"multi level takes lowest", ReminderInput{HealthPrev: 85, HealthNow: 15, Now: now}, ActionSend, 20, ""},
		{"already sent is skipped", ReminderInput{HealthPrev: 85, HealthNow: 79, LevelsSent: []int{80}, Now: now}, ActionNone, 0, ""},
		{"optout suppresses", ReminderInput{HealthPrev: 85, HealthNow: 79, Optout: true, Now: now}, ActionSuppressed, 80, ReasonOptout},
		{"cooldown suppresses", ReminderInput{HealthPrev: 85, HealthNow: 79, LastSentAt: &fresh, Now: now}, ActionSuppressed, 80, ReasonCooldown},
		{"cooldown elapsed sends", ReminderInput{HealthPrev: 85, HealthNow: 79, LastSentAt: &past, Now: now}, ActionSend, 80, ""},
		{"working defers", ReminderInput{HealthPrev: 85, HealthNow: 79, WorkState: 2, Now: now}, ActionDeferred, 80, ""},
		{"preheating defers", ReminderInput{HealthPrev: 85, HealthNow: 79, WorkState: 1, Now: now}, ActionDeferred, 80, ""},
		{"first run uses 101 sentinel", ReminderInput{HealthPrev: 101, HealthNow: 45, Now: now}, ActionSend, 50, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := DecideReminder(c.in)
			if d.Action != c.action || d.Level != c.level || d.Reason != c.reason {
				t.Fatalf("got %+v want %s/%d/%s", d, c.action, c.level, c.reason)
			}
		})
	}
}

func TestShouldFuseBatch(t *testing.T) {
	cases := []struct {
		skipped, total int
		ratio          float64
		want           bool
	}{
		{0, 100, 0.3, false},
		{30, 100, 0.3, false}, // 等于阈值不熔断
		{31, 100, 0.3, true},
		{1, 1, 0.3, true},
		{5, 0, 0.3, false}, // 没样本不熔断
	}
	for _, c := range cases {
		if got := ShouldFuseBatch(c.skipped, c.total, c.ratio); got != c.want {
			t.Errorf("ShouldFuseBatch(%d,%d,%v)=%v want %v", c.skipped, c.total, c.ratio, got, c.want)
		}
	}
}

func TestSuspiciousAttributionStale(t *testing.T) {
	if !SuspiciousByScans(6, 5) || SuspiciousByScans(5, 5) {
		t.Fatal("suspicious is strictly greater than threshold")
	}
	if !SuspiciousByScans(6, 0) {
		t.Fatal("threshold 0 falls back to the default")
	}
	sent := now.Add(-10 * 24 * time.Hour)
	if !AttributionWindowOK(sent, now, 0) {
		t.Fatal("10 days is inside the default 30 day window")
	}
	if AttributionWindowOK(now, now.Add(-time.Hour), 0) {
		t.Fatal("payment before the reminder must not be attributed")
	}
	if AttributionWindowOK(now.Add(-40*24*time.Hour), now, 0) {
		t.Fatal("outside the window must not be attributed")
	}
	if !IsStale(now.Add(-3*time.Hour), now, 2*time.Hour) || IsStale(now.Add(-time.Hour), now, 2*time.Hour) {
		t.Fatal("stale boundary")
	}
}

func TestShareFromAvgPower(t *testing.T) {
	cases := []struct {
		p           float64
		lo, mid, hi float64
	}{
		{10, 1, 0, 0}, {50, 1, 0, 0}, {51, 0, 1, 0}, {80, 0, 1, 0}, {81, 0, 0, 1}, {100, 0, 0, 1},
	}
	for _, c := range cases {
		lo, mid, hi := ShareFromAvgPower(c.p)
		if lo != c.lo || mid != c.mid || hi != c.hi {
			t.Errorf("ShareFromAvgPower(%v)=%v,%v,%v want %v,%v,%v", c.p, lo, mid, hi, c.lo, c.mid, c.hi)
		}
	}
}
