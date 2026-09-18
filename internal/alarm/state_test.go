package alarm

import (
	"reflect"
	"testing"
)

func TestAllowed(t *testing.T) {
	cases := []struct {
		from, to string
		want     bool
	}{
		{"open", "notified", true},
		{"open", "acked", true},
		{"notified", "acked", true},
		{"acked", "closed", true},
		{"open", "closed", false},     // 不能跳过 ack
		{"notified", "closed", false}, // 同上
		{"notified", "open", false},   // 不可回退
		{"acked", "notified", false},
		{"acked", "acked", false}, // 重复 ack 非法（affected=0 → 409）
		{"closed", "acked", false},
		{"closed", "closed", false},
		{"bogus", "acked", false},
		{"open", "bogus", false},
		{"", "", false},
	}
	for _, c := range cases {
		if got := Allowed(c.from, c.to); got != c.want {
			t.Errorf("Allowed(%q,%q)=%v want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestSources(t *testing.T) {
	cases := map[string][]string{
		"acked":    {"notified", "open"},
		"closed":   {"acked"},
		"notified": {"open"},
		"open":     nil,
	}
	for to, want := range cases {
		if got := Sources(to); !reflect.DeepEqual(got, want) {
			t.Errorf("Sources(%q)=%v want %v", to, got, want)
		}
	}
	// 表与 Allowed 一致
	for _, to := range []string{"notified", "acked", "closed"} {
		for _, from := range Sources(to) {
			if !Allowed(from, to) {
				t.Errorf("Sources(%q) contains %q but Allowed is false", to, from)
			}
		}
	}
}

func TestReservoirQuantile(t *testing.T) {
	r := NewReservoir(1000)
	for i := 1; i <= 100; i++ {
		r.Add(float64(i))
	}
	if r.Count() != 100 {
		t.Fatalf("count=%d", r.Count())
	}
	if p50 := r.Quantile(0.5); p50 < 49 || p50 > 51 {
		t.Errorf("p50=%v", p50)
	}
	if p99 := r.Quantile(0.99); p99 < 98 || p99 > 100 {
		t.Errorf("p99=%v", p99)
	}
	if NewReservoir(8).Quantile(0.5) != 0 {
		t.Error("empty reservoir should return 0")
	}
	// 有界：超过容量后 items 不增长
	small := NewReservoir(16)
	for i := 0; i < 10000; i++ {
		small.Add(float64(i))
	}
	if len(small.items) != 16 || small.Count() != 10000 {
		t.Errorf("reservoir not bounded: len=%d n=%d", len(small.items), small.Count())
	}
}

func TestSquelchKey(t *testing.T) {
	if got := SquelchKey("SN1", "FLAME_DETECTED"); got != "alarm:squelch:SN1:FLAME_DETECTED" {
		t.Errorf("got %q", got)
	}
}
