package simulator

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestApplyPurifierDesired(t *testing.T) {
	base := PurifierState{PowerOn: false, FanLevel: 2}
	cases := []struct {
		name    string
		desired map[string]any
		want    PurifierState
		changed bool
	}{
		{"power on bool", map[string]any{"power_on": true}, PurifierState{PowerOn: true, FanLevel: 2, TriggerSource: "cloud"}, true},
		{"power on string", map[string]any{"power_on": "1"}, PurifierState{PowerOn: true, FanLevel: 2, TriggerSource: "cloud"}, true},
		{"power on number + level float", map[string]any{"power_on": float64(1), "fan_level": float64(4)}, PurifierState{PowerOn: true, FanLevel: 4, TriggerSource: "cloud"}, true},
		{"level string", map[string]any{"fan_level": "3"}, PurifierState{PowerOn: false, FanLevel: 3, TriggerSource: "cloud"}, true},
		{"level out of range ignored", map[string]any{"fan_level": float64(9)}, base, false},
		{"same values no change", map[string]any{"power_on": false, "fan_level": float64(2)}, base, false},
		{"unrelated keys", map[string]any{"paired_sn": "HOST0001"}, base, false},
		{"garbage ignored", map[string]any{"power_on": "maybe", "fan_level": "x"}, base, false},
	}
	for _, c := range cases {
		got, changed := ApplyPurifierDesired(base, c.desired)
		if got != c.want || changed != c.changed {
			t.Errorf("%s: got %+v changed=%v want %+v changed=%v", c.name, got, changed, c.want, c.changed)
		}
	}
}

func TestPurifierTelemetry(t *testing.T) {
	if PurifierRPM(PurifierState{PowerOn: false, FanLevel: 3}) != 0 || PurifierRPM(PurifierState{PowerOn: true, FanLevel: 3}) != 2400 ||
		PurifierRPM(PurifierState{PowerOn: true, FanLevel: 9}) != 3200 || PurifierRPM(PurifierState{PowerOn: true, FanLevel: 0}) != 800 {
		t.Fatal("PurifierRPM")
	}
	st := PurifierState{PowerOn: true, FanLevel: 2, RuntimeS: 7200, TriggerSource: "cloud"}
	tel := BuildPurifierTelemetry(11, 1000, st, 0)
	if tel.WorkState != 2 || tel.FanRPM != 1600 || tel.PowerOn == nil || !*tel.PowerOn || *tel.FanLevel != 2 || *tel.RuntimeH != 2 || tel.TriggerSource != "cloud" {
		t.Fatalf("telemetry %+v", tel)
	}
	b, _ := json.Marshal(tel)
	for _, k := range []string{`"power_on":true`, `"fan_level":2`, `"pressure_diff":`, `"runtime_h":2`, `"trigger_source":"cloud"`, `"fan_rpm":1600`} {
		if !strings.Contains(string(b), k) {
			t.Fatalf("json missing %s: %s", k, b)
		}
	}
	off := BuildPurifierTelemetry(12, 1000, PurifierState{PowerOn: false, FanLevel: 2}, 0)
	if off.WorkState != 0 || off.FanRPM != 0 || *off.PowerOn {
		t.Fatalf("off telemetry %+v", off)
	}
	// 普通主机遥测不带净化器字段
	hostB, _ := json.Marshal(BuildTelemetry(1, 1, 0, 0))
	if strings.Contains(string(hostB), "power_on") || strings.Contains(string(hostB), "trigger_source") {
		t.Fatalf("host telemetry must not carry purifier fields: %s", hostB)
	}
	adv := PurifierState{PowerOn: true}.Advance(5 * time.Second)
	if adv.RuntimeS != 5 || (PurifierState{}).Advance(5*time.Second).RuntimeS != 0 {
		t.Fatal("Advance")
	}
}
