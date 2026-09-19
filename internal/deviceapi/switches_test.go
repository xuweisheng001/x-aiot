package deviceapi

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSwitchState(t *testing.T) {
	cases := []struct {
		name      string
		reported  any
		desired   any
		online    bool
		wantV     bool
		wantState string
	}{
		{"一致 true 在线", "true", true, true, true, SwitchApplied},
		{"一致 true 离线", "true", true, false, true, SwitchApplied},
		{"一致 false 在线", "false", false, true, false, SwitchApplied},
		{"reported 缺失视为 false，desired false → applied", nil, false, false, false, SwitchApplied},
		{"desired true reported false 在线 → pending 显示 false", "false", true, true, false, SwitchPending},
		{"desired true reported 缺失 离线 → offline 显示 false", nil, true, false, false, SwitchOffline},
		{"desired false reported true 在线 → pending 显示 true", "true", false, true, true, SwitchPending},
		{"desired false reported true 离线 → offline 显示 true", "1", false, false, true, SwitchOffline},
	}
	for _, c := range cases {
		v, st := SwitchState(c.reported, c.desired, c.online)
		if v != c.wantV || st != c.wantState {
			t.Errorf("%s: got (%v,%s) want (%v,%s)", c.name, v, st, c.wantV, c.wantState)
		}
	}
}

func TestBuildSwitchesAndOnline(t *testing.T) {
	now := time.UnixMilli(1_726_300_000_000)
	fresh := now.Add(-30 * time.Second).UnixMilli()
	stale := now.Add(-5 * time.Minute).UnixMilli()
	desired := json.RawMessage(`{"camera_cloud_optin":false,"job_feedback_optin":true,"power_limit":80,"name":"x"}`)

	// 在线：job 不一致 → pending；camera 一致 → applied；非布尔字段不出现
	sw := BuildSwitches(map[string]string{"updated_at": itoa(fresh), "job_feedback_optin": "false"}, desired, now)
	if len(sw) != 2 || sw["camera_cloud_optin"] != (SwitchInfo{false, SwitchApplied}) || sw["job_feedback_optin"] != (SwitchInfo{false, SwitchPending}) {
		t.Fatalf("online switches %+v", sw)
	}
	// 离线：不一致 → offline
	sw = BuildSwitches(map[string]string{"updated_at": itoa(stale)}, desired, now)
	if sw["job_feedback_optin"] != (SwitchInfo{false, SwitchOffline}) {
		t.Fatalf("offline switches %+v", sw)
	}
	// 无时间信息：按在线处理 → pending
	sw = BuildSwitches(map[string]string{}, desired, now)
	if sw["job_feedback_optin"] != (SwitchInfo{false, SwitchPending}) {
		t.Fatalf("unknown-online switches %+v", sw)
	}
	// 收敛后 applied
	sw = BuildSwitches(map[string]string{"updated_at": itoa(fresh), "job_feedback_optin": "true"}, desired, now)
	if sw["job_feedback_optin"] != (SwitchInfo{true, SwitchApplied}) {
		t.Fatalf("applied switches %+v", sw)
	}
	if len(BuildSwitches(nil, nil, now)) != 0 {
		t.Fatal("no desired → no switches")
	}
	if _, known := OnlineFromReported(map[string]string{"updated_at": "abc"}, now); known {
		t.Fatal("bad updated_at must be unknown")
	}
}

func TestParseBoolLoose(t *testing.T) {
	for in, want := range map[string]bool{"true": true, "1": true, "TRUE": true, "false": false, "0": false, "": false} {
		if v, ok := ParseBoolLoose(in); !ok || v != want {
			t.Errorf("ParseBoolLoose(%q)=%v,%v", in, v, ok)
		}
	}
	if _, ok := ParseBoolLoose("maybe"); ok {
		t.Fatal("non-bool must be ok=false")
	}
}

func itoa(v int64) string { return json.Number(fmtInt(v)).String() }

func fmtInt(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
