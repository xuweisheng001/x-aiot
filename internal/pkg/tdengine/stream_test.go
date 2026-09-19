package tdengine

import "testing"

func TestStreamNeedsRebuild(t *testing.T) {
	req := Telemetry1hRequiredCols
	cases := []struct {
		name     string
		existing []string
		want     bool
	}{
		{"table missing → create path, no rebuild", nil, false},
		{"old stream lacks share cols", []string{"ts", "avg_temp", "max_temp", "laser_hours", "group_id"}, true},
		{"new stream complete", []string{"ts", "avg_temp", "max_temp", "laser_hours", "avg_power", "share_low", "share_mid", "share_high", "group_id"}, false},
		{"case-insensitive", []string{"TS", "AVG_POWER", "SHARE_LOW", "SHARE_MID", "SHARE_HIGH"}, false},
		{"partial", []string{"ts", "avg_power", "share_low"}, true},
	}
	for _, c := range cases {
		if got := StreamNeedsRebuild(c.existing, req); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
