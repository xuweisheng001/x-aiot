package bootstrap

import "testing"

func TestDecide(t *testing.T) {
	cells := map[int]Cell{
		1: {ID: 1, MQTTHost: "h1", MQTTPort: 1884, Status: StatusActive},
		2: {ID: 2, MQTTHost: "h2", MQTTPort: 1884, Status: StatusOverloaded},
		3: {ID: 3, MQTTHost: "h3", MQTTPort: 1884, Status: StatusDraining},
		5: {ID: 5, MQTTHost: "h5", MQTTPort: 1884, Status: StatusStandby},
		4: {ID: 4, MQTTHost: "h4", MQTTPort: 1884, Status: StatusStandby},
	}
	noStandby := map[int]Cell{3: cells[3]}
	tests := []struct {
		name  string
		cells map[int]Cell
		home  int
		ok    bool
		want  Response
	}{
		{"active", cells, 1, true, Response{CellID: 1, MQTTHost: "h1", MQTTPort: 1884, CellMapVer: 7}},
		{"overloaded keeps cell with retry", cells, 2, true, Response{CellID: 2, MQTTHost: "h2", MQTTPort: 1884, RetryAfter: 30, CellMapVer: 7}},
		{"draining redirects to lowest standby", cells, 3, true, Response{CellID: 4, MQTTHost: "h4", MQTTPort: 1884, RetryAfter: 30, CellMapVer: 7}},
		{"draining without standby stays", noStandby, 3, true, Response{CellID: 3, MQTTHost: "h3", MQTTPort: 1884, RetryAfter: 30, CellMapVer: 7}},
		{"standby home serves directly", cells, 5, true, Response{CellID: 5, MQTTHost: "h5", MQTTPort: 1884, CellMapVer: 7}},
		{"missing cell", cells, 9, false, Response{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Decide(tc.cells, tc.home, 7)
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

func TestValidSN(t *testing.T) {
	tests := map[string]bool{"SIM00001": true, "XT-1_A": true, "abc": false, "ab": false, "A B C D": false, "": false,
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456": false, "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345": true}
	for sn, want := range tests {
		if ValidSN(sn) != want {
			t.Errorf("ValidSN(%q)=%v want %v", sn, !want, want)
		}
	}
}
