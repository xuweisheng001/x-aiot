package auth

import "testing"

func TestAllowTopic(t *testing.T) {
	tests := []struct {
		name   string
		client string
		topic  string
		action string
		want   bool
	}{
		{"pub own telemetry", "SIM00001", "up/LM_S1/SIM00001/telemetry", "publish", true},
		{"pub own nested", "SIM00001", "up/LM_S1/SIM00001/ota_progress/1", "publish", true},
		{"pub short action alias", "SIM00001", "up/LM_S1/SIM00001/event", "PUB", true},
		{"pub other sn", "SIM00001", "up/LM_S1/SIM00002/telemetry", "publish", false},
		{"pub missing kind", "SIM00001", "up/LM_S1/SIM00001", "publish", false},
		{"pub empty pk", "SIM00001", "up//SIM00001/telemetry", "publish", false},
		{"pub wildcard", "SIM00001", "up/LM_S1/SIM00001/#", "publish", false},
		{"pub plus", "SIM00001", "up/+/SIM00001/telemetry", "publish", false},
		{"pub trailing empty", "SIM00001", "up/LM_S1/SIM00001/telemetry/", "publish", false},
		{"pub down topic", "SIM00001", "down/SIM00001/cmd", "publish", false},
		{"pub sys", "SIM00001", "$SYS/brokers", "publish", false},
		{"sub own wildcard", "SIM00001", "down/SIM00001/#", "subscribe", true},
		{"sub alias", "SIM00001", "down/SIM00001/#", "sub", true},
		{"sub other sn", "SIM00001", "down/SIM00002/#", "subscribe", false},
		{"sub exact cmd not allowed", "SIM00001", "down/SIM00001/cmd", "subscribe", false},
		{"sub plus", "SIM00001", "down/+/#", "subscribe", false},
		{"sub all", "SIM00001", "#", "subscribe", false},
		{"sub up", "SIM00001", "up/LM_S1/SIM00001/telemetry", "subscribe", false},
		{"sub share group", "SIM00001", "$share/bridge/up/#", "subscribe", false},
		{"unknown action", "SIM00001", "down/SIM00001/#", "read", false},
		{"invalid clientid", "bad", "up/LM_S1/bad/telemetry", "publish", false},
		{"empty topic", "SIM00001", "", "publish", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := AllowTopic(tc.client, tc.topic, ParseAction(tc.action)); got != tc.want {
				t.Fatalf("AllowTopic(%q,%q,%q)=%v want %v", tc.client, tc.topic, tc.action, got, tc.want)
			}
		})
	}
}

func TestValidSN(t *testing.T) {
	tests := map[string]bool{
		"SIM00001": true, "XT-1_A": true, "ABCD": true, "ABC": false, "abcd": false, "": false, "A B C D": false,
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ012345": true, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456": false, "SIM/1": false,
	}
	for sn, want := range tests {
		if ValidSN(sn) != want {
			t.Errorf("ValidSN(%q) want %v", sn, want)
		}
	}
}
