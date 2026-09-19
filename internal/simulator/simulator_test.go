package simulator

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/backoff"
)

func TestParseEvent(t *testing.T) {
	tests := []struct {
		in   string
		want EventSpec
		err  bool
	}{
		{"", EventSpec{}, false},
		{"FLAME_DETECTED@30s", EventSpec{"FLAME_DETECTED", 30 * time.Second}, false},
		{"OVER_TEMP@1m30s", EventSpec{"OVER_TEMP", 90 * time.Second}, false},
		{"TILT@0s", EventSpec{"TILT", 0}, false},
		{"FLAME_DETECTED", EventSpec{}, true},
		{"@30s", EventSpec{}, true},
		{"X@abc", EventSpec{}, true},
		{"X@-1s", EventSpec{}, true},
	}
	for _, tc := range tests {
		got, err := ParseEvent(tc.in)
		if (err != nil) != tc.err || got != tc.want {
			t.Errorf("ParseEvent(%q) = %+v,%v want %+v err=%v", tc.in, got, err, tc.want, tc.err)
		}
	}
}

func TestSNAndBadFirmware(t *testing.T) {
	if SNFor("SIM", 1) != "SIM00001" || SNFor("FLOOD", 123) != "FLOOD00123" {
		t.Fatal("SNFor")
	}
	tests := []struct {
		n    int
		frac float64
		bad  int
	}{{50, 0.05, 3}, {50, 0, 0}, {100, 0.05, 5}, {10, 1, 10}, {1, 0.05, 0}}
	for _, tc := range tests {
		cnt := 0
		for i := 0; i < tc.n; i++ {
			if IsBadFirmware(i, tc.n, tc.frac) {
				cnt++
			}
		}
		if cnt != tc.bad {
			t.Errorf("n=%d frac=%v bad=%d want %d", tc.n, tc.frac, cnt, tc.bad)
		}
	}
}

func TestReconnectDelay(t *testing.T) {
	for n := 0; n < 30; n++ {
		if ReconnectDelay(true, n) != time.Second {
			t.Fatal("bad firmware must be fixed 1s")
		}
		d := ReconnectDelay(false, n)
		lo := time.Second << uint(min(n, 20))
		if lo > backoff.MaxInterval {
			lo = backoff.MaxInterval
		}
		if d < lo || d > lo+backoff.MaxJitter {
			t.Fatalf("n=%d delay %v out of [%v,%v]", n, d, lo, lo+backoff.MaxJitter)
		}
	}
}

func TestHostPort(t *testing.T) {
	tests := map[string]string{"tcp://127.0.0.1:1884": "127.0.0.1:1884", "127.0.0.1:1884": "127.0.0.1:1884",
		"tcp://broker": "broker:1883", "mqtt://h:1": "h:1"}
	for in, want := range tests {
		got, err := HostPort(in)
		if err != nil || got != want {
			t.Errorf("HostPort(%q)=%q,%v want %q", in, got, err, want)
		}
	}
	if _, err := HostPort(""); err == nil {
		t.Fatal("empty must error")
	}
}

func TestTelemetryCycleAndSeq(t *testing.T) {
	s, p := WorkCycle(0)
	if s != 0 || p != 0 {
		t.Fatal("tick0 idle")
	}
	s, p = WorkCycle(1)
	if s != 1 || p != 0 {
		t.Fatal("tick1 ready")
	}
	s, p = WorkCycle(2)
	if s != 2 || p != 5 {
		t.Fatal("tick2 working 5")
	}
	s, p = WorkCycle(21)
	if s != 2 || p != 100 {
		t.Fatalf("tick21 working 100: %d %d", s, p)
	}
	s, _ = WorkCycle(22)
	if s != 0 {
		t.Fatal("wraps")
	}
	d := NewDevice(&Config{N: 1, SNPrefix: "SIM", SeqBase: -1}, 1, &Stats{}) // SeqBase<0：从 0 起，便于断言步进
	prev := int64(0)
	for i := 0; i < 100; i++ {
		seq := d.NextSeq()
		if seq != prev+1 {
			t.Fatalf("seq not monotonic: %d after %d", seq, prev)
		}
		prev = seq
	}
	tel := BuildTelemetry(7, 1000, 5, 0.5)
	b, _ := json.Marshal(tel)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"seq", "ts", "work_state", "power_level", "temp_cavity", "temp_water", "fan_rpm", "laser_hours", "progress"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("telemetry missing %s", k)
		}
	}
	if m["seq"].(float64) != 7 || m["work_state"].(float64) != 2 {
		t.Fatalf("telemetry %v", m)
	}
}

func TestCmdHelpers(t *testing.T) {
	if r, _ := AckFor("pause"); r != "ok" {
		t.Fatal("pause ok")
	}
	if r, d := AckFor("remote_restart"); r != "fail" || d == "" {
		t.Fatal("remote_restart fail with detail")
	}
	ok := OTAPhases(false)
	if len(ok) != 4 || ok[0].Phase != "notified" || ok[1].Phase != "downloading" || ok[2].Phase != "verifying" || ok[3].Phase != "success" || ok[3].Pct != 100 {
		t.Fatalf("ok phases %+v", ok)
	}
	bad := OTAPhases(true)
	if len(bad) != 4 || bad[3].Phase != "failed" || bad[3].ErrorCode != "E_VERIFY" {
		t.Fatalf("fail phases %+v", bad)
	}
	tests := map[string]int64{`{"batch_id":12}`: 12, `{"batch_id":"34"}`: 34, `{"batch_id":5.0}`: 5, `{}`: 0, ``: 0, `notjson`: 0, `{"x":1}`: 0}
	for in, want := range tests {
		if got := BatchIDFrom(json.RawMessage(in)); got != want {
			t.Errorf("BatchIDFrom(%s)=%d want %d", in, got, want)
		}
	}
	if DownKind("down/SIM00001/cmd", "SIM00001") != "cmd" || DownKind("down/SIM00002/cmd", "SIM00001") != "" || DownKind("down/SIM00001/desired", "SIM00001") != "desired" {
		t.Fatal("DownKind")
	}
	if TopicUp("LM_S1", "SIM00001", "telemetry") != "up/LM_S1/SIM00001/telemetry" || TopicDown("SIM00001") != "down/SIM00001/#" {
		t.Fatal("topics")
	}
}

func TestParseBootstrap(t *testing.T) {
	r, err := ParseBootstrap([]byte(`{"code":0,"data":{"cell_id":2,"mqtt_host":"h","mqtt_port":1884,"retry_after":30,"cell_map_ver":1}}`))
	if err != nil || r.CellID != 2 || r.MQTTHost != "h" || r.MQTTPort != 1884 || r.RetryAfter != 30 {
		t.Fatalf("wrapped: %+v %v", r, err)
	}
	r, err = ParseBootstrap([]byte(`{"cell_id":1,"mqtt_host":"h","mqtt_port":1883}`))
	if err != nil || r.MQTTPort != 1883 {
		t.Fatalf("flat: %+v %v", r, err)
	}
	if _, err = ParseBootstrap([]byte(`{"code":10001,"msg":"invalid sn"}`)); err == nil {
		t.Fatal("error code must fail")
	}
	if _, err = ParseBootstrap([]byte(`{"code":0,"data":{"cell_id":1}}`)); err == nil {
		t.Fatal("missing host must fail")
	}
}

func TestSeqBase(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		base int64
		want int64
	}{{100, 100}, {0, now.UnixMilli() * 1000}, {-1, 0}}
	for _, c := range cases {
		if got := SeqBase(c.base, now); got != c.want {
			t.Errorf("SeqBase(%d)=%d want %d", c.base, got, c.want)
		}
	}
	d := NewDevice(&Config{N: 1, SNPrefix: "SIM"}, 1, &Stats{})
	if d.NextSeq() <= now.UnixMilli()*1000 {
		t.Errorf("default seq should be time-derived, got %d", d.NextSeq())
	}
}
