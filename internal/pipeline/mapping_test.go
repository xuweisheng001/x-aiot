package pipeline

import (
	"errors"
	"testing"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

func env(kind envelope.Kind, seq int64, payload string) []byte {
	e := envelope.Envelope{PK: "LM_S1", SN: "XT001", Kind: kind, Seq: seq, RecvTs: 1726300000999, Payload: []byte(payload)}
	return e.Marshal()
}

func TestParseAndValidate(t *testing.T) {
	cases := []struct {
		name   string
		data   []byte
		poison bool
		check  func(*Parsed) bool
	}{
		{"telemetry ok", env(envelope.KindTelemetry, 123, `{"seq":123,"ts":1726300000000,"work_state":2,"power_level":87,"temp_cavity":41.5,"temp_water":25.1,"fan_rpm":3000,"laser_hours":120.3,"progress":55}`), false,
			func(p *Parsed) bool {
				return p.Telemetry != nil && p.Telemetry.Seq == 123 && p.Telemetry.FanRPM == 3000 && p.Env.Seq == 123
			}},
		{"telemetry ts fallback to recv_ts", env(envelope.KindTelemetry, 5, `{"seq":5}`), false,
			func(p *Parsed) bool { return p.Telemetry.Ts == 1726300000999 }},
		{"telemetry seq from envelope", env(envelope.KindTelemetry, 9, `{"ts":1}`), false,
			func(p *Parsed) bool { return p.Telemetry.Seq == 9 && p.Env.Seq == 9 }},
		{"telemetry no seq anywhere", env(envelope.KindTelemetry, 0, `{"ts":1}`), true, nil},
		{"telemetry wrong type", env(envelope.KindTelemetry, 1, `{"seq":1,"ts":1,"fan_rpm":"fast"}`), true, nil},
		{"telemetry bad json", env(envelope.KindTelemetry, 1, `{seq`), true, nil},
		{"telemetry empty payload", env(envelope.KindTelemetry, 1, ``), true, nil},
		{"event ok", env(envelope.KindEvent, 124, `{"seq":124,"ts":1,"code":"FLAME_DETECTED","msg":"x"}`), false,
			func(p *Parsed) bool { return p.Event != nil && p.Event.Code == "FLAME_DETECTED" }},
		{"event no code", env(envelope.KindEvent, 124, `{"seq":124,"ts":1}`), true, nil},
		{"cmd_ack ok", env(envelope.KindCmdAck, 125, `{"seq":125,"ts":1,"cmd_id":"abc","result":"ok"}`), false,
			func(p *Parsed) bool { return p.CmdAck != nil && p.CmdAck.CmdID == "abc" }},
		{"cmd_ack no cmd_id", env(envelope.KindCmdAck, 125, `{"seq":125,"result":"ok"}`), true, nil},
		{"ota ok", env(envelope.KindOTAProgress, 126, `{"seq":126,"phase":"downloading","pct":40}`), false,
			func(p *Parsed) bool { return p.Telemetry == nil && p.Event == nil && p.CmdAck == nil }},
		{"ota bad json", env(envelope.KindOTAProgress, 126, `{`), true, nil},
		{"unknown kind", env(envelope.Kind("weird"), 1, `{}`), true, nil},
		{"not an envelope", []byte(`{"foo":1}`), true, nil},
		{"garbage", []byte(`garbage`), true, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := ParseAndValidate(c.data)
			if c.poison {
				if !errors.Is(err, ErrPoison) {
					t.Fatalf("want ErrPoison, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if c.check != nil && !c.check(p) {
				t.Fatalf("check failed: %+v", p)
			}
		})
	}
}

func TestMergeDevice(t *testing.T) {
	e := &envelope.Envelope{PK: "LM_S1", SN: "XT001"}
	cases := []struct {
		name   string
		fields map[string]string
		want   DeviceInfo
	}{
		{"nil fields → defaults", nil, DeviceInfo{"LM_S1", "unknown", "US", 2}},
		{"full", map[string]string{"pk": "P2", "fw": "1.2.3", "region": "EU", "cell": "1"}, DeviceInfo{"P2", "1.2.3", "EU", 1}},
		{"partial", map[string]string{"fw": "2.0"}, DeviceInfo{"LM_S1", "2.0", "US", 2}},
		{"bad cell ignored", map[string]string{"cell": "abc"}, DeviceInfo{"LM_S1", "unknown", "US", 2}},
		{"zero cell ignored", map[string]string{"cell": "0"}, DeviceInfo{"LM_S1", "unknown", "US", 2}},
	}
	for _, c := range cases {
		if got := MergeDevice(e, 2, c.fields); got != c.want {
			t.Errorf("%s: got %+v want %+v", c.name, got, c.want)
		}
	}
}

func TestCellFromSubject(t *testing.T) {
	cases := map[string]int{"iot.up.telemetry.3": 3, "iot.up.event.1": 1, "iot.up.event.x": 0, "nodots": 0, "iot.up.event.0": 0}
	for in, want := range cases {
		if got := CellFromSubject(in); got != want {
			t.Errorf("%s: %d want %d", in, got, want)
		}
	}
}

func TestToTelemetryRowAndShadowFields(t *testing.T) {
	e := &envelope.Envelope{PK: "LM_S1", SN: "XT001"}
	d := DeviceInfo{PK: "LM_S1", FW: "1.0", Region: "US", Cell: 1}
	p := &TelemetryPayload{Seq: 7, Ts: 1000, WorkState: 2, PowerLevel: 87.0, TempCavity: 41.5, TempWater: 25.1, FanRPM: 3000, LaserHours: 120.3, Progress: 55}
	r := ToTelemetryRow(e, d, p)
	want := tdengine.TelemetryRow{SN: "XT001", PK: "LM_S1", FW: "1.0", Region: "US", Cell: 1, Ts: 1000, Seq: 7,
		WorkState: 2, PowerLevel: 87, TempCavity: 41.5, TempWater: 25.1, FanRPM: 3000, LaserHours: 120.3, Progress: 55}
	if r != want {
		t.Fatalf("row=%+v want %+v", r, want)
	}
	f := ShadowFields(r)
	for _, k := range []string{"work_state", "power_level", "temp_cavity", "temp_water", "fan_rpm", "laser_hours", "progress", "seq", "ts"} {
		if _, ok := f[k]; !ok {
			t.Errorf("missing shadow field %s", k)
		}
	}
	if len(f) != 9 {
		t.Fatalf("unexpected extra fields: %v", f)
	}
}

func TestLatestPerSN(t *testing.T) {
	rows := []tdengine.TelemetryRow{
		{SN: "A", Ts: 10, Seq: 1}, {SN: "B", Ts: 5, Seq: 1}, {SN: "A", Ts: 30, Seq: 3},
		{SN: "A", Ts: 20, Seq: 2}, {SN: "B", Ts: 5, Seq: 2}, {SN: "C", Ts: 1, Seq: 1},
	}
	got := LatestPerSN(rows)
	if len(got) != 3 {
		t.Fatalf("len=%d", len(got))
	}
	if got[0].SN != "A" || got[0].Seq != 3 {
		t.Errorf("A latest=%+v", got[0])
	}
	if got[1].SN != "B" || got[1].Seq != 2 { // 同 ts 取 seq 大
		t.Errorf("B latest=%+v", got[1])
	}
	if got[2].SN != "C" {
		t.Errorf("C=%+v", got[2])
	}
	if LatestPerSN(nil) == nil || len(LatestPerSN(nil)) != 0 {
		t.Error("nil input should give empty slice")
	}
}
