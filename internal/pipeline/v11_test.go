package pipeline

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

func envBytes(kind envelope.Kind, payload string) []byte {
	e := envelope.Envelope{PK: "LM_S1", SN: "SIM00001", Kind: kind, Seq: 7, RecvTs: 1_726_300_000_000, Payload: json.RawMessage(payload)}
	return e.Marshal()
}

func TestParseEventWithAndWithoutJobFields(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    EventPayload
	}{
		{"老固件/未 opt-in：四字段为空", `{"seq":7,"ts":1,"code":"JOB_START","msg":"m"}`,
			EventPayload{Seq: 7, Ts: 1, Code: "JOB_START", Msg: "m"}},
		{"v1.1 opt-in：四字段透传", `{"seq":7,"ts":1,"code":"JOB_DONE","msg":"m","job_id":"11111111-2222-4333-8444-555555555555","material_id":"BASSWOOD_3MM","param_profile_id":"official-1","params_hash":"abc","duration_s":12}`,
			EventPayload{Seq: 7, Ts: 1, Code: "JOB_DONE", Msg: "m", JobID: "11111111-2222-4333-8444-555555555555", MaterialID: "BASSWOOD_3MM", ParamProfileID: "official-1", ParamsHash: "abc"}},
	}
	for _, c := range cases {
		p, err := ParseAndValidate(envBytes(envelope.KindEvent, c.payload))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if *p.Event != c.want {
			t.Errorf("%s: got %+v want %+v", c.name, *p.Event, c.want)
		}
		row := ToEventRow(p.Env, DeviceInfo{PK: "LM_S1"}, p.Event)
		want := tdengine.EventRow{SN: "SIM00001", PK: "LM_S1", Ts: c.want.Ts, Seq: c.want.Seq, Code: c.want.Code, Msg: c.want.Msg,
			JobID: c.want.JobID, MaterialID: c.want.MaterialID, ParamProfileID: c.want.ParamProfileID, ParamsHash: c.want.ParamsHash}
		if row != want {
			t.Errorf("%s: row %+v want %+v", c.name, row, want)
		}
	}
}

func TestExtractTelemetryExtras(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    map[string]any
	}{
		{"无未知字段", `{"seq":1,"ts":2,"work_state":2,"progress":5}`, nil},
		{"v1.1 属性透传", `{"seq":1,"ts":2,"module_model":"LM40","job_feedback_optin":true,"progress":5}`,
			map[string]any{"module_model": "LM40", "job_feedback_optin": true}},
		{"对象/数组/null 丢弃，数值保留", `{"seq":1,"ts":2,"obj":{"a":1},"arr":[1],"nul":null,"fan_rpm":3000,"custom_num":42}`,
			map[string]any{"custom_num": float64(42)}},
		{"非法键名丢弃", `{"seq":1,"ts":2,"Bad-Key":"x","9lead":"y","ok_key":"z"}`, map[string]any{"ok_key": "z"}},
		{"非 JSON", `garbage`, nil},
	}
	for _, c := range cases {
		got := ExtractTelemetryExtras([]byte(c.payload))
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
	// 上限截断
	big := `{"seq":1,"ts":2`
	for i := 0; i < MaxTelemetryExtras+5; i++ {
		big += `,"k_` + string(rune('a'+i%26)) + string(rune('a'+i/26)) + `":1`
	}
	big += `}`
	if got := ExtractTelemetryExtras([]byte(big)); len(got) != MaxTelemetryExtras {
		t.Errorf("extras cap: got %d want %d", len(got), MaxTelemetryExtras)
	}
	// ParseAndValidate 填 Extras
	p, err := ParseAndValidate(envBytes(envelope.KindTelemetry, `{"seq":1,"ts":2,"module_model":"LM40"}`))
	if err != nil || p.Extras["module_model"] != "LM40" {
		t.Fatalf("Parsed.Extras = %v err=%v", p.Extras, err)
	}
}

func TestShadowFieldsWithExtras(t *testing.T) {
	r := tdengine.TelemetryRow{SN: "S", Ts: 5, Seq: 6, WorkState: 2}
	f := ShadowFieldsWithExtras(r, map[string]any{"module_model": "LM40", "work_state": 99})
	if f["module_model"] != "LM40" {
		t.Fatal("extra missing")
	}
	if f["work_state"] != 2 {
		t.Fatal("extras must not override modeled keys")
	}
	if len(ShadowFieldsWithExtras(r, nil)) != len(ShadowFields(r)) {
		t.Fatal("nil extras must be a no-op")
	}
}
