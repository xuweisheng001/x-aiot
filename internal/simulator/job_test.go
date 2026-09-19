package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/simulator/mqtt"
)

// 固定向量：sha256(`{"passes":1,"power":60,"speed":100}`)
const wantHash = "0b93a048accbfc88c8c89fca83e927a0862b325647a8d5a28b3bf691c70f5d78"

func TestParamsHashFixedVector(t *testing.T) {
	if got := ParamsHash(DefaultJobParams); got != wantHash {
		t.Fatalf("ParamsHash=%s want %s", got, wantHash)
	}
	// 键顺序无关
	if ParamsHash(map[string]any{"speed": 100, "passes": 1, "power": 60}) != wantHash {
		t.Fatal("hash must be key-order independent")
	}
}

func TestBuildJobFields(t *testing.T) {
	cases := []struct {
		name  string
		optin bool
		want  JobFields
	}{
		{"optin false → 四字段全空", false, JobFields{}},
		{"optin true → 携带并算 hash", true, JobFields{JobID: "j-1", MaterialID: "BASSWOOD_3MM", ParamProfileID: "official-1", ParamsHash: wantHash}},
	}
	for _, c := range cases {
		got := BuildJobFields(c.optin, "j-1", "BASSWOOD_3MM", "official-1", DefaultJobParams)
		if got != c.want {
			t.Errorf("%s: got %+v want %+v", c.name, got, c.want)
		}
		// 序列化后 optin=false 时四个键一律不出现
		b, _ := json.Marshal(JobEvent{Event: Event{Seq: 1, Ts: 2, Code: "JOB_START"}, JobFields: got})
		for _, k := range []string{"job_id", "material_id", "param_profile_id", "params_hash"} {
			has := strings.Contains(string(b), `"`+k+`"`)
			if has != c.optin {
				t.Errorf("%s: key %s present=%v want %v (%s)", c.name, k, has, c.optin, b)
			}
		}
	}
}

func TestJobTransitionAndMaterial(t *testing.T) {
	cases := []struct {
		prev, cur   int
		start, done bool
	}{{0, 0, false, false}, {0, 1, false, false}, {1, 2, true, false}, {2, 2, false, false}, {2, 0, false, true}, {0, 2, true, false}, {2, 1, false, true}}
	for _, c := range cases {
		s, d := JobTransition(c.prev, c.cur)
		if s != c.start || d != c.done {
			t.Errorf("JobTransition(%d,%d)=%v,%v want %v,%v", c.prev, c.cur, s, d, c.start, c.done)
		}
	}
	if MaterialFor(0) != "BASSWOOD_3MM" || MaterialFor(1) != "ACRYLIC_3MM" || MaterialFor(2) != "LEATHER_1MM" || MaterialFor(3) != "BASSWOOD_3MM" {
		t.Fatal("MaterialFor rotation")
	}
	if id := NewJobID(); len(id) != 36 || id[14] != '4' {
		t.Fatalf("bad uuid %s", id)
	}
}

func TestOptinFromDesired(t *testing.T) {
	cases := []struct {
		in    map[string]any
		v, ok bool
	}{
		{map[string]any{"job_feedback_optin": true}, true, true},
		{map[string]any{"job_feedback_optin": false}, false, true},
		{map[string]any{"job_feedback_optin": "true"}, false, false},
		{map[string]any{"power_limit": 80}, false, false},
		{nil, false, false},
	}
	for _, c := range cases {
		v, ok := OptinFromDesired(c.in)
		if v != c.v || ok != c.ok {
			t.Errorf("OptinFromDesired(%v)=%v,%v want %v,%v", c.in, v, ok, c.v, c.ok)
		}
	}
}

// 端到端：optin=false 时 JOB 事件无四字段；desired 打开后下一帧遥测回报 true，之后的 JOB 事件带四字段。
func TestDeviceJobEventsAndOptin(t *testing.T) {
	br, err := mqtt.NewFakeBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	host, port, _ := strings.Cut(br.Addr(), ":")
	bs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"code":0,"data":{"cell_id":1,"mqtt_host":%q,"mqtt_port":%s,"retry_after":0,"cell_map_ver":1}}`, host, port)
	}))
	defer bs.Close()

	cfg := &Config{N: 1, PK: "LM_S1", MQTTURL: "tcp://127.0.0.1:1", Bootstrap: bs.URL, HB: time.Hour,
		Work: 20 * time.Millisecond, SNPrefix: "SIM", Jobs: true, Module: "LM40", JobOptin: false, SeqBase: -1}
	d := NewDevice(cfg, 1, &Stats{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()

	jobEvents := func() (starts, dones []JobEvent) {
		for _, m := range br.Received() {
			if !strings.HasSuffix(m.Topic, "/event") {
				continue
			}
			var e JobEvent
			_ = json.Unmarshal(m.Payload, &e)
			switch e.Code {
			case "JOB_START":
				starts = append(starts, e)
			case "JOB_DONE":
				dones = append(dones, e)
			}
		}
		return
	}
	// 一轮 22 tick ≈ 440ms：等到至少一个完整 START→DONE
	waitUntil(t, 5*time.Second, func() bool { s, dn := jobEvents(); return len(s) >= 1 && len(dn) >= 1 })
	starts, dones := jobEvents()
	if starts[0].JobID != "" || starts[0].ParamsHash != "" || dones[0].MaterialID != "" {
		t.Fatalf("optin=false must not carry job fields: %+v %+v", starts[0], dones[0])
	}
	if dones[0].Msg != "simulated job" {
		t.Fatalf("done event %+v", dones[0])
	}

	// 第一帧遥测带属性
	var first Telemetry
	for _, m := range br.Received() {
		if strings.HasSuffix(m.Topic, "/telemetry") {
			_ = json.Unmarshal(m.Payload, &first)
			break
		}
	}
	if first.ModuleModel != "LM40" || first.JobFeedbackOptin == nil || *first.JobFeedbackOptin {
		t.Fatalf("first telemetry attrs %+v", first)
	}

	// desired 打开 opt-in
	before := len(br.Received())
	br.Push("down/SIM00001/desired", []byte(`{"version":2,"desired":{"job_feedback_optin":true}}`))
	waitUntil(t, 3*time.Second, func() bool { return d.Optin() })
	// 之后的遥测里应出现 job_feedback_optin=true 的回报
	waitUntil(t, 3*time.Second, func() bool {
		for _, m := range br.Received()[before:] {
			if strings.HasSuffix(m.Topic, "/telemetry") {
				var tl Telemetry
				_ = json.Unmarshal(m.Payload, &tl)
				if tl.JobFeedbackOptin != nil && *tl.JobFeedbackOptin && tl.ModuleModel == "LM40" {
					return true
				}
			}
		}
		return false
	})
	// 之后的 JOB_START 带四字段且 hash 为固定向量
	waitUntil(t, 5*time.Second, func() bool {
		s, _ := jobEvents()
		last := s[len(s)-1]
		return last.JobID != "" && last.ParamsHash == wantHash && last.ParamProfileID == DefaultParamProfileID && last.MaterialID != ""
	})
	// 首个 JOB_DONE 的 job_id 与对应 START 一致
	waitUntil(t, 5*time.Second, func() bool {
		s, dn := jobEvents()
		for _, e := range dn {
			if e.JobID == "" {
				continue
			}
			for _, st := range s {
				if st.JobID == e.JobID {
					return true
				}
			}
		}
		return false
	})
	cancel()
	<-done
}
