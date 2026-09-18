package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/simulator/mqtt"
)

func countKind(msgs []mqtt.Message, kind string) int {
	n := 0
	for _, m := range msgs {
		if strings.HasSuffix(m.Topic, "/"+kind) {
			n++
		}
	}
	return n
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestDeviceEndToEnd(t *testing.T) {
	br, err := mqtt.NewFakeBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	host, port, _ := strings.Cut(br.Addr(), ":")
	bs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("sn") != "SIM00001" || r.URL.Query().Get("pk") != "LM_S1" {
			http.Error(w, "bad", 400)
			return
		}
		fmt.Fprintf(w, `{"code":0,"data":{"cell_id":1,"mqtt_host":%q,"mqtt_port":%s,"retry_after":0,"cell_map_ver":1}}`, host, port)
	}))
	defer bs.Close()

	cfg := &Config{N: 1, PK: "LM_S1", MQTTURL: "tcp://127.0.0.1:1", Bootstrap: bs.URL, HB: 150 * time.Millisecond,
		Work: 50 * time.Millisecond, Event: EventSpec{Code: "FLAME_DETECTED", At: 100 * time.Millisecond},
		OTAFailRate: 0, SNPrefix: "SIM"}
	stats := &Stats{}
	d := NewDevice(cfg, 1, stats)
	d.otaStep = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()

	waitUntil(t, 3*time.Second, func() bool {
		r := br.Received()
		return countKind(r, "telemetry") >= 3 && countKind(r, "event") >= 2 // FLAME + HEARTBEAT
	})
	// 事件只发一次；心跳 code=HEARTBEAT
	flame, hb := 0, 0
	for _, m := range br.Received() {
		if strings.HasSuffix(m.Topic, "/event") {
			var e Event
			_ = json.Unmarshal(m.Payload, &e)
			switch e.Code {
			case "FLAME_DETECTED":
				flame++
			case "HEARTBEAT":
				hb++
			}
		}
		if !strings.HasPrefix(m.Topic, "up/LM_S1/SIM00001/") {
			t.Fatalf("bad topic %s", m.Topic)
		}
	}
	if flame != 1 || hb < 1 {
		t.Fatalf("flame=%d hb=%d", flame, hb)
	}

	// 指令 → cmd_ack
	br.Push("down/SIM00001/cmd", []byte(`{"cmd_id":"c-1","action":"pause","params":{}}`))
	br.Push("down/SIM00001/cmd", []byte(`{"cmd_id":"c-2","action":"remote_restart"}`))
	waitUntil(t, 3*time.Second, func() bool { return countKind(br.Received(), "cmd_ack") == 2 })
	acks := map[string]string{}
	for _, m := range br.Received() {
		if strings.HasSuffix(m.Topic, "/cmd_ack") {
			var a CmdAck
			_ = json.Unmarshal(m.Payload, &a)
			acks[a.CmdID] = a.Result
		}
	}
	if acks["c-1"] != "ok" || acks["c-2"] != "fail" {
		t.Fatalf("acks %v", acks)
	}

	// OTA → 4 段进度，batch_id 透传
	br.Push("down/SIM00001/cmd", []byte(`{"cmd_id":"c-3","action":"ota","params":{"batch_id":77,"version":"1.1"}}`))
	waitUntil(t, 3*time.Second, func() bool { return countKind(br.Received(), "ota_progress") == 4 })
	var phases []string
	for _, m := range br.Received() {
		if strings.HasSuffix(m.Topic, "/ota_progress") {
			var p OTAProgress
			_ = json.Unmarshal(m.Payload, &p)
			if p.BatchID != 77 {
				t.Fatalf("batch id %d", p.BatchID)
			}
			phases = append(phases, p.Phase)
		}
	}
	if strings.Join(phases, ",") != "notified,downloading,verifying,success" {
		t.Fatalf("phases %v", phases)
	}
	// desired 只记日志，不崩
	br.Push("down/SIM00001/desired", []byte(`{"version":3,"desired":{"power_limit":80}}`))

	// seq 单调
	var last int64
	for _, m := range br.Received() {
		var s struct{ Seq int64 }
		_ = json.Unmarshal(m.Payload, &s)
		if s.Seq <= last {
			t.Fatalf("seq not monotonic: %d after %d", s.Seq, last)
		}
		last = s.Seq
	}
	if stats.Connected.Load() != 1 || stats.Published.Load() == 0 || stats.Acks.Load() == 0 {
		t.Fatalf("stats %+v", StatusLine(1, stats))
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("device did not stop")
	}
	if stats.Connected.Load() != 0 {
		t.Fatal("connected should drop to 0")
	}
}

func TestDeviceFallsBackWhenBootstrapUnreachable(t *testing.T) {
	br, err := mqtt.NewFakeBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	cfg := &Config{N: 1, PK: "LM_S1", MQTTURL: "tcp://" + br.Addr(), Bootstrap: "http://127.0.0.1:1", HB: time.Hour, Work: time.Hour, SNPrefix: "SIM"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, cfg, io.Discard); close(done) }()
	waitUntil(t, 3*time.Second, func() bool { return br.Connections() == 1 && countKind(br.Received(), "telemetry") >= 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestRunStatusAndShutdown(t *testing.T) {
	br, err := mqtt.NewFakeBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	cfg := &Config{N: 3, PK: "LM_S1", MQTTURL: "tcp://" + br.Addr(), HB: time.Hour, Work: 30 * time.Millisecond, SNPrefix: "SIM", BadFirmware: 0.34}
	ctx, cancel := context.WithCancel(context.Background())
	var sb strings.Builder
	var stats *Stats
	done := make(chan struct{})
	go func() { stats = Run(ctx, cfg, &sb); close(done) }()
	waitUntil(t, 3*time.Second, func() bool { return br.Connections() == 3 && countKind(br.Received(), "telemetry") >= 6 })
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}
	if !strings.Contains(sb.String(), "connected=0/3") {
		t.Fatalf("final status line missing: %q", sb.String())
	}
	if stats.Published.Load() < 6 {
		t.Fatal("published")
	}
}
