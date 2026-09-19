package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// fakeMsg 实现 jetstream.Msg，只记录 Ack/Nak 结果。
type fakeMsg struct {
	data    []byte
	subject string
	acked   int
	naked   int
	nakDur  time.Duration
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, errors.New("n/a") }
func (m *fakeMsg) Data() []byte                              { return m.data }
func (m *fakeMsg) Headers() nats.Header                      { return nil }
func (m *fakeMsg) Subject() string                           { return m.subject }
func (m *fakeMsg) Reply() string                             { return "" }
func (m *fakeMsg) Ack() error                                { m.acked++; return nil }
func (m *fakeMsg) DoubleAck(context.Context) error           { m.acked++; return nil }
func (m *fakeMsg) Nak() error                                { m.naked++; return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error        { m.naked++; m.nakDur = d; return nil }
func (m *fakeMsg) InProgress() error                         { return nil }
func (m *fakeMsg) Term() error                               { return nil }
func (m *fakeMsg) TermWithReason(string) error               { return nil }

type fakePub struct {
	fail     error
	subjects []string
	payloads [][]byte
}

func (p *fakePub) Publish(_ context.Context, subject string, payload []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if p.fail != nil {
		return nil, p.fail
	}
	p.subjects = append(p.subjects, subject)
	p.payloads = append(p.payloads, payload)
	return &jetstream.PubAck{Stream: envelope.StreamDLQ}, nil
}

func newPoisonWorker(t *testing.T, pub Publisher) (*Worker, *Metrics) {
	t.Helper()
	m := NewMetrics(AllMetricNames...)
	w := NewWorker(1, nil, nil, nil, m, Config{NakDelay: 2 * time.Second})
	w.DLQ = pub
	return w, m
}

func TestPoison_NoDLQ_AckDrop(t *testing.T) {
	w, m := newPoisonWorker(t, nil)
	msg := &fakeMsg{data: []byte("garbage"), subject: envelope.Subject(envelope.KindTelemetry, 1)}
	w.process(msg, make(chan item, 1))
	if msg.acked != 1 || msg.naked != 0 || m.Get(MPoison) != 1 || m.Get(MDLQ) != 0 {
		t.Fatalf("acked=%d naked=%d poison=%d dlq=%d", msg.acked, msg.naked, m.Get(MPoison), m.Get(MDLQ))
	}
}

func TestPoison_DLQ_PublishThenAck(t *testing.T) {
	pub := &fakePub{}
	w, m := newPoisonWorker(t, pub)
	raw := []byte(`{"sn":"X1","kind":"telemetry","seq":1,"payload":"not-an-object"}`)
	msg := &fakeMsg{data: raw, subject: envelope.Subject(envelope.KindTelemetry, 1)}
	w.process(msg, make(chan item, 1))
	if msg.acked != 1 || msg.naked != 0 {
		t.Fatalf("acked=%d naked=%d", msg.acked, msg.naked)
	}
	if m.Get(MPoison) != 1 || m.Get(MDLQ) != 1 || m.Get(MDLQPublishErr) != 0 {
		t.Fatalf("poison=%d dlq=%d dlq_err=%d", m.Get(MPoison), m.Get(MDLQ), m.Get(MDLQPublishErr))
	}
	if len(pub.subjects) != 1 || pub.subjects[0] != "iot.dlq.telemetry" {
		t.Fatalf("subjects=%v", pub.subjects)
	}
	var rec DLQRecord
	if err := json.Unmarshal(pub.payloads[0], &rec); err != nil {
		t.Fatal(err)
	}
	got, _ := base64.StdEncoding.DecodeString(rec.DataB64)
	if string(got) != string(raw) || rec.Subject != msg.subject || rec.Cell != 1 || rec.Error == "" || rec.Ts == 0 {
		t.Fatalf("record=%+v", rec)
	}
}

func TestPoison_DLQ_PublishFail_Nak(t *testing.T) {
	pub := &fakePub{fail: errors.New("jetstream down")}
	w, m := newPoisonWorker(t, pub)
	msg := &fakeMsg{data: []byte("garbage"), subject: "iot.up.event.2"}
	w.process(msg, make(chan item, 1))
	if msg.acked != 0 || msg.naked != 1 || msg.nakDur != 2*time.Second {
		t.Fatalf("acked=%d naked=%d nakDur=%v", msg.acked, msg.naked, msg.nakDur)
	}
	if m.Get(MDLQPublishErr) != 1 || m.Get(MDLQ) != 0 || m.Get(MNak) != 1 {
		t.Fatalf("dlq_err=%d dlq=%d nak=%d", m.Get(MDLQPublishErr), m.Get(MDLQ), m.Get(MNak))
	}
}

func TestKindFromSubjectAndDLQSubject(t *testing.T) {
	cases := map[string]string{
		"iot.up.telemetry.1":    "iot.dlq.telemetry",
		"iot.up.event.12":       "iot.dlq.event",
		"iot.up.cmd_ack.3":      "iot.dlq.cmd_ack",
		"iot.up.ota_progress.1": "iot.dlq.ota_progress",
		"iot.up.bogus.1":        "iot.dlq.unknown",
		"garbage":               "iot.dlq.unknown",
		"":                      "iot.dlq.unknown",
	}
	for subj, want := range cases {
		if got := envelope.SubjectDLQ(KindFromSubject(subj)); got != want {
			t.Errorf("%q → %q want %q", subj, got, want)
		}
	}
}
