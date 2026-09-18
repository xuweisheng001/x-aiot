package bridge

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/cellmap"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

type fakeFuture struct {
	ok  chan *jetstream.PubAck
	err chan error
}

func (f *fakeFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *fakeFuture) Err() <-chan error            { return f.err }
func (f *fakeFuture) Msg() *nats.Msg               { return nil }

type fakeJS struct {
	mu       sync.Mutex
	subjects []string
	failAll  bool
	hang     bool
	syncErr  error
}

func (f *fakeJS) Publish(_ context.Context, subj string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if f.syncErr != nil {
		return nil, f.syncErr
	}
	f.mu.Lock()
	f.subjects = append(f.subjects, subj)
	f.mu.Unlock()
	return &jetstream.PubAck{}, nil
}

func (f *fakeJS) PublishAsync(subj string, _ []byte, _ ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	f.mu.Lock()
	f.subjects = append(f.subjects, subj)
	f.mu.Unlock()
	fut := &fakeFuture{ok: make(chan *jetstream.PubAck, 1), err: make(chan error, 1)}
	switch {
	case f.hang:
	case f.failAll:
		fut.err <- errors.New("boom")
	default:
		fut.ok <- &jetstream.PubAck{}
	}
	return fut, nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestBridgePublishesToCellSubject(t *testing.T) {
	js := &fakeJS{}
	b := New(js, nil, nil, Options{Cells: 4, Window: 8, Now: func() int64 { return 42 }})
	b.Start()
	b.Handle("up/LM_S1/XT001/telemetry", []byte(`{"seq":7}`))
	b.Handle("bad/topic", []byte(`{}`))
	waitFor(t, func() bool { return b.Metrics().Get(MPublished) == 1 })
	b.Close(time.Second)
	want := envelope.Subject(envelope.KindTelemetry, cellmap.CellOf("XT001", 4))
	if len(js.subjects) != 1 || js.subjects[0] != want {
		t.Fatalf("subjects=%v want %s", js.subjects, want)
	}
	if b.Metrics().Get(MBadTopic) != 1 || b.Metrics().Get(MReceived) != 2 {
		t.Fatalf("metrics bad_topic=%d received=%d", b.Metrics().Get(MBadTopic), b.Metrics().Get(MReceived))
	}
}

func TestBridgeFailureSemantics(t *testing.T) {
	js := &fakeJS{failAll: true}
	buf, _ := NewDiskBuffer(filepath.Join(t.TempDir(), "b.jsonl"))
	b := New(js, buf, nil, Options{Cells: 2, Window: 8})
	b.Start()
	b.Handle("up/LM_S1/XT001/telemetry", []byte(`{"seq":1}`))
	b.Handle("up/LM_S1/XT001/event", []byte(`{"seq":2,"code":"FLAME_DETECTED"}`))
	b.Handle("up/LM_S1/XT001/cmd_ack", []byte(`{"seq":3}`))
	b.Handle("up/LM_S1/XT001/ota_progress", []byte(`{"seq":4}`))
	waitFor(t, func() bool { return b.Metrics().Get(MPubErr) == 4 })
	b.Close(time.Second)
	if b.Metrics().Get(MDropped) != 1 {
		t.Fatalf("dropped=%d", b.Metrics().Get(MDropped))
	}
	if b.Metrics().Get(MBuffered) != 3 || buf.Len() != 3 {
		t.Fatalf("buffered=%d len=%d", b.Metrics().Get(MBuffered), buf.Len())
	}
	// JetStream 恢复后重放：同步 Publish 成功、缓冲清空
	js.failAll = false
	res, err := b.Replay(context.Background())
	if err != nil || res.Replayed != 3 || buf.Len() != 0 {
		t.Fatalf("replay res=%+v err=%v len=%d", res, err, buf.Len())
	}
	if b.Metrics().Get(MReplayed) != 3 {
		t.Fatalf("replayed metric=%d", b.Metrics().Get(MReplayed))
	}
}

func TestBridgeAckTimeoutBuffersEvent(t *testing.T) {
	js := &fakeJS{hang: true}
	buf, _ := NewDiskBuffer(filepath.Join(t.TempDir(), "b.jsonl"))
	b := New(js, buf, nil, Options{Cells: 2, Window: 8, AckTimeout: 20 * time.Millisecond})
	b.Start()
	b.Handle("up/LM_S1/XT001/event", []byte(`{"seq":2}`))
	waitFor(t, func() bool { return b.Metrics().Get(MBuffered) == 1 })
	b.Close(time.Second)
	if b.Metrics().Get(MPending) != 0 {
		t.Fatalf("pending should be released: %d", b.Metrics().Get(MPending))
	}
}

func TestBridgeHandleAfterCloseBuffers(t *testing.T) {
	js := &fakeJS{}
	buf, _ := NewDiskBuffer(filepath.Join(t.TempDir(), "b.jsonl"))
	b := New(js, buf, nil, Options{Cells: 2, Window: 8})
	b.Start()
	b.Close(time.Second)
	b.Handle("up/LM_S1/XT001/event", []byte(`{"seq":2}`))
	if buf.Len() != 1 || len(js.subjects) != 0 {
		t.Fatalf("len=%d subjects=%v", buf.Len(), js.subjects)
	}
}
