package bridge

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

func mkEnv(sn string, seq int64) *envelope.Envelope {
	return &envelope.Envelope{PK: "LM_S1", SN: sn, Kind: envelope.KindEvent, Seq: seq, RecvTs: 1, Payload: []byte(`{"seq":1}`)}
}

func TestDiskBufferAppendReplay(t *testing.T) {
	buf, err := NewDiskBuffer(filepath.Join(t.TempDir(), "sub", "b.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		if err := buf.Append(mkEnv("XT1", i)); err != nil {
			t.Fatal(err)
		}
	}
	if n := buf.Len(); n != 3 {
		t.Fatalf("len=%d", n)
	}
	var got []int64
	res, err := buf.Replay(context.Background(), func(_ context.Context, e *envelope.Envelope) error {
		got = append(got, e.Seq)
		return nil
	})
	if err != nil || res.Replayed != 3 || res.Requeued != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("order: %v", got)
	}
	if n := buf.Len(); n != 0 {
		t.Fatalf("after replay len=%d", n)
	}
	// 空文件再 Replay 无事发生
	res, err = buf.Replay(context.Background(), func(context.Context, *envelope.Envelope) error { return errors.New("x") })
	if err != nil || res.Replayed != 0 {
		t.Fatalf("empty replay: %+v %v", res, err)
	}
}

func TestDiskBufferReplayPartialFailureRequeuesInOrder(t *testing.T) {
	buf, _ := NewDiskBuffer(filepath.Join(t.TempDir(), "b.jsonl"))
	for i := int64(1); i <= 4; i++ {
		_ = buf.Append(mkEnv("XT1", i))
	}
	calls := 0
	res, err := buf.Replay(context.Background(), func(_ context.Context, e *envelope.Envelope) error {
		calls++
		if e.Seq == 2 {
			return errors.New("nats down")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 2 { // 第 2 条失败后不再尝试
		t.Fatalf("calls=%d", calls)
	}
	if res.Replayed != 1 || res.Requeued != 3 {
		t.Fatalf("res=%+v", res)
	}
	// 重发时并发追加的新数据也要保留在后面
	_ = buf.Append(mkEnv("XT1", 5))
	var seqs []int64
	res, err = buf.Replay(context.Background(), func(_ context.Context, e *envelope.Envelope) error {
		seqs = append(seqs, e.Seq)
		return nil
	})
	if err != nil || res.Replayed != 4 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	want := []int64{2, 3, 4, 5}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("seqs=%v", seqs)
		}
	}
	if buf.Len() != 0 {
		t.Fatal("buffer should be empty")
	}
}

func TestDiskBufferSkipsGarbageLines(t *testing.T) {
	buf, _ := NewDiskBuffer(filepath.Join(t.TempDir(), "b.jsonl"))
	_ = buf.appendLines([][]byte{[]byte("garbage"), mkEnv("XT1", 1).Marshal()})
	res, err := buf.Replay(context.Background(), func(context.Context, *envelope.Envelope) error { return nil })
	if err != nil || res.Skipped != 1 || res.Replayed != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}
