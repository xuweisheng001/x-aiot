package loadgen

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/cellmap"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Outcome
	}{
		{"nil = data arrived = alive", nil, OutcomeAlive},
		{"timeout = alive", timeoutErr{}, OutcomeAlive},
		{"wrapped timeout", &net.OpError{Op: "read", Err: timeoutErr{}}, OutcomeAlive},
		{"EOF = rst", errors.New("EOF"), OutcomeRST},
		{"connection reset", &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}, OutcomeRST},
	}
	for _, tc := range tests {
		if got := Classify(tc.err); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

// rstServer 接受连接：mode(i) 为 true 时 SetLinger(0)+Close 发 RST，否则保活。
func rstServer(t *testing.T, mode func(i int) bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []net.Conn
	go func() {
		for i := 0; ; i++ {
			c, err := ln.Accept()
			if err != nil {
				for _, h := range held {
					h.Close()
				}
				return
			}
			if mode(i) {
				c.(*net.TCPConn).SetLinger(0)
				c.Close()
			} else {
				held = append(held, c)
			}
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

func TestProbeDetectsRSTAndAlive(t *testing.T) {
	addr := rstServer(t, func(i int) bool { return i%2 == 0 })
	got := map[Outcome]int{}
	for i := 0; i < 6; i++ {
		o, c := DialProbe(addr, time.Second, 50*time.Millisecond, 100*time.Millisecond)
		got[o]++
		if c != nil {
			c.Close()
		}
	}
	if got[OutcomeRST] != 3 || got[OutcomeAlive] != 3 {
		t.Fatalf("got %v", got)
	}
	if o, _ := DialProbe("127.0.0.1:1", 200*time.Millisecond, 0, 0); o != OutcomeDialFail {
		t.Fatal("closed port must be dial_fail")
	}
}

func TestRunConn(t *testing.T) {
	addr := rstServer(t, func(i int) bool { return i%2 == 1 })
	var buf bytes.Buffer
	sum, err := RunConn(context.Background(), ConnOpts{N: 10, Rate: 1000, Target: addr, Wait: 50 * time.Millisecond, ReadTimeout: 100 * time.Millisecond}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if sum.OK != 10 || sum.RST != 5 || sum.Alive != 5 || sum.DialFail != 0 {
		t.Fatalf("summary %+v", sum)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if lines[0] != "t_ms,ok,rst,alive,dial_fail" || len(lines) < 2 || !strings.HasSuffix(lines[len(lines)-1], ",10,5,5,0") {
		t.Fatalf("csv:\n%s", buf.String())
	}
}

func TestRunStorm(t *testing.T) {
	var n atomic.Int64
	addr := rstServer(t, func(i int) bool { return n.Add(1) <= 3 }) // 前 3 次 accept 拒绝
	var buf bytes.Buffer
	sum, err := RunStorm(context.Background(), StormOpts{N: 4, Target: addr, Timeout: 10 * time.Second, Wait: 20 * time.Millisecond,
		ReadTimeout: 50 * time.Millisecond, Backoff: func(int) time.Duration { return 10 * time.Millisecond }}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if !sum.AllConnected || sum.Accepted != 4 || sum.Rejected != 3 {
		t.Fatalf("summary %+v", sum)
	}
	if !strings.HasPrefix(buf.String(), "t_ms,accepted,rejected\n") || !strings.HasSuffix(strings.TrimSpace(buf.String()), ",4,3") {
		t.Fatalf("csv:\n%s", buf.String())
	}
}

func TestRunStormTimesOut(t *testing.T) {
	addr := rstServer(t, func(int) bool { return true })
	sum, _ := RunStorm(context.Background(), StormOpts{N: 2, Target: addr, Timeout: 300 * time.Millisecond, Wait: 10 * time.Millisecond,
		ReadTimeout: 20 * time.Millisecond, Backoff: func(int) time.Duration { return 20 * time.Millisecond }}, &bytes.Buffer{})
	if sum.AllConnected || sum.Accepted != 0 || sum.Rejected == 0 {
		t.Fatalf("summary %+v", sum)
	}
}

func TestBuildFloodEnvelopes(t *testing.T) {
	msgs := BuildFloodEnvelopes(1000, 2, 100, "LM_S1", "FLOOD", 1_700_000_000_000)
	if len(msgs) != 1000 {
		t.Fatal("count")
	}
	lastSeq := map[string]int64{}
	subjects := map[string]int{}
	for _, m := range msgs {
		if m.Env.Seq != lastSeq[m.Env.SN]+1 {
			t.Fatalf("seq for %s: %d after %d", m.Env.SN, m.Env.Seq, lastSeq[m.Env.SN])
		}
		lastSeq[m.Env.SN] = m.Env.Seq
		if want := envelope.Subject(envelope.KindTelemetry, cellmap.CellOf(m.Env.SN, 2)); m.Subject != want {
			t.Fatalf("subject %s want %s", m.Subject, want)
		}
		subjects[m.Subject]++
		if m.Env.Kind != envelope.KindTelemetry || m.Env.PK != "LM_S1" || !strings.HasPrefix(m.Env.SN, "FLOOD") {
			t.Fatalf("envelope %+v", m.Env)
		}
		e, err := envelope.Unmarshal(m.Env.Marshal())
		if err != nil || e.Seq != m.Env.Seq {
			t.Fatal("roundtrip")
		}
	}
	if len(lastSeq) != 100 {
		t.Fatalf("distinct SNs %d", len(lastSeq))
	}
	if len(subjects) != 2 {
		t.Fatalf("cells used %v", subjects)
	}
	for _, s := range lastSeq {
		if s != 10 {
			t.Fatalf("each SN should have 10 msgs, got %d", s)
		}
	}
}

func TestFloodSummaryReconciliation(t *testing.T) {
	s := FloodSummary{Pre: 10, Before: 5, After: 15}
	if !strings.Contains(s.String(), "RECONCILED") {
		t.Fatal(s.String())
	}
	s.After = 14
	if !strings.Contains(s.String(), "DELTA -1") {
		t.Fatal(s.String())
	}
}

func TestOpenOutCreatesDir(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenOut(dir + "/nested/out.csv")
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	if _, err := os.Stat(dir + "/nested/out.csv"); err != nil {
		t.Fatal(err)
	}
	if w, err := OpenOut(""); err != nil || w == nil {
		t.Fatal("empty path must yield discard writer")
	}
}

// 集成：需要 NATS + TDengine + 运行中的 pipeline（IOT_IT 门控）。
func TestRunFloodIntegration(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	nc, js := config.MustNATS()
	defer nc.Close()
	if err := config.EnsureStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	td := tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())
	sum, err := RunFlood(ctx, FloodOpts{Pre: 200, Cells: config.Cells(), Timeout: time.Minute}, js, td, &bytes.Buffer{}, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(sum.String())
	if !sum.Drained {
		t.Fatal("not drained")
	}
}
