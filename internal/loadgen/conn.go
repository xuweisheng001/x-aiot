package loadgen

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// ConnOpts 是 conn 剧本参数。
type ConnOpts struct {
	N           int
	Rate        float64 // 建连速率 /s
	Target      string
	DialTimeout time.Duration // 默认 5s
	Wait        time.Duration // 连接后等待，默认 1s
	ReadTimeout time.Duration // 探测读超时，默认 200ms
}

func (o *ConnOpts) defaults() {
	if o.DialTimeout <= 0 {
		o.DialTimeout = 5 * time.Second
	}
	if o.Wait <= 0 {
		o.Wait = time.Second
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 200 * time.Millisecond
	}
	if o.Rate <= 0 {
		o.Rate = 500
	}
}

// ConnSummary 是 conn 剧本汇总。
type ConnSummary struct {
	OK, RST, Alive, DialFail int64
	Elapsed                  time.Duration
}

func (s ConnSummary) String() string {
	return fmt.Sprintf("conn summary: dial_ok=%d rst=%d alive=%d dial_fail=%d elapsed=%s (rst = Dial succeeded but 1-byte probe got reset: false-positive connects)",
		s.OK, s.RST, s.Alive, s.DialFail, s.Elapsed.Truncate(time.Millisecond))
}

// RunConn 以 -rate/s 建 N 条裸 TCP 连接，连接后 1s 做 1 字节读探测识别 RST；alive 连接保持到结束。
// 每秒写一行 CSV：t_ms,ok,rst,alive,dial_fail（累计）。
func RunConn(ctx context.Context, opts ConnOpts, csvOut io.Writer) (ConnSummary, error) {
	opts.defaults()
	cw := NewCSVWriter(csvOut, "t_ms", "ok", "rst", "alive", "dial_fail")
	var ok, rst, alive, dialFail atomic.Int64
	held := &connSet{}
	start := time.Now()
	row := func() { cw.Row(time.Since(start).Milliseconds(), ok.Load(), rst.Load(), alive.Load(), dialFail.Load()) }

	var wg sync.WaitGroup
	lim := rate.NewLimiter(rate.Limit(opts.Rate), 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < opts.N; i++ {
			if err := lim.Wait(ctx); err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				out, c := DialProbe(opts.Target, opts.DialTimeout, opts.Wait, opts.ReadTimeout)
				switch out {
				case OutcomeDialFail:
					dialFail.Add(1)
				case OutcomeRST:
					ok.Add(1)
					rst.Add(1)
				case OutcomeAlive:
					ok.Add(1)
					alive.Add(1)
					held.add(c)
				}
			}()
		}
		wg.Wait()
	}()

	t := time.NewTicker(time.Second)
	defer t.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-done:
			break loop
		case <-t.C:
			row()
		}
	}
	<-done
	row()
	held.closeAll()
	return ConnSummary{OK: ok.Load(), RST: rst.Load(), Alive: alive.Load(), DialFail: dialFail.Load(), Elapsed: time.Since(start)}, ctx.Err()
}
