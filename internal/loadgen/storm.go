package loadgen

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/backoff"
)

// StormOpts 是 storm 剧本参数：N 台同时（零错峰）建连，失败按退避重试。
type StormOpts struct {
	N           int
	Target      string
	Timeout     time.Duration // 默认 120s
	DialTimeout time.Duration
	Wait        time.Duration
	ReadTimeout time.Duration
	Backoff     func(attempt int) time.Duration // 默认 backoff.Next(n, 1s)
}

func (o *StormOpts) defaults() {
	if o.Timeout <= 0 {
		o.Timeout = 120 * time.Second
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 5 * time.Second
	}
	if o.Wait <= 0 {
		o.Wait = time.Second
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 200 * time.Millisecond
	}
	if o.Backoff == nil {
		o.Backoff = func(n int) time.Duration { return backoff.Next(n, time.Second) }
	}
}

type StormSummary struct {
	Accepted, Rejected int64
	Elapsed            time.Duration
	AllConnected       bool
}

func (s StormSummary) String() string {
	return fmt.Sprintf("storm summary: accepted=%d rejected_attempts=%d all_connected=%v elapsed=%s",
		s.Accepted, s.Rejected, s.AllConnected, s.Elapsed.Truncate(time.Millisecond))
}

// RunStorm：全部 N 台零错峰同时连；被拒（dial 失败或 RST）按 Backoff 重试；每秒记录累计 accepted/rejected；
// 直到全部连上或超时。
func RunStorm(ctx context.Context, opts StormOpts, csvOut io.Writer) (StormSummary, error) {
	opts.defaults()
	cw := NewCSVWriter(csvOut, "t_ms", "accepted", "rejected")
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	var accepted, rejected atomic.Int64
	held := &connSet{}
	start := time.Now()
	row := func() { cw.Row(time.Since(start).Milliseconds(), accepted.Load(), rejected.Load()) }

	var wg sync.WaitGroup
	for i := 0; i < opts.N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; ctx.Err() == nil; attempt++ {
				out, c := DialProbe(opts.Target, opts.DialTimeout, opts.Wait, opts.ReadTimeout)
				if out == OutcomeAlive {
					accepted.Add(1)
					held.add(c)
					return
				}
				rejected.Add(1)
				select {
				case <-ctx.Done():
					return
				case <-time.After(opts.Backoff(attempt)):
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	t := time.NewTicker(time.Second)
	defer t.Stop()
loop:
	for {
		select {
		case <-done:
			break loop
		case <-ctx.Done():
			break loop
		case <-t.C:
			row()
		}
	}
	<-done
	row()
	held.closeAll()
	return StormSummary{Accepted: accepted.Load(), Rejected: rejected.Load(), Elapsed: time.Since(start),
		AllConnected: accepted.Load() == int64(opts.N)}, nil
}
