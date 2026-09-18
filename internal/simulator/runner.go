package simulator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// Run 起 cfg.N 台设备并每 5s 打印一行状态，直到 ctx 取消且所有设备退出。
func Run(ctx context.Context, cfg *Config, status io.Writer) *Stats {
	stats := &Stats{}
	var wg sync.WaitGroup
	for i := 1; i <= cfg.N; i++ {
		d := NewDevice(cfg, i, stats)
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Run(ctx)
		}()
		// 轻微错开起连，避免自造一次风暴（坏固件除外，风暴正是它们的活）
		if !d.Bad && cfg.N > 1 {
			time.Sleep(2 * time.Millisecond)
		}
	}
	slog.Info("simulator started", "n", cfg.N, "pk", cfg.PK, "bad_firmware", cfg.BadFirmware, "event", cfg.Event.Code)

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			fmt.Fprintf(status, "%s\n", StatusLine(cfg.N, stats))
			return stats
		case <-t.C:
			fmt.Fprintf(status, "%s\n", StatusLine(cfg.N, stats))
		}
	}
}

// StatusLine 是一行状态。
func StatusLine(n int, s *Stats) string {
	return fmt.Sprintf("[sim %s] connected=%d/%d reconnects=%d published=%d acks=%d events=%d pub_errors=%d",
		time.Now().Format("15:04:05"), s.Connected.Load(), n, s.Reconnects.Load(), s.Published.Load(), s.Acks.Load(),
		s.Events.Load(), s.PubErrors.Load())
}
