// job-svc：BL4 加工记录与反哺（durable consumer "job" + 标记接口 + 撤回删除）。端口 :8091。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/job"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
)

const version = "0.1.0"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()
	nc, js := config.MustNATS()
	defer nc.Drain()
	if err := config.EnsureStreams(ctx, js); err != nil {
		slog.Error("ensure streams", "err", err)
		os.Exit(1)
	}

	reader := job.ShadowReaderFunc(func(ctx context.Context, sn string) (map[string]string, error) {
		return shadow.Read(ctx, rdb, sn)
	})
	svc := job.NewService(&job.PGStore{Pool: db}, reader, job.NewMetrics())

	go func() {
		if err := job.RunConsumer(ctx, js, svc); err != nil && ctx.Err() == nil {
			slog.Error("consumer exited", "err", err)
			stop()
		}
	}()
	// 撤回删除（INC-4-26）：以 desired 为触发，默认每 10 分钟扫一轮。
	go svc.RunPurger(ctx, config.EnvDuration("IOT_JOB_PURGE_INTERVAL", 10*time.Minute))

	addr := config.Env("IOT_HTTP_ADDR", ":8091")
	slog.Info("job-svc listening", "addr", addr, "version", version, "consumer", job.ConsumerName)
	if err := httpx.Serve(addr, job.Routes(svc, version), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("job-svc stopped")
}
