// param-svc：BL4 参数库——版本化参数档、双人审批发布、差异校验、灰度、增量同步、回滚、用户自定义参数、校正系数。端口 :8090。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/xtool/xtool-aiot/internal/param"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

const version = "0.1.0"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db := config.MustPG(ctx)
	defer db.Close()

	snapDir := config.Env("IOT_PARAM_SNAPSHOT_DIR", "./data/params")
	svc := param.NewService(&param.PGStore{DB: db}, param.NewMetrics(), snapDir)

	addr := config.Env("IOT_HTTP_ADDR", ":8090")
	slog.Info("param-svc listening", "addr", addr, "version", version, "snapshot_dir", snapDir)
	if err := httpx.Serve(addr, param.Routes(svc, version), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("param-svc stopped")
}
