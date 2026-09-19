// bootstrap-svc：接入调度（:8081）。只做装配：读 config → 建依赖 → 起 server。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/bootstrap"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

const name, version = "bootstrap-svc", "0.1.0"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool := config.MustPG(ctx)
	defer pool.Close()
	repo := &bootstrap.PGRepo{Pool: pool}
	cache := bootstrap.NewCellCache(repo, 5*time.Second)
	if err := cache.Refresh(ctx); err != nil {
		slog.Warn("initial cell load failed; will retry in background", "err", err)
	}
	go cache.Run(ctx)

	rdb := config.MustRedis()
	defer rdb.Close()
	srv := &bootstrap.Server{Devices: repo, Cells: cache, CellRepo: repo, NCells: config.Cells(), Dims: &bootstrap.RedisDims{RDB: rdb}}
	addr := config.Env("IOT_HTTP_ADDR", ":8081")
	slog.Info("listening", "svc", name, "addr", addr, "cells", config.Cells())
	if err := httpx.Serve(addr, srv.Handler(name, version), ctx.Done()); err != nil {
		slog.Error("server exit", "err", err)
		os.Exit(1)
	}
	slog.Info("shutdown complete", "svc", name)
}
