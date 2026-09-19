// auth-svc：EMQX HTTP 认证 / ACL 后端（:8082）。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/auth"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

const name, version = "auth-svc", "0.1.0"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool := config.MustPG(ctx)
	defer pool.Close()
	failOpen := config.Env("IOT_AUTH_FAILOPEN", "false") == "true"
	ttl := config.EnvDuration("IOT_AUTH_CACHE_TTL", auth.DefaultCacheTTL)
	m := auth.NewMetrics()
	a := auth.NewAuthenticator(&auth.PGStore{Pool: pool}, failOpen, ttl, m)
	if failOpen {
		slog.Warn("IOT_AUTH_FAILOPEN=true: store errors fall back to recent-auth cache (DEGRADED MODE, register it and turn off after PG recovers)",
			"cache_ttl", ttl.String())
	}
	go a.RunSweeper(ctx, 10*time.Minute)
	srv := &auth.Server{Store: a.Store, Auth: a, M: m}
	addr := config.Env("IOT_HTTP_ADDR", ":8082")
	slog.Info("listening", "svc", name, "addr", addr)
	if err := httpx.Serve(addr, srv.Handler(name, version), ctx.Done()); err != nil {
		slog.Error("server exit", "err", err)
		os.Exit(1)
	}
	slog.Info("shutdown complete", "svc", name)
}
