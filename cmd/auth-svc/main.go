// auth-svc：EMQX HTTP 认证 / ACL 后端（:8082）。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
	srv := &auth.Server{Store: &auth.PGStore{Pool: pool}}
	addr := config.Env("IOT_HTTP_ADDR", ":8082")
	slog.Info("listening", "svc", name, "addr", addr)
	if err := httpx.Serve(addr, srv.Handler(name, version), ctx.Done()); err != nil {
		slog.Error("server exit", "err", err)
		os.Exit(1)
	}
	slog.Info("shutdown complete", "svc", name)
}
