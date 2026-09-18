// cf001-svc：代工机型激活体系（配额工单 / OAEP 认证 / SN 签发 / PSS 自检）。端口 :8087。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/xtool/xtool-aiot/internal/cf001"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

const version = "0.1.0"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	key, generated, err := cf001.LoadOrGenerateKey(config.Env("IOT_CF001_KEY", "./data/keys/dev.pem"))
	if err != nil {
		slog.Error("load key", "err", err)
		os.Exit(1)
	}
	if generated {
		slog.Warn("running with ephemeral key: signatures will not verify after restart (dev only)")
	}
	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()

	svc := cf001.NewService(db, rdb, key)
	addr := config.Env("IOT_HTTP_ADDR", ":8087")
	slog.Info("cf001-svc listening", "addr", addr, "version", version)
	if err := httpx.Serve(addr, cf001.Routes(svc, version), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("cf001-svc stopped")
}
