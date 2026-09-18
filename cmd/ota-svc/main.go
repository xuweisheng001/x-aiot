// ota-svc：固件登记、灰度批次圈选/下发、进度回流与熔断。端口 :8086。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/ota"
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
	nc, js := config.MustNATS()
	defer nc.Drain()
	if err := config.EnsureStreams(ctx, js); err != nil {
		slog.Error("ensure streams", "err", err)
		os.Exit(1)
	}
	pub, err := ota.NewMiniMQTT(config.MQTTURL(), fmt.Sprintf("ota-svc-%d", os.Getpid()),
		config.Env("IOT_MQTT_USER", ""), config.Env("IOT_MQTT_PASS", ""))
	if err != nil {
		slog.Error("mqtt", "err", err)
		os.Exit(1)
	}
	go pub.KeepAlive(ctx)

	svc := ota.NewService(db, pub, ota.NewMetrics(), config.EnvInt("IOT_OTA_DISPATCH_RPS", ota.DefaultDispatchRPS))
	go func() {
		if err := ota.RunProgressConsumer(ctx, js, svc); err != nil && ctx.Err() == nil {
			slog.Error("progress consumer exited", "err", err)
			stop()
		}
	}()
	go svc.RunDispatcher(ctx, config.EnvDuration("IOT_OTA_DISPATCH_INTERVAL", 2*time.Second))

	addr := config.Env("IOT_HTTP_ADDR", ":8086")
	slog.Info("ota-svc listening", "addr", addr, "version", version)
	if err := httpx.Serve(addr, ota.Routes(svc, version), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("ota-svc stopped")
}
