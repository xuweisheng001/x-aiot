// alarm-svc：安全事件告警（durable consumer "alarm" + 状态机 + 10 分钟升级）。端口 :8085。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/xtool/xtool-aiot/internal/alarm"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
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

	svc := alarm.NewService(db, rdb, alarm.NewMetrics())
	// BL5 §09.1：有 fleet-svc 就查组织告警目标（500 ms 超时，失败回退个人绑定）；没配就完全不查。
	fleetURL := config.Env("IOT_FLEET_URL", "")
	svc.Targets = alarm.NewTargetLookup(fleetURL)
	go func() {
		if err := alarm.RunConsumer(ctx, js, svc); err != nil && ctx.Err() == nil {
			slog.Error("consumer exited", "err", err)
			stop()
		}
	}()
	go svc.RunEscalation(ctx, alarm.EscalationInterval)
	// 事件与告警对账（INC-10）：TDengine 有安全事件而 PG 无告警 → 补录并计数。
	td := tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())
	rec := alarm.NewReconciler(td, svc,
		config.EnvDuration("IOT_ALARM_RECONCILE_WINDOW", alarm.DefaultReconcileWindow),
		config.EnvDuration("IOT_ALARM_RECONCILE_TOLERANCE", alarm.DefaultReconcileTolerance))
	go rec.Run(ctx, config.EnvDuration("IOT_ALARM_RECONCILE_INTERVAL", alarm.DefaultReconcileInterval))

	addr := config.Env("IOT_HTTP_ADDR", ":8085")
	slog.Info("alarm-svc listening", "addr", addr, "version", version, "fleet_url", fleetURL)
	if err := httpx.Serve(addr, alarm.Routes(svc, version), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("alarm-svc stopped")
}
