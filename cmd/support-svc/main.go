// support-svc：BL6 售后与保修——诊断包、客服授权自检、Agent 只读代理、错误码字典、批次缺陷早发现、保修数据汇总。端口 :8095。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
	"github.com/xtool/xtool-aiot/internal/support"
)

const version = "0.1.0"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db := config.MustPG(ctx)
	defer db.Close()

	td := tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())
	devAPI := support.NewHTTPDeviceAPI(config.Env("IOT_DEVICEAPI_URL", "http://127.0.0.1:8083"))
	store := &support.PGStore{DB: db}
	svc := support.NewService(store, devAPI, &support.TDSource{C: td}, td, support.NewMetrics())

	// 指令授权对账（INC-6-02）：有人绕过 grant 下 support/agent 指令时，护栏不会报错，只有对账会。
	svc.Audit = support.NewAuditReconciler(store, svc.M,
		config.EnvDuration("IOT_SUPPORT_AUDIT_WINDOW", support.DefaultAuditWindow))
	go svc.Audit.Run(ctx, config.EnvDuration("IOT_SUPPORT_AUDIT_RECONCILE_INTERVAL", support.DefaultAuditReconcileInterval))

	interval := support.DefaultDefectInterval
	if v := config.Env("IOT_SUPPORT_DEFECT_INTERVAL", ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		} else {
			slog.Warn("bad IOT_SUPPORT_DEFECT_INTERVAL, using default", "value", v)
		}
	}
	go svc.RunDefectAggregator(ctx, interval)

	addr := config.Env("IOT_HTTP_ADDR", ":8095")
	slog.Info("support-svc listening", "addr", addr, "version", version, "defect_interval", interval.String())
	if err := httpx.Serve(addr, support.Routes(svc, version), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("support-svc stopped")
}
