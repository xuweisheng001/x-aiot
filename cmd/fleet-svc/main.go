// fleet-svc：BL5 教育与 B 端——组织 / 站点 / 成员 / 设备归属、多机看板、课表锁对账、班级任务队列与调度器、耗材集采汇总。端口 :8094。
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/xtool/xtool-aiot/internal/fleet"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

const version = "0.1.0"

func main() {
	reconcileOnce := flag.Bool("reconcile-once", false, "run one schedule reconcile round and exit")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()

	opt := fleet.DefaultOptions()
	opt.ReconcileInterval = config.EnvDuration("IOT_FLEET_RECONCILE_INTERVAL", opt.ReconcileInterval)
	opt.DispatchInterval = config.EnvDuration("IOT_FLEET_DISPATCH_INTERVAL", opt.DispatchInterval)
	opt.ExpireInterval = config.EnvDuration("IOT_FLEET_EXPIRE_INTERVAL", opt.ExpireInterval)
	opt.UnlockTTL = config.EnvDuration("IOT_FLEET_UNLOCK_TTL", opt.UnlockTTL)
	opt.ApprovedTTL = config.EnvDuration("IOT_FLEET_APPROVED_TTL", opt.ApprovedTTL)
	opt.DispatchedTTL = config.EnvDuration("IOT_FLEET_DISPATCHED_TTL", opt.DispatchedTTL)
	opt.MaxRetry = config.EnvInt("IOT_FLEET_MAX_RETRY", opt.MaxRetry)

	svc := fleet.NewService(fleet.NewPGStore(db), &fleet.RedisShadow{RDB: rdb}, fleet.NewMetrics(), opt)

	if *reconcileOnce {
		n, err := svc.ReconcileOnce(ctx)
		if err != nil {
			slog.Error("reconcile", "err", err)
			os.Exit(1)
		}
		slog.Info("reconcile done", "patches", n)
		return
	}

	devURL := config.Env("IOT_DEVICEAPI_URL", "http://127.0.0.1:8083")
	otaURL := config.Env("IOT_OTA_URL", "http://127.0.0.1:8086")
	svc.OTA = fleet.NewHTTPOTAClient(otaURL) // 组织批量 OTA 建子批次（BL5 §08）
	sch := fleet.NewScheduler(svc, fleet.NewHTTPDispatcher(devURL))
	go svc.RunReconcile(ctx)
	go sch.Run(ctx)

	addr := config.Env("IOT_HTTP_ADDR", ":8094")
	slog.Info("fleet-svc listening", "addr", addr, "version", version, "deviceapi", devURL, "ota", otaURL,
		"reconcile", opt.ReconcileInterval, "dispatch", opt.DispatchInterval, "unlock_ttl", opt.UnlockTTL)
	if err := httpx.Serve(addr, fleet.Routes(svc, version), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("fleet-svc stopped")
}
