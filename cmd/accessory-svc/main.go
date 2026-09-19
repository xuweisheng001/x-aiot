// accessory-svc：BL2 配件与安全生态——配对、云端联动规则引擎（durable consumer "accessory"）、
// 待关闭定时器、配件告警关联主机、对账兜底、滤芯寿命每小时批。端口 :8092。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/accessory"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

const version = "0.1.0"

func envBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes"
}

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

	store := &accessory.PGStore{Pool: db}
	m := accessory.NewMetrics()
	act := accessory.NewDeviceAPI(config.Env("IOT_DEVICEAPI_URL", "http://127.0.0.1:8083"))
	opt := accessory.Options{
		AllowOff:       envBool("IOT_ACC_ALLOW_OFF", true),
		StopHostOnFire: envBool("IOT_ACC_STOP_HOST_ON_FIRE", true),
		DefaultLevel:   config.EnvInt("IOT_ACC_DEFAULT_LEVEL", accessory.DefaultFanLevel),
		RulesProduct:   config.Env("IOT_ACC_RULES_PRODUCT", "ACC_PURIFIER"),
	}
	if !opt.AllowOff {
		slog.Warn("IOT_ACC_ALLOW_OFF=false：云端不再下发关闭建议（登记到临时关闭表）")
	}
	eng := accessory.NewEngine(store, rdb, act, m, opt)
	td := tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())
	fb := &accessory.FilterBatch{Store: store, TD: td, RDB: rdb, M: m,
		SampleSec: float64(config.EnvInt("IOT_ACC_SAMPLE_SEC", 5)), Now: time.Now}

	if n, err := store.EnsureAuditPartitions(ctx, config.EnvInt("IOT_AUDIT_PARTITION_MONTHS", 3)); err != nil {
		slog.Error("linkage_audit partitions", "err", err)
	} else {
		slog.Info("linkage_audit partitions ensured", "created", n)
	}

	go func() {
		if err := accessory.RunConsumer(ctx, js, eng); err != nil && ctx.Err() == nil {
			slog.Error("consumer exited", "err", err)
			stop()
		}
	}()
	go eng.RunOffTimer(ctx, time.Second)
	go eng.RunReconciler(ctx, config.EnvDuration("IOT_ACC_RECONCILE_INTERVAL", time.Minute))
	go fb.Run(ctx, config.EnvDuration("IOT_ACC_FILTER_INTERVAL", time.Hour))
	go func() {
		t := time.NewTicker(24 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := store.EnsureAuditPartitions(ctx, 3); err != nil {
					slog.Error("linkage_audit partitions", "err", err)
				}
			}
		}
	}()

	// 配对归属对账（INC-2-06 / INC-2-12）：配对时 owner 一致，转让之后就成了越权通道。
	ownerRec := accessory.NewOwnerReconciler(store, rdb, m)
	go ownerRec.Run(ctx, config.EnvDuration("IOT_ACC_OWNER_RECONCILE_INTERVAL", accessory.DefaultOwnerReconcileInterval))

	svc := &accessory.Service{Store: store, RDB: rdb, Engine: eng, Filter: fb, Act: act, M: m, Owner: ownerRec}
	addr := config.Env("IOT_HTTP_ADDR", ":8092")
	slog.Info("accessory-svc listening", "addr", addr, "version", version, "consumer", accessory.ConsumerName,
		"allow_off", opt.AllowOff, "stop_host_on_fire", opt.StopHostOnFire)
	if err := httpx.Serve(addr, accessory.Routes(svc, version), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("accessory-svc stopped")
}
