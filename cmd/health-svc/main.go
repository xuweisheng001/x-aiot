// health-svc：BL3 耗材与材料——模块健康度小时级批计算、阈值提醒与冷却、SKU 映射与下单归因、材料码校验。端口 :8093。
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/health"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

const version = "0.1.0"

// jsNotifier 把提醒发到 IOT_NOTIFY 流（非安全通道，与 alarm-svc 无共享路径）。
type jsNotifier struct{ js jetstream.JetStream }

func (n *jsNotifier) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := n.js.Publish(ctx, subject, data)
	return err
}

// materialKey 读主密钥；缺失时生成内存随机密钥并 WARN（仅开发，重启后旧码验签失败）。
func materialKey() []byte {
	if v := config.Env("IOT_MATERIAL_KEY", ""); v != "" {
		return []byte(v)
	}
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		slog.Error("generate material key", "err", err)
		os.Exit(1)
	}
	slog.Warn("IOT_MATERIAL_KEY missing: using in-memory key; codes minted now fail to verify after restart (dev only)")
	return k
}

func main() {
	once := flag.Bool("once", false, "run one health batch and exit")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()
	td := tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())

	opt := health.DefaultOptions()
	opt.Interval = config.EnvDuration("IOT_HEALTH_INTERVAL", opt.Interval)
	opt.Cooldown = config.EnvDuration("IOT_HEALTH_COOLDOWN", opt.Cooldown)
	opt.StaleAfter = config.EnvDuration("IOT_HEALTH_STALE_AFTER", opt.StaleAfter)
	opt.ScanWindow = config.EnvDuration("IOT_HEALTH_SCAN_WINDOW", opt.ScanWindow)
	if v := config.Env("IOT_HEALTH_FUSE_RATIO", ""); v != "" {
		var f float64
		if err := json.Unmarshal([]byte(v), &f); err == nil && f > 0 {
			opt.FuseRatio = f
		}
	}
	opt.MaxScans = config.EnvInt("IOT_MATERIAL_MAX_SCANS", opt.MaxScans)
	opt.MaterialKey = materialKey()

	var notifier health.Notifier
	nc, js := config.MustNATS()
	defer nc.Close()
	if err := config.EnsureStreams(ctx, js); err != nil {
		slog.Warn("ensure streams; reminders will be logged only", "err", err)
	} else {
		notifier = &jsNotifier{js: js}
	}

	svc := health.NewService(&health.PGStore{DB: db}, &health.TDSource{TD: td}, &health.RedisShadow{RDB: rdb},
		notifier, health.NewMetrics(), opt)

	if *once {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		rep, err := svc.RunOnce(rctx)
		if err != nil {
			slog.Error("health batch", "err", err)
			os.Exit(1)
		}
		slog.Info("health batch done", "report", rep)
		return
	}

	go svc.Run(ctx)

	addr := config.Env("IOT_HTTP_ADDR", ":8093")
	slog.Info("health-svc listening", "addr", addr, "version", version,
		"interval", opt.Interval, "cooldown", opt.Cooldown, "fuse_ratio", opt.FuseRatio)
	if err := httpx.Serve(addr, health.Routes(svc, version), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("health-svc stopped")
}
