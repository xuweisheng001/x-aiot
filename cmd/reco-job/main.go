// reco-job：BL4 推荐离线批（候选生成 / 功率上限 / 安全关联下线 / 校正系数分布）。
//
//	go run ./cmd/reco-job -once            跑一轮退出
//	go run ./cmd/reco-job -interval 24h    常驻，立即跑一轮后每 24h 一轮
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
	"github.com/xtool/xtool-aiot/internal/reco"
)

func main() {
	once := flag.Bool("once", false, "run one batch and exit")
	interval := flag.Duration("interval", 24*time.Hour, "batch interval when not -once")
	noTD := flag.Bool("no-tdengine", false, "skip safety correlation (no TDengine)")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()
	var td reco.TDQuerier
	if !*noTD {
		td = tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())
	}
	r := reco.NewRunner(db, rdb, td, config.EnvInt("IOT_RECO_WINDOW_DAYS", 30))

	if *once {
		if _, err := r.RunOnce(ctx); err != nil {
			slog.Error("reco run failed", "err", err)
			os.Exit(1)
		}
		return
	}
	slog.Info("reco-job started", "interval", interval.String(), "window_days", r.WindowDays)
	r.Run(ctx, *interval)
	slog.Info("reco-job stopped")
}
