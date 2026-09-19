// pipeline：消费 JetStream iot.up.*.<cell>，解析→富化→幂等→分发，攒批写 TDengine 与 Redis 影子。
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pipeline"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

const (
	name    = "pipeline"
	version = "0.1.0"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	initSchema := flag.Bool("init-schema", false, "execute sql/tdengine.sql via TDengine REST and exit")
	schemaFile := flag.String("schema-file", "sql/tdengine.sql", "TDengine schema file")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	td := tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())

	if *initSchema {
		if err := ensureSchema(ctx, td, *schemaFile); err != nil {
			slog.Error("init schema", "err", err)
			os.Exit(1)
		}
		slog.Info("tdengine schema ensured", "file", *schemaFile)
		return
	}
	// 启动时幂等建表（spec §5）；失败只告警，不阻断启动。
	sctx, scancel := context.WithTimeout(ctx, 20*time.Second)
	if err := ensureSchema(sctx, td, *schemaFile); err != nil {
		slog.Warn("ensure schema at startup", "err", err)
	}
	scancel()

	nc, js := config.MustNATS()
	if err := config.EnsureStreams(ctx, js); err != nil {
		slog.Error("ensure streams", "err", err)
		os.Exit(1)
	}
	rdb := config.MustRedis()
	m := pipeline.NewMetrics(pipeline.AllMetricNames...)
	cfg := pipeline.Config{
		BatchSize: config.EnvInt("IOT_BATCH_SIZE", 500),
		Window:    config.EnvDuration("IOT_BATCH_WINDOW", 120*time.Millisecond),
		Flushers:  config.EnvInt("IOT_FLUSHERS", 1),
	}

	cells := config.Cells()
	var consumers []jetstream.Consumer
	var workers []*pipeline.Worker
	for cell := 1; cell <= cells; cell++ {
		cons, err := js.CreateOrUpdateConsumer(ctx, envelope.StreamUp, pipeline.ConsumerConfig(cell))
		if err != nil {
			slog.Error("create consumer", "cell", cell, "err", err)
			os.Exit(1)
		}
		consumers = append(consumers, cons)
		w := pipeline.NewWorker(cell, cons, rdb, td, m, cfg)
		w.DLQ = js // 毒消息落 IOT_DLQ 而不是丢弃
		workers = append(workers, w)
	}

	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(w *pipeline.Worker) {
			defer wg.Done()
			if err := w.Run(ctx); err != nil {
				slog.Error("worker", "err", err)
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		pipeline.RunLagRefresher(ctx, consumers, m, 2*time.Second)
	}()

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /healthz", httpx.Healthz(name, version))
	addr := config.Env("IOT_HTTP_ADDR", ":8089")
	go func() {
		if err := httpx.Serve(addr, mux, ctx.Done()); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server", "err", err)
		}
	}()
	slog.Info("pipeline started", "cells", cells, "batch_size", cfg.BatchSize, "window", cfg.Window, "flushers", cfg.Flushers, "metrics", addr)

	<-ctx.Done()
	slog.Info("pipeline shutting down")
	wg.Wait() // 消费停止 + 尾批刷出
	_ = nc.Drain()
	_ = rdb.Close()
	slog.Info("pipeline stopped")
}

func ensureSchema(ctx context.Context, td *tdengine.Client, file string) error {
	b, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	// BL3：telemetry_1h 流目标表缺功率档占比列时先 DROP 流与目标表，再由 EnsureSchema 用新定义重建（幂等）
	if rebuilt, err := td.RebuildStreamIfMissing(ctx, "telemetry_1h_s", "telemetry_1h", tdengine.Telemetry1hRequiredCols); err != nil {
		return err
	} else if rebuilt {
		slog.Warn("telemetry_1h stream rebuilt with power-share columns; historical hourly buckets dropped (health-svc falls back to avg_power)")
	}
	return td.EnsureSchema(ctx, pipeline.SplitStatements(string(b)))
}
