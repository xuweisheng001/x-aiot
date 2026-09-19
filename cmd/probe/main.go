// probe：安全事件合成探针（docs/incident-premortem-bl1.md INC-10 P0 根治项）。
// 每 IOT_PROBE_INTERVAL 经真实链路打一发 FLAME_DETECTED，量端到端到推送的时间，
// 超 SLO 告警、未到达按 S1 报错，并清理自己造的告警。端口 :8096。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/probe"
)

const version = "0.1.0"

func main() {
	once := flag.Bool("once", false, "run a single probe and exit (exit 1 if the alarm never arrived)")
	mode := flag.String("mode", "", "emit path: mqtt (full chain, default) | jetstream (skip the access layer)")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opt := probe.DefaultOptions()
	opt.SN = config.Env("IOT_PROBE_SN", opt.SN)
	opt.PK = config.Env("IOT_PROBE_PK", opt.PK)
	opt.Code = config.Env("IOT_PROBE_CODE", opt.Code)
	opt.Interval = config.EnvDuration("IOT_PROBE_INTERVAL", opt.Interval)
	opt.SLO = config.EnvDuration("IOT_PROBE_SLO", opt.SLO)
	opt.Timeout = config.EnvDuration("IOT_PROBE_TIMEOUT", 0)
	opt.NoCleanup = config.Env("IOT_PROBE_CLEANUP", "true") == "false"

	db := config.MustPG(ctx)
	defer db.Close()

	emitter, closeFn := buildEmitter(ctx, strings.TrimSpace(firstNonEmpty(*mode, config.Env("IOT_PROBE_MODE", "mqtt"))))
	defer closeFn()

	p := probe.New(db, emitter, probe.NewMetrics(), opt)

	if *once {
		res, err := p.RunOnce(ctx)
		if err != nil {
			slog.Error("probe failed", "err", err)
			os.Exit(1)
		}
		b, _ := json.Marshal(res)
		slog.Info("probe result", "result", json.RawMessage(b))
		if res.Fatal() {
			os.Exit(1) // 让 cron / CI 能据退出码判 S1
		}
		return
	}

	go p.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz("probe", version))
	mux.Handle("GET /metrics", p.M.Handler())
	mux.HandleFunc("POST /internal/probe/run", func(w http.ResponseWriter, r *http.Request) {
		rctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
		defer cancel()
		res, err := p.RunOnce(rctx)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
			return
		}
		httpx.OK(w, res)
	})

	addr := config.Env("IOT_HTTP_ADDR", ":8096")
	slog.Info("probe listening", "addr", addr, "version", version, "sn", opt.SN,
		"interval", opt.Interval, "slo", opt.SLO, "path", emitter.Name())
	if err := httpx.Serve(addr, httpx.Chain(mux, httpx.Recover, httpx.Logging), ctx.Done()); err != nil && ctx.Err() == nil {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
	slog.Info("probe stopped")
}

func buildEmitter(ctx context.Context, mode string) (probe.Emitter, func()) {
	switch mode {
	case "jetstream":
		nc, js := config.MustNATS()
		if err := config.EnsureStreams(ctx, js); err != nil {
			slog.Warn("ensure streams", "err", err)
		}
		return &probe.JetStreamEmitter{JS: js, Cells: config.Cells()}, nc.Close
	default:
		addr := config.Env("IOT_PROBE_MQTT_ADDR", "127.0.0.1:1884") // 默认走 conn-gate，连接许可网关也在探测范围内
		return &probe.MQTTEmitter{Addr: addr}, func() {}
	}
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}
