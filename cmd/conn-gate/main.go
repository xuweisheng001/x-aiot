// conn-gate：连接许可网关（:1884 → 上游 EMQX 1883），指标 :8084/metrics。
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/conngate"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

const name, version = "conn-gate", "0.1.0"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	listen := config.Env("IOT_GATE_ADDR", ":1884")
	upstream := config.Env("IOT_GATE_UPSTREAM", "127.0.0.1:1883")
	rps, _ := strconv.ParseFloat(config.Env("IOT_GATE_RPS", "5000"), 64)
	burst := config.EnvInt("IOT_GATE_BURST", 200)
	breakN := config.EnvInt("IOT_GATE_BREAK_N", 5)
	breakFor := config.EnvDuration("IOT_GATE_BREAK_FOR", 30*time.Second)
	metricsAddr := config.Env("IOT_GATE_METRICS_ADDR", ":8084")

	m := &conngate.Metrics{}
	p := conngate.NewProxy(upstream, rps, burst, conngate.NewBreaker(breakN, breakFor, nil), m)

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		slog.Error("listen", "addr", listen, "err", err)
		os.Exit(1)
	}
	slog.Info("listening", "svc", name, "addr", listen, "upstream", upstream, "rps", rps, "burst", burst,
		"breaker_n", breakN, "breaker_for", breakFor.String(), "metrics", metricsAddr)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz(name, version))
	mux.Handle("GET /metrics", m)
	go func() {
		if err := httpx.Serve(metricsAddr, mux, ctx.Done()); err != nil {
			slog.Error("metrics server", "err", err)
		}
	}()

	if err := p.Serve(ctx, ln); err != nil {
		slog.Error("proxy exit", "err", err)
		os.Exit(1)
	}
	slog.Info("shutdown complete", "svc", name)
}
