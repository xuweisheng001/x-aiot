// bridge：EMQX 共享订阅 $share/bridge/up/# → 信封 → JetStream iot.up.{kind}.{cell}。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/bridge"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

const (
	name    = "bridge"
	version = "0.1.0"
	shared  = "$share/bridge/up/#"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	window := config.EnvInt("IOT_BRIDGE_WINDOW", 2048)
	nc, _ := config.MustNATS()
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(window))
	if err != nil {
		slog.Error("jetstream", "err", err)
		os.Exit(1)
	}
	if err := config.EnsureStreams(ctx, js); err != nil {
		slog.Warn("ensure streams (continuing, pipeline also ensures)", "err", err)
	}
	buf, err := bridge.NewDiskBuffer(config.Env("IOT_BRIDGE_BUFFER", "./data/bridge-buffer.jsonl"))
	if err != nil {
		slog.Error("buffer", "err", err)
		os.Exit(1)
	}
	b := bridge.New(js, buf, bridge.NewMetrics(), bridge.Options{Cells: config.Cells(), Window: window})
	b.Start()
	b.RunRetryLoop(ctx, 5*time.Second)

	handler := func(_ mqtt.Client, m mqtt.Message) { b.Handle(m.Topic(), m.Payload()) }
	opts := mqtt.NewClientOptions().
		AddBroker(config.MQTTURL()).
		SetClientID(fmt.Sprintf("bridge-%d", os.Getpid())).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetMaxReconnectInterval(10 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetConnectTimeout(5 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			if tok := c.Subscribe(shared, 1, handler); tok.Wait() && tok.Error() != nil {
				slog.Error("mqtt subscribe", "err", tok.Error())
				return
			}
			slog.Info("mqtt subscribed", "topic", shared)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) { slog.Warn("mqtt connection lost", "err", err) })
	cli := mqtt.NewClient(opts)
	if tok := cli.Connect(); tok.Wait() && tok.Error() != nil {
		slog.Error("mqtt connect", "err", tok.Error())
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", b.Metrics().Handler())
	mux.HandleFunc("GET /healthz", httpx.Healthz(name, version))
	addr := config.Env("IOT_HTTP_ADDR", ":8088")
	go func() {
		if err := httpx.Serve(addr, mux, ctx.Done()); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server", "err", err)
		}
	}()
	slog.Info("bridge started", "mqtt", config.MQTTURL(), "nats", config.NATSURL(), "cells", config.Cells(), "metrics", addr)

	<-ctx.Done()
	slog.Info("bridge shutting down")
	if tok := cli.Unsubscribe(shared); !tok.WaitTimeout(2 * time.Second) {
		slog.Warn("mqtt unsubscribe timeout")
	}
	cli.Disconnect(500)
	b.Close(10 * time.Second)
	_ = nc.Drain()
	slog.Info("bridge stopped")
}
