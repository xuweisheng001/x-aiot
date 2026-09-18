// device-simulator：N 台虚拟设备（bootstrap → MQTT → 遥测/事件/指令应答/OTA 进度），含坏固件重连风暴注入。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xtool/xtool-aiot/internal/simulator"
)

func main() {
	cfg := &simulator.Config{}
	var eventFlag string
	flag.IntVar(&cfg.N, "n", 50, "number of virtual devices")
	flag.StringVar(&cfg.PK, "pk", "LM_S1", "product key")
	flag.StringVar(&cfg.MQTTURL, "mqtt", "tcp://127.0.0.1:1884", "mqtt broker (fallback when bootstrap unreachable)")
	flag.StringVar(&cfg.Bootstrap, "bootstrap", "http://127.0.0.1:8081", "bootstrap-svc base url (empty = skip)")
	flag.DurationVar(&cfg.HB, "hb", 60*time.Second, "heartbeat interval")
	flag.DurationVar(&cfg.Work, "work", 5*time.Second, "telemetry interval")
	flag.StringVar(&eventFlag, "event", "FLAME_DETECTED@30s", "one-shot event CODE@DELAY (empty = none)")
	flag.Float64Var(&cfg.BadFirmware, "bad-firmware", 0.05, "fraction of devices with fixed-1s reconnect (no backoff)")
	flag.Float64Var(&cfg.OTAFailRate, "ota-fail-rate", 0, "probability an OTA ends in failed/E_VERIFY")
	flag.StringVar(&cfg.SNPrefix, "sn-prefix", "SIM", "SN prefix (SN = prefix + 5-digit index)")
	logLevel := flag.String("log", "info", "slog level: debug|info|warn|error")
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))

	ev, err := simulator.ParseEvent(eventFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	cfg.Event = ev
	if cfg.N <= 0 || cfg.Work <= 0 || cfg.HB <= 0 {
		fmt.Fprintln(os.Stderr, "-n, -work and -hb must be positive")
		os.Exit(2)
	}
	if _, err := simulator.HostPort(cfg.MQTTURL); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	simulator.Run(ctx, cfg, os.Stdout)
	slog.Info("simulator stopped")
}
