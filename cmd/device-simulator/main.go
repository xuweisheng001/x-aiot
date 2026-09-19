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
	flag.BoolVar(&cfg.Jobs, "jobs", true, "emit JOB_START/JOB_DONE events per work cycle (thing model v1.1)")
	flag.StringVar(&cfg.Module, "module", "LM40", "module_model attribute reported on connect")
	flag.BoolVar(&cfg.JobOptin, "job-optin", false, "initial reported.job_feedback_optin; desired job_feedback_optin overrides and is reported back")
	flag.Int64Var(&cfg.SeqBase, "seq-base", 0, "uplink seq start; 0 = derive from start time (monotonic across runs), <0 = start at 0")
	flag.BoolVar(&cfg.Accessory, "accessory", false, "purifier mode (BL2): report power_on/fan_level/pressure_diff/runtime_h, obey desired; implies -jobs=false and -pk ACC_PURIFIER unless -pk given")
	flag.StringVar(&cfg.PairHost, "pair-host", "", "with -accessory: pair every accessory to this host SN via accessory-svc after bootstrap")
	flag.StringVar(&cfg.AccessoryURL, "accessory-url", "http://127.0.0.1:8092", "accessory-svc base url (for -pair-host)")
	logLevel := flag.String("log", "info", "slog level: debug|info|warn|error")
	flag.Parse()
	if cfg.Accessory {
		cfg.Jobs = false
		if cfg.PK == "LM_S1" {
			cfg.PK = "ACC_PURIFIER"
		}
	}

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
	if cfg.Accessory && cfg.PairHost != "" {
		// 配对要求双方 device 行已存在：等设备完成一次 bootstrap 再调 accessory-svc
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			ok, errs := simulator.PairAccessories(ctx, cfg.AccessoryURL, cfg.PairHost, cfg.SNPrefix, cfg.N)
			slog.Info("accessory pairing", "host", cfg.PairHost, "paired", ok, "failed", len(errs))
			for _, e := range errs {
				slog.Warn("pairing failed", "err", e)
			}
		}()
	}
	simulator.Run(ctx, cfg, os.Stdout)
	slog.Info("simulator stopped")
}
