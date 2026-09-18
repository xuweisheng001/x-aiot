// loadgen：压测剧本 conn / storm / flood。结果 CSV 写 -out（默认 docs/loadtest/<name>.csv）。
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

	"github.com/xtool/xtool-aiot/internal/loadgen"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: loadgen <conn|storm|flood> [flags]
  conn  -n 5000 -rate 500 -target 127.0.0.1:1884 -out docs/loadtest/conn.csv
  storm -n 3000 -target 127.0.0.1:1884 -timeout 120s -out docs/loadtest/storm.csv
  flood -pre 30000 -out docs/loadtest/flood.csv   (needs NATS/TDengine and running pipeline)`)
	os.Exit(2)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "conn":
		err = runConn(ctx, os.Args[2:])
	case "storm":
		err = runStorm(ctx, os.Args[2:])
	case "flood":
		err = runFlood(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		slog.Error("loadgen failed", "scenario", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func runConn(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("conn", flag.ExitOnError)
	n := fs.Int("n", 5000, "connections to open")
	rt := fs.Float64("rate", 500, "connections per second")
	target := fs.String("target", "127.0.0.1:1884", "host:port")
	out := fs.String("out", "docs/loadtest/conn.csv", "csv output path")
	wait := fs.Duration("wait", time.Second, "wait after connect before 1-byte probe")
	_ = fs.Parse(args)
	w, err := loadgen.OpenOut(*out)
	if err != nil {
		return err
	}
	defer w.Close()
	sum, err := loadgen.RunConn(ctx, loadgen.ConnOpts{N: *n, Rate: *rt, Target: *target, Wait: *wait}, w)
	fmt.Println(sum.String())
	return err
}

func runStorm(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("storm", flag.ExitOnError)
	n := fs.Int("n", 3000, "devices connecting simultaneously")
	target := fs.String("target", "127.0.0.1:1884", "host:port")
	timeout := fs.Duration("timeout", 120*time.Second, "give up after")
	out := fs.String("out", "docs/loadtest/storm.csv", "csv output path")
	_ = fs.Parse(args)
	w, err := loadgen.OpenOut(*out)
	if err != nil {
		return err
	}
	defer w.Close()
	sum, err := loadgen.RunStorm(ctx, loadgen.StormOpts{N: *n, Target: *target, Timeout: *timeout}, w)
	fmt.Println(sum.String())
	return err
}

func runFlood(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("flood", flag.ExitOnError)
	pre := fs.Int("pre", 30000, "envelopes to pre-publish into JetStream IOT_UP")
	sns := fs.Int("sns", 100, "distinct synthetic SNs (prefix FLOOD)")
	timeout := fs.Duration("timeout", 10*time.Minute, "drain timeout")
	out := fs.String("out", "docs/loadtest/flood.csv", "csv output path")
	_ = fs.Parse(args)
	w, err := loadgen.OpenOut(*out)
	if err != nil {
		return err
	}
	defer w.Close()
	nc, js := config.MustNATS()
	defer nc.Close()
	if err := config.EnsureStreams(ctx, js); err != nil {
		return err
	}
	td := tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())
	sum, err := loadgen.RunFlood(ctx, loadgen.FloodOpts{Pre: *pre, Cells: config.Cells(), SNs: *sns, Timeout: *timeout}, js, td, w, os.Stdout)
	fmt.Println(sum.String())
	return err
}
