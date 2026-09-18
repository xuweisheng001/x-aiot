// deviceapi：影子 / desired / 指令下发 / 指令结果 / 审计消费者 / 遥测查询。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/xtool/xtool-aiot/internal/deviceapi"
	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := config.MustPG(ctx)
	defer pool.Close()
	rdb := config.MustRedis()
	defer rdb.Close()
	nc, js := config.MustNATS()
	if err := config.EnsureStreams(ctx, js); err != nil {
		slog.Error("ensure streams", "err", err)
		os.Exit(1)
	}
	td := tdengine.New(config.TDURL(), config.TDUser(), config.TDPass())

	opts := mqtt.NewClientOptions().
		AddBroker(config.MQTTURL()).
		SetClientID(fmt.Sprintf("deviceapi-%d", os.Getpid())).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetConnectTimeout(5 * time.Second).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) { slog.Warn("mqtt connection lost", "err", err) })
	cli := mqtt.NewClient(opts)
	if tok := cli.Connect(); tok.Wait() && tok.Error() != nil {
		slog.Error("mqtt connect", "err", tok.Error())
		os.Exit(1)
	}

	srv := &deviceapi.Server{
		Store: deviceapi.NewPGStore(pool),
		RDB:   rdb,
		TD:    td,
		MQTT:  &deviceapi.PahoPublisher{Client: cli, Timeout: 5 * time.Second},
		JS:    js,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := deviceapi.RunAuditConsumer(ctx, js, srv.Store); err != nil {
			slog.Error("audit consumer", "err", err)
		}
	}()

	addr := config.Env("IOT_HTTP_ADDR", ":8083")
	hs := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("deviceapi started", "addr", addr)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("deviceapi shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = hs.Shutdown(sctx)
	wg.Wait()
	cli.Disconnect(500)
	_ = nc.Drain()
	slog.Info("deviceapi stopped")
}
