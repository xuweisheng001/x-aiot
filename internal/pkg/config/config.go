// Package config 统一读取环境变量并构建基础依赖客户端。
package config

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func EnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func EnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func NATSURL() string   { return Env("IOT_NATS_URL", nats.DefaultURL) }
func RedisAddr() string { return Env("IOT_REDIS_ADDR", "127.0.0.1:6379") }
func PGDSN() string {
	return Env("IOT_PG_DSN", "postgres://iot:iot@127.0.0.1:5432/iot?sslmode=disable")
}
func TDURL() string   { return Env("IOT_TD_URL", "http://127.0.0.1:6041") }
func TDUser() string  { return Env("IOT_TD_USER", "root") }
func TDPass() string  { return Env("IOT_TD_PASS", "taosdata") }
func MQTTURL() string { return Env("IOT_MQTT_URL", "tcp://127.0.0.1:1883") }
func Cells() int      { return EnvInt("IOT_CELLS", 2) }

// Integration 为真时集成测试打真实依赖，否则 t.Skip。
func Integration() bool { return os.Getenv("IOT_IT") != "" }

func MustPG(ctx context.Context) *pgxpool.Pool {
	pool, err := pgxpool.New(ctx, PGDSN())
	if err != nil {
		panic(fmt.Errorf("pg: %w", err))
	}
	return pool
}

func MustRedis() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: RedisAddr(), DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second})
}

func MustNATS() (*nats.Conn, jetstream.JetStream) {
	nc, err := nats.Connect(NATSURL(), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		panic(fmt.Errorf("nats: %w", err))
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(fmt.Errorf("jetstream: %w", err))
	}
	return nc, js
}

// EnsureStreams 幂等创建 IOT_UP（72h 重放窗口）与 IOT_CMD（30 天审计流）。
func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: envelope.StreamUp, Subjects: []string{"iot.up.>"}, MaxAge: 72 * time.Hour,
		Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		return fmt.Errorf("stream %s: %w", envelope.StreamUp, err)
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: envelope.StreamCmd, Subjects: []string{"iot.cmd.>"}, MaxAge: 30 * 24 * time.Hour,
		Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		return fmt.Errorf("stream %s: %w", envelope.StreamCmd, err)
	}
	return nil
}
