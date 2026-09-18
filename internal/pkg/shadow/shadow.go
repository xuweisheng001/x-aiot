// Package shadow 是 Redis 影子 reported 最新值的读写（key shadow:{sn}）。
// reported 是可再生数据（JetStream 72h 可重放重建），所以放 Redis 换读性能。
package shadow

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func Key(sn string) string { return "shadow:" + sn }

// WriteReported 在 pipeline 中追加 HSET；调用方负责 Exec。
func WriteReported(ctx context.Context, pipe redis.Pipeliner, sn string, fields map[string]any) {
	fields["updated_at"] = time.Now().UnixMilli()
	pipe.HSet(ctx, Key(sn), fields)
}

func Read(ctx context.Context, rdb *redis.Client, sn string) (map[string]string, error) {
	m, err := rdb.HGetAll(ctx, Key(sn)).Result()
	if err != nil {
		return nil, fmt.Errorf("shadow read: %w", err)
	}
	return m, nil
}
