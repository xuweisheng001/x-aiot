// Package dedupe 是管道幂等：SETNX dedupe:{sn}:{seq} EX 600。
// Redis 出错时返回 dup=false 与 err：调用方放行并计数——宁可重写明细（TDengine 同 ts 覆盖）不可丢数据。
package dedupe

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const TTL = 10 * time.Minute

func Key(sn string, seq int64) string { return fmt.Sprintf("dedupe:%s:%d", sn, seq) }

// Seen 返回该 (sn, seq) 是否已处理过。
func Seen(ctx context.Context, rdb *redis.Client, sn string, seq int64) (bool, error) {
	ok, err := rdb.SetNX(ctx, Key(sn, seq), 1, TTL).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil
}
