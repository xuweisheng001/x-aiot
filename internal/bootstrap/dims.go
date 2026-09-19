package bootstrap

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// DimWriter 写设备维表 device:{sn}（pipeline 富化、reco-job 解析机型都依赖它）。
// 原型此前没有任何服务写这张表，导致 pipeline enrich_missing 与 reco unknown_dims；
// 设备首次接入调度时由 bootstrap 写入是最自然的位置：它是第一个知道 sn→pk/region/cell 的服务。
type DimWriter interface {
	WriteDims(ctx context.Context, sn string, fields map[string]any) error
}

// DimFields 纯函数：Device → 维表字段（键名与 spec §4 富化契约一致：pk、region、cell）。
func DimFields(d *Device) map[string]any {
	return map[string]any{
		"pk":     d.ProductKey,
		"region": d.Region,
		"cell":   strconv.Itoa(d.CellID),
	}
}

// DimKey = "device:" + sn。
func DimKey(sn string) string { return "device:" + sn }

// RedisDims 用 HSET 写 device:{sn}。
type RedisDims struct{ RDB *redis.Client }

func (r *RedisDims) WriteDims(ctx context.Context, sn string, fields map[string]any) error {
	return r.RDB.HSet(ctx, DimKey(sn), fields).Err()
}

// DimWriteTimeout 维表写入不阻塞调度响应的上限。
const DimWriteTimeout = time.Second
