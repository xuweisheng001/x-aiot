package deviceapi

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

const (
	DefaultTelemetryLimit = 100
	MaxTelemetryLimit     = 1000
	DefaultTelemetryRange = time.Hour
)

// TelemetryRange 是解析后的查询区间（毫秒）与条数。
type TelemetryRange struct {
	From, To int64
	Limit    int
}

// ParseTelemetryRange 解析 from/to/limit：默认最近 1h，limit 默认 100 上限 1000（纯函数）。
func ParseTelemetryRange(q url.Values, now time.Time) (TelemetryRange, error) {
	r := TelemetryRange{To: now.UnixMilli(), Limit: DefaultTelemetryLimit}
	r.From = r.To - DefaultTelemetryRange.Milliseconds()
	var err error
	if v := q.Get("to"); v != "" {
		if r.To, err = strconv.ParseInt(v, 10, 64); err != nil || r.To < 0 {
			return r, errors.New("to must be ms epoch")
		}
		if q.Get("from") == "" {
			r.From = r.To - DefaultTelemetryRange.Milliseconds()
		}
	}
	if v := q.Get("from"); v != "" {
		if r.From, err = strconv.ParseInt(v, 10, 64); err != nil || r.From < 0 {
			return r, errors.New("from must be ms epoch")
		}
	}
	if r.From > r.To {
		return r, errors.New("from must be <= to")
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return r, errors.New("limit must be a positive integer")
		}
		if n > MaxTelemetryLimit {
			n = MaxTelemetryLimit
		}
		r.Limit = n
	}
	return r, nil
}

var telemetryColumns = []string{"ts", "seq", "work_state", "power_level", "temp_cavity", "temp_water", "fan_rpm", "laser_hours", "progress"}

// BuildTelemetryQuery 生成 TDengine 查询 SQL（sn 经 SanitizeSN，区间与条数为整数，无注入面）。
func BuildTelemetryQuery(sn string, r TelemetryRange) string {
	return fmt.Sprintf("SELECT ts,seq,work_state,power_level,temp_cavity,temp_water,fan_rpm,laser_hours,progress FROM iot.t_%s WHERE ts >= %d AND ts <= %d ORDER BY ts DESC LIMIT %d",
		SanitizeSN(sn), r.From, r.To, r.Limit)
}

// RowsToMaps 把 REST 结果按列名转成对象列表（列名优先取 column_meta，缺失时用固定列序）。
func RowsToMaps(res *tdengine.Result) []map[string]any {
	out := make([]map[string]any, 0, len(res.Data))
	names := telemetryColumns
	if len(res.ColumnMeta) > 0 {
		names = make([]string, len(res.ColumnMeta))
		for i, cm := range res.ColumnMeta {
			if len(cm) > 0 {
				if s, ok := cm[0].(string); ok {
					names[i] = s
				}
			}
			if names[i] == "" && i < len(telemetryColumns) {
				names[i] = telemetryColumns[i]
			}
		}
	}
	for _, row := range res.Data {
		m := make(map[string]any, len(row))
		for i, v := range row {
			if i < len(names) {
				m[names[i]] = v
			}
		}
		out = append(out, m)
	}
	return out
}
