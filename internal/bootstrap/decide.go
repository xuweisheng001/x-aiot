// Package bootstrap 实现接入调度：sn → cell → mqtt 地址 / retry_after。
package bootstrap

import (
	"regexp"
	"sort"
)

// Cell 是 iot_global.cell 的内存镜像。
type Cell struct {
	ID       int    `json:"cell_id"`
	Region   string `json:"region"`
	MQTTHost string `json:"mqtt_host"`
	MQTTPort int    `json:"mqtt_port"`
	Status   string `json:"status"` // active/draining/standby/overloaded
}

// Device 是 iot_shard.device 的调度所需子集。
type Device struct {
	SN         string
	ProductKey string
	Region     string
	CellID     int
	CellMapVer int
	Status     string
}

// Response 是 GET /api/v1/bootstrap 的 data。
type Response struct {
	CellID     int    `json:"cell_id"`
	MQTTHost   string `json:"mqtt_host"`
	MQTTPort   int    `json:"mqtt_port"`
	RetryAfter int    `json:"retry_after"`
	CellMapVer int    `json:"cell_map_ver"`
}

const (
	StatusActive     = "active"
	StatusDraining   = "draining"
	StatusStandby    = "standby"
	StatusOverloaded = "overloaded"

	// RetryAfterSeconds 是单元过载/排空时让设备稍后再来的秒数。
	RetryAfterSeconds = 30
)

var snRe = regexp.MustCompile(`^[A-Z0-9_-]{4,32}$`)

// ValidSN 与 auth-svc 同一 SN 格式规则。
func ValidSN(sn string) bool { return snRe.MatchString(sn) }

// Decide 是纯调度决策：
//   - active/standby：直接给该单元；
//   - overloaded：仍给该单元，但 retry_after=30；
//   - draining：重定向到编号最小的 standby 单元（无 standby 则原单元），retry_after=30。
//
// ok=false 表示 home 单元不存在（cell 表缺行）。
func Decide(cells map[int]Cell, home int, cellMapVer int) (Response, bool) {
	c, ok := cells[home]
	if !ok {
		return Response{}, false
	}
	resp := Response{CellID: c.ID, MQTTHost: c.MQTTHost, MQTTPort: c.MQTTPort, CellMapVer: cellMapVer}
	switch c.Status {
	case StatusOverloaded:
		resp.RetryAfter = RetryAfterSeconds
	case StatusDraining:
		resp.RetryAfter = RetryAfterSeconds
		if sb, found := pickStandby(cells); found {
			resp.CellID, resp.MQTTHost, resp.MQTTPort = sb.ID, sb.MQTTHost, sb.MQTTPort
		}
	}
	return resp, true
}

func pickStandby(cells map[int]Cell) (Cell, bool) {
	ids := make([]int, 0, len(cells))
	for id, c := range cells {
		if c.Status == StatusStandby {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return Cell{}, false
	}
	sort.Ints(ids)
	return cells[ids[0]], true
}
