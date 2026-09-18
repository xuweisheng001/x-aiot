package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// ErrPoison 表示消息无法解析/校验失败，应 Ack 丢弃并计数。
var ErrPoison = errors.New("poison message")

// TelemetryPayload 是设备遥测 JSON。
type TelemetryPayload struct {
	Seq        int64   `json:"seq"`
	Ts         int64   `json:"ts"`
	WorkState  float64 `json:"work_state"`
	PowerLevel float64 `json:"power_level"`
	TempCavity float64 `json:"temp_cavity"`
	TempWater  float64 `json:"temp_water"`
	FanRPM     float64 `json:"fan_rpm"`
	LaserHours float64 `json:"laser_hours"`
	Progress   float64 `json:"progress"`
}

// EventPayload 是设备事件 JSON。
type EventPayload struct {
	Seq  int64  `json:"seq"`
	Ts   int64  `json:"ts"`
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

// CmdAckPayload 是指令回执 JSON。
type CmdAckPayload struct {
	Seq    int64  `json:"seq"`
	Ts     int64  `json:"ts"`
	CmdID  string `json:"cmd_id"`
	Result string `json:"result"`
	Detail string `json:"detail"`
}

// Parsed 是解析校验后的消息（Telemetry/Event/CmdAck 按 Kind 只填一个）。
type Parsed struct {
	Env       *envelope.Envelope
	Telemetry *TelemetryPayload
	Event     *EventPayload
	CmdAck    *CmdAckPayload
}

// ParseAndValidate 是四步中的第一步：信封 + payload 解析校验。任何失败包裹 ErrPoison。
func ParseAndValidate(data []byte) (*Parsed, error) {
	env, err := envelope.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPoison, err)
	}
	if len(env.Payload) == 0 {
		return nil, fmt.Errorf("%w: empty payload", ErrPoison)
	}
	p := &Parsed{Env: env}
	switch env.Kind {
	case envelope.KindTelemetry:
		var t TelemetryPayload
		if err := json.Unmarshal(env.Payload, &t); err != nil {
			return nil, fmt.Errorf("%w: telemetry: %v", ErrPoison, err)
		}
		if t.Seq <= 0 {
			t.Seq = env.Seq
		}
		if t.Ts <= 0 {
			t.Ts = env.RecvTs
		}
		if t.Seq <= 0 || t.Ts <= 0 {
			return nil, fmt.Errorf("%w: telemetry missing seq/ts", ErrPoison)
		}
		p.Telemetry = &t
	case envelope.KindEvent:
		var e EventPayload
		if err := json.Unmarshal(env.Payload, &e); err != nil {
			return nil, fmt.Errorf("%w: event: %v", ErrPoison, err)
		}
		if e.Seq <= 0 {
			e.Seq = env.Seq
		}
		if e.Ts <= 0 {
			e.Ts = env.RecvTs
		}
		if e.Seq <= 0 || e.Code == "" {
			return nil, fmt.Errorf("%w: event missing seq/code", ErrPoison)
		}
		p.Event = &e
	case envelope.KindCmdAck:
		var a CmdAckPayload
		if err := json.Unmarshal(env.Payload, &a); err != nil {
			return nil, fmt.Errorf("%w: cmd_ack: %v", ErrPoison, err)
		}
		if a.Seq <= 0 {
			a.Seq = env.Seq
		}
		if a.Seq <= 0 || a.CmdID == "" {
			return nil, fmt.Errorf("%w: cmd_ack missing seq/cmd_id", ErrPoison)
		}
		p.CmdAck = &a
	case envelope.KindOTAProgress:
		if !json.Valid(env.Payload) {
			return nil, fmt.Errorf("%w: ota_progress invalid json", ErrPoison)
		}
	default:
		return nil, fmt.Errorf("%w: unknown kind %q", ErrPoison, env.Kind)
	}
	// 统一信封 Seq（dedupe 用）
	switch {
	case p.Telemetry != nil:
		env.Seq = p.Telemetry.Seq
	case p.Event != nil:
		env.Seq = p.Event.Seq
	case p.CmdAck != nil:
		env.Seq = p.CmdAck.Seq
	}
	return p, nil
}

// DeviceInfo 是富化后的设备维度。
type DeviceInfo struct {
	PK, FW, Region string
	Cell           int
}

// MergeDevice 用 Redis 维表字段覆盖默认值（纯函数）：缺失用信封 PK / "unknown" / "US" / subject cell。
func MergeDevice(env *envelope.Envelope, subjectCell int, fields map[string]string) DeviceInfo {
	d := DeviceInfo{PK: env.PK, FW: "unknown", Region: "US", Cell: subjectCell}
	if v := fields["pk"]; v != "" {
		d.PK = v
	}
	if v := fields["fw"]; v != "" {
		d.FW = v
	}
	if v := fields["region"]; v != "" {
		d.Region = v
	}
	if v := fields["cell"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			d.Cell = n
		}
	}
	return d
}

// CellFromSubject 解析 iot.up.<kind>.<cell> 的 cell；失败返回 0。
func CellFromSubject(subject string) int {
	i := strings.LastIndexByte(subject, '.')
	if i < 0 {
		return 0
	}
	n, err := strconv.Atoi(subject[i+1:])
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// ToTelemetryRow 把 payload + 维度映射为 TDengine 行（纯函数）。
func ToTelemetryRow(env *envelope.Envelope, d DeviceInfo, t *TelemetryPayload) tdengine.TelemetryRow {
	return tdengine.TelemetryRow{
		SN: env.SN, PK: d.PK, FW: d.FW, Region: d.Region, Cell: d.Cell,
		Ts: t.Ts, Seq: t.Seq,
		WorkState: int(t.WorkState), PowerLevel: int(t.PowerLevel),
		TempCavity: t.TempCavity, TempWater: t.TempWater,
		FanRPM: int(t.FanRPM), LaserHours: t.LaserHours, Progress: int(t.Progress),
	}
}

// ShadowFields 是写入 shadow:{sn} 的 reported 字段。
func ShadowFields(r tdengine.TelemetryRow) map[string]any {
	return map[string]any{
		"work_state": r.WorkState, "power_level": r.PowerLevel,
		"temp_cavity": r.TempCavity, "temp_water": r.TempWater,
		"fan_rpm": r.FanRPM, "laser_hours": r.LaserHours, "progress": r.Progress,
		"seq": r.Seq, "ts": r.Ts,
	}
}

// EventShadowFields 是事件写入影子的 last_event 字段。
func EventShadowFields(e *EventPayload) map[string]any {
	return map[string]any{"last_event_code": e.Code, "last_event_msg": e.Msg, "last_event_ts": e.Ts, "last_event_seq": e.Seq}
}

// LatestPerSN 每 SN 取批内最新一行（Ts 大者优先，Ts 相同 Seq 大者优先），保持首次出现顺序。
func LatestPerSN(rows []tdengine.TelemetryRow) []tdengine.TelemetryRow {
	idx := map[string]int{}
	out := make([]tdengine.TelemetryRow, 0, len(rows))
	for _, r := range rows {
		i, ok := idx[r.SN]
		if !ok {
			idx[r.SN] = len(out)
			out = append(out, r)
			continue
		}
		cur := out[i]
		if r.Ts > cur.Ts || (r.Ts == cur.Ts && r.Seq > cur.Seq) {
			out[i] = r
		}
	}
	return out
}
