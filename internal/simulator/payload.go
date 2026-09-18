package simulator

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Telemetry 对应 spec §4 遥测 JSON。
type Telemetry struct {
	Seq        int64   `json:"seq"`
	Ts         int64   `json:"ts"`
	WorkState  int     `json:"work_state"`
	PowerLevel int     `json:"power_level"`
	TempCavity float64 `json:"temp_cavity"`
	TempWater  float64 `json:"temp_water"`
	FanRPM     int     `json:"fan_rpm"`
	LaserHours float64 `json:"laser_hours"`
	Progress   int     `json:"progress"`
}

// Event 对应 spec §4 事件 JSON。
type Event struct {
	Seq  int64  `json:"seq"`
	Ts   int64  `json:"ts"`
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

type CmdAck struct {
	Seq    int64  `json:"seq"`
	Ts     int64  `json:"ts"`
	CmdID  string `json:"cmd_id"`
	Result string `json:"result"`
	Detail string `json:"detail,omitempty"`
}

type OTAProgress struct {
	Seq       int64  `json:"seq"`
	Ts        int64  `json:"ts"`
	BatchID   int64  `json:"batch_id"`
	Phase     string `json:"phase"`
	Pct       int    `json:"pct"`
	ErrorCode string `json:"error_code"`
}

// Cmd 是 down/{sn}/cmd 载荷。
type Cmd struct {
	CmdID  string          `json:"cmd_id"`
	Action string          `json:"action"`
	Params json.RawMessage `json:"params"`
}

// Desired 是 down/{sn}/desired 载荷。
type Desired struct {
	Version int64          `json:"version"`
	Desired map[string]any `json:"desired"`
}

// WorkCycle 是纯遥测状态机：tick 序号 → work_state/progress。
// 每 22 个 tick 一轮：0 idle, 1 ready, 2..21 working(progress 5..100)。
func WorkCycle(tick int64) (state, progress int) {
	switch p := tick % 22; {
	case p == 0:
		return 0, 0
	case p == 1:
		return 1, 0
	default:
		return 2, int(p-1) * 5
	}
}

// BuildTelemetry 由 (seq, ts, tick, 微扰) 生成遥测；纯函数便于单测。
func BuildTelemetry(seq, ts, tick int64, jitter float64) Telemetry {
	state, progress := WorkCycle(tick)
	t := Telemetry{Seq: seq, Ts: ts, WorkState: state, Progress: progress,
		PowerLevel: 0, TempCavity: 24 + jitter, TempWater: 22 + jitter/2, FanRPM: 0,
		LaserHours: 100 + float64(tick)/720}
	if state == 2 {
		t.PowerLevel = 80 + int(jitter*4)%20
		t.TempCavity = 38 + jitter*3
		t.FanRPM = 3000 + int(jitter*100)
	}
	return t
}

// AckFor 是指令应答策略：remote_restart 固件层拒绝 → fail；其余 ok。
func AckFor(action string) (result, detail string) {
	if action == "remote_restart" {
		return "fail", "remote_restart is not permitted by firmware policy"
	}
	return "ok", ""
}

// OTAPhase 是一段升级进度。
type OTAPhase struct {
	Phase     string
	Pct       int
	ErrorCode string
}

// OTAPhases 返回升级阶段序列：notified→downloading→verifying→success，或在 verifying 后 failed(E_VERIFY)。
func OTAPhases(fail bool) []OTAPhase {
	ph := []OTAPhase{{"notified", 0, ""}, {"downloading", 40, ""}, {"verifying", 90, ""}}
	if fail {
		return append(ph, OTAPhase{"failed", 90, "E_VERIFY"})
	}
	return append(ph, OTAPhase{"success", 100, ""})
}

// BatchIDFrom 从 params 提取 batch_id：兼容数字与字符串；缺失为 0。
func BatchIDFrom(params json.RawMessage) int64 {
	if len(params) == 0 {
		return 0
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(params, &m); err != nil {
		return 0
	}
	raw, ok := m["batch_id"]
	if !ok {
		return 0
	}
	s := strings.Trim(string(raw), `"`)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f)
	}
	return 0
}

// Topics
func TopicUp(pk, sn, kind string) string { return "up/" + pk + "/" + sn + "/" + kind }
func TopicDown(sn string) string         { return "down/" + sn + "/#" }

// DownKind 从 down/{sn}/{kind} 取 kind；不匹配返回空。
func DownKind(topic, sn string) string {
	prefix := "down/" + sn + "/"
	if !strings.HasPrefix(topic, prefix) {
		return ""
	}
	return topic[len(prefix):]
}
