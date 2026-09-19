// Package envelope 定义 EMQX→NATS 桥接后的统一信封与 subject 规则。
package envelope

import (
	"encoding/json"
	"fmt"
)

type Kind string

const (
	KindTelemetry   Kind = "telemetry"
	KindEvent       Kind = "event"
	KindCmdAck      Kind = "cmd_ack"
	KindOTAProgress Kind = "ota_progress"
)

// Envelope 是所有上行消息进入 JetStream 的统一形态。
type Envelope struct {
	PK      string          `json:"pk"`
	SN      string          `json:"sn"`
	Kind    Kind            `json:"kind"`
	Seq     int64           `json:"seq"`
	RecvTs  int64           `json:"recv_ts"` // 毫秒，桥接接收时间
	Payload json.RawMessage `json:"payload"`
}

const (
	StreamUp        = "IOT_UP"
	StreamCmd       = "IOT_CMD"
	SubjectCmdAudit = "iot.cmd.audit"
	// StreamDLQ：pipeline 解析失败的毒消息不再直接丢弃，而是落到死信流（subjects iot.dlq.>，保留 7 天）供排查与重放。
	StreamDLQ = "IOT_DLQ"
	// StreamNotify：非安全类通知流（耗材提醒等），subjects iot.notify.>，保留 7 天；与 alarm-svc 的安全通道无共享路径（BL3 §7.3）。
	StreamNotify = "IOT_NOTIFY"
)

// SubjectNotify 返回通知 subject "iot.notify.<kind>"（kind 如 consumable）。
func SubjectNotify(kind string) string { return "iot.notify." + kind }

// SafetyCodes 是走独立告警通道的安全事件码。
var SafetyCodes = map[string]bool{
	"FLAME_DETECTED": true, "OVER_TEMP": true, "TILT": true, "ESTOP": true, "WATER_FLOW": true,
	// BL2 配件安全码（净化器 / 灭火器）：走同一独立告警通道，level critical；accessory-svc 另做主机关联与停机第二道
	"FIRE_SUPPRESSED": true, "SMOKE_HIGH": true,
}

// AccessorySafetyCodes 是由配件而非主机上报的安全码子集（accessory-svc 用它区分「主机安全事件→排烟」与「配件安全事件→关联主机」）。
var AccessorySafetyCodes = map[string]bool{"FIRE_SUPPRESSED": true, "SMOKE_HIGH": true}

func Subject(kind Kind, cell int) string { return fmt.Sprintf("iot.up.%s.%d", kind, cell) }

// SubjectDLQ 返回死信 subject "iot.dlq.<kind>"；kind 为空或不在已知集合时用 "unknown"。
func SubjectDLQ(kind Kind) string {
	switch kind {
	case KindTelemetry, KindEvent, KindCmdAck, KindOTAProgress:
		return "iot.dlq." + string(kind)
	}
	return "iot.dlq.unknown"
}
func SubjectAll(kind Kind) string { return fmt.Sprintf("iot.up.%s.*", kind) }

func (e *Envelope) Marshal() []byte { b, _ := json.Marshal(e); return b }

func Unmarshal(b []byte) (*Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	if e.SN == "" || e.Kind == "" {
		return nil, fmt.Errorf("envelope: missing sn/kind")
	}
	return &e, nil
}
