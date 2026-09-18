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
)

// SafetyCodes 是走独立告警通道的安全事件码。
var SafetyCodes = map[string]bool{
	"FLAME_DETECTED": true, "OVER_TEMP": true, "TILT": true, "ESTOP": true, "WATER_FLOW": true,
}

func Subject(kind Kind, cell int) string { return fmt.Sprintf("iot.up.%s.%d", kind, cell) }
func SubjectAll(kind Kind) string        { return fmt.Sprintf("iot.up.%s.*", kind) }

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
