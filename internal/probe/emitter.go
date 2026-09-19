package probe

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/cellmap"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/simulator/mqtt"
)

// ===================== MQTT：覆盖全链路 =====================

// MQTTEmitter 像真实设备一样经 conn-gate / EMQX 发事件，覆盖 bridge 与 JetStream 两跳。
// 每轮重新拨号：探针的价值之一就是顺带验证「现在还能不能接进来」，
// 复用长连接会让接入面故障对探针不可见。
type MQTTEmitter struct {
	Addr           string // conn-gate 或 EMQX 的 host:port
	ConnectTimeout time.Duration
}

func (e *MQTTEmitter) Name() string { return "mqtt" }
func (e *MQTTEmitter) Close()       {}

func (e *MQTTEmitter) Emit(ctx context.Context, sn, pk, code string, seq int64, ts time.Time) error {
	to := e.ConnectTimeout
	if to <= 0 {
		to = 5 * time.Second
	}
	cli, err := mqtt.Dial(ctx, mqtt.Options{
		Addr: e.Addr, ClientID: sn, Username: sn, CleanSession: true,
		KeepAlive: 30 * time.Second, ConnectTimeout: to,
	})
	if err != nil {
		return fmt.Errorf("probe mqtt dial %s: %w", e.Addr, err)
	}
	defer cli.Disconnect()

	payload, err := eventPayload(code, seq, ts)
	if err != nil {
		return err
	}
	// QoS1：安全事件在真实链路上就是 QoS1，探针必须用同一等级才算同一条路
	if err := cli.Publish(fmt.Sprintf("up/%s/%s/event", pk, sn), 1, payload); err != nil {
		return fmt.Errorf("probe mqtt publish: %w", err)
	}
	// 给 PUBACK 与服务端处理留一点时间再断开，避免 CleanSession 断连丢掉未确认的消息
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(200 * time.Millisecond):
	}
	return nil
}

// ===================== JetStream：绕开接入面 =====================

// JetStreamEmitter 直接把信封投进 IOT_UP，覆盖 alarm-svc 之后的半段。
// 接入面已知故障时用它区分「进不来」与「进来了但没告警」。
type JetStreamEmitter struct {
	JS    jetstream.JetStream
	Cells int
}

func (e *JetStreamEmitter) Name() string { return "jetstream" }
func (e *JetStreamEmitter) Close()       {}

func (e *JetStreamEmitter) Emit(ctx context.Context, sn, pk, code string, seq int64, ts time.Time) error {
	body, err := envelopeFor(pk, sn, code, seq, ts)
	if err != nil {
		return err
	}
	n := e.Cells
	if n <= 0 {
		n = 1
	}
	subj := envelope.Subject(envelope.KindEvent, cellmap.CellOf(sn, n))
	if _, err := e.JS.Publish(ctx, subj, body); err != nil {
		return fmt.Errorf("probe jetstream publish %s: %w", subj, err)
	}
	return nil
}
