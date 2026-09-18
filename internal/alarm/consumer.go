package alarm

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

const ConsumerName = "alarm"

// RunConsumer 以 durable pull consumer "alarm" 消费 iot.up.event.*，阻塞直到 ctx 取消。
// Ack 语义：解析失败 Ack 丢弃+计数；squelch 重复 Ack；DB 瞬时错误 Nak（AckWait 30s 后重投）。
func RunConsumer(ctx context.Context, js jetstream.JetStream, svc *Service) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, envelope.StreamUp, jetstream.ConsumerConfig{
		Durable:       ConsumerName,
		FilterSubject: envelope.SubjectAll(envelope.KindEvent),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    -1,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		env, err := envelope.Unmarshal(msg.Data())
		if err != nil {
			svc.M.Inc("envelope_bad")
			_ = msg.Ack()
			return
		}
		outcome, err := svc.HandleEvent(ctx, env)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("handle event failed, nak", "sn", env.SN, "err", err)
			}
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
		if outcome == OutcomeNotified {
			slog.Debug("alarm handled", "sn", env.SN, "outcome", outcome)
		}
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	defer cc.Stop()
	<-ctx.Done()
	return nil
}
