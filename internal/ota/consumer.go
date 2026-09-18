package ota

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

const ConsumerName = "ota-progress"

// RunProgressConsumer 以 durable "ota-progress" 消费 iot.up.ota_progress.*。
func RunProgressConsumer(ctx context.Context, js jetstream.JetStream, svc *Service) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, envelope.StreamUp, jetstream.ConsumerConfig{
		Durable:       ConsumerName,
		FilterSubject: envelope.SubjectAll(envelope.KindOTAProgress),
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
		if _, err := svc.HandleProgress(ctx, env); err != nil {
			if ctx.Err() == nil {
				slog.Error("handle progress failed, nak", "sn", env.SN, "err", err)
			}
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	defer cc.Stop()
	<-ctx.Done()
	return nil
}
