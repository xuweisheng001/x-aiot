package job

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// ConsumerName 是 IOT_UP 上的 durable 名；与 pipeline-<cell>、alarm 并列，互不阻塞。
const ConsumerName = "job"

// ConsumerConfig 返回默认 durable consumer 配置（导出便于测试与运维核对）。
func ConsumerConfig() jetstream.ConsumerConfig { return ConsumerConfigNamed(ConsumerName) }

// ConsumerConfigNamed 用指定 durable 名构造配置。测试与运维重放需要与常驻服务不同的消费组，
// 否则两边抢同一个 durable 的消息（一条消息只投一个成员）。
func ConsumerConfigNamed(durable string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: envelope.SubjectAll(envelope.KindEvent),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    -1,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}
}

// RunConsumer 阻塞消费直到 ctx 取消。Ack 语义：信封 / 载荷坏、非 JOB、合规或格式丢弃 → Ack；PG 错误 → Nak。
func RunConsumer(ctx context.Context, js jetstream.JetStream, svc *Service) error {
	return RunConsumerNamed(ctx, js, svc, ConsumerName)
}

// RunConsumerNamed 用指定 durable 名消费（测试隔离与运维重放用）。
func RunConsumerNamed(ctx context.Context, js jetstream.JetStream, svc *Service, durable string) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, envelope.StreamUp, ConsumerConfigNamed(durable))
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
		if _, err := svc.HandleEvent(ctx, env); err != nil {
			if ctx.Err() == nil {
				slog.Error("job event failed, nak", "sn", env.SN, "err", err)
			}
			_ = msg.NakWithDelay(2 * time.Second)
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
