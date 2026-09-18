package deviceapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// AuditConsumerName 是 IOT_CMD 上的 durable 名。
const AuditConsumerName = "cmd-audit"

// AuditConsumerConfig 返回审计消费者配置。
func AuditConsumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       AuditConsumerName,
		FilterSubject: envelope.SubjectCmdAudit,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxAckPending: 1000,
	}
}

// RunAuditConsumer 消费 iot.cmd.audit 落 cmd_audit（ON CONFLICT DO NOTHING），阻塞直到 ctx 取消。
func RunAuditConsumer(ctx context.Context, js jetstream.JetStream, store Store) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, envelope.StreamCmd, AuditConsumerConfig())
	if err != nil {
		return err
	}
	iter, err := cons.Messages(jetstream.PullMaxMessages(100))
	if err != nil {
		return err
	}
	stopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-stopCtx.Done()
		iter.Stop()
	}()
	for {
		msg, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				return nil
			}
			slog.Warn("audit next", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		handleAudit(store, msg)
	}
}

func handleAudit(store Store, msg jetstream.Msg) {
	var rec AuditRecord
	if err := json.Unmarshal(msg.Data(), &rec); err != nil || rec.CmdID == "" || rec.SN == "" {
		slog.Warn("audit poison", "err", err)
		_ = msg.Ack()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.InsertAudit(ctx, rec); err != nil {
		slog.Error("audit insert", "cmd_id", rec.CmdID, "err", err)
		_ = msg.NakWithDelay(2 * time.Second)
		return
	}
	_ = msg.Ack()
}
