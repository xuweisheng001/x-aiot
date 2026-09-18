package bridge

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/cellmap"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// Publisher 是 Bridge 依赖的 JetStream 子集（jetstream.JetStream 满足），便于测试注入。
type Publisher interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	PublishAsync(subject string, payload []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error)
}

// Options 是 Bridge 可调参数。
type Options struct {
	Cells      int           // 单元数 N
	Window     int           // 未确认的异步发布上限（背压窗口）
	AckTimeout time.Duration // 单条发布等待 ack 的上限，超时按失败处理
	Now        func() int64  // 毫秒时钟（测试注入）
}

func (o *Options) defaults() {
	if o.Cells <= 0 {
		o.Cells = 1
	}
	if o.Window <= 0 {
		o.Window = 2048
	}
	if o.AckTimeout <= 0 {
		o.AckTimeout = 5 * time.Second
	}
	if o.Now == nil {
		o.Now = func() int64 { return time.Now().UnixMilli() }
	}
}

type pending struct {
	f   jetstream.PubAckFuture
	env *envelope.Envelope
}

// Bridge 负责：topic → 信封 → 异步发布；失败语义：telemetry 丢弃计数，其余进磁盘缓冲。
type Bridge struct {
	js  Publisher
	buf *DiskBuffer
	m   *Metrics
	opt Options

	sem  chan struct{}
	acks chan pending

	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

// Metric names.
const (
	MReceived  = "received"
	MPublished = "published"
	MBadTopic  = "bad_topic"
	MDropped   = "dropped_telemetry"
	MBuffered  = "buffered"
	MBufferErr = "buffer_err"
	MReplayed  = "replayed"
	MPubErr    = "publish_err"
	MPending   = "pending"
	MBufferLen = "buffer_len"
)

func New(js Publisher, buf *DiskBuffer, m *Metrics, opt Options) *Bridge {
	opt.defaults()
	if m == nil {
		m = NewMetrics()
	}
	for _, n := range []string{MReceived, MPublished, MBadTopic, MDropped, MBuffered, MBufferErr, MReplayed, MPubErr, MPending, MBufferLen} {
		m.Set(n, 0)
	}
	return &Bridge{js: js, buf: buf, m: m, opt: opt,
		sem: make(chan struct{}, opt.Window), acks: make(chan pending, opt.Window)}
}

func (b *Bridge) Metrics() *Metrics { return b.m }

// Start 启动 ack 等待协程。
func (b *Bridge) Start() {
	b.wg.Add(1)
	go b.ackLoop()
}

func (b *Bridge) ackLoop() {
	defer b.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for p := range b.acks {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(b.opt.AckTimeout)
		select {
		case <-p.f.Ok():
			b.m.Inc(MPublished)
		case err := <-p.f.Err():
			b.onFailure(p.env, err)
		case <-timer.C:
			b.onFailure(p.env, errors.New("ack timeout"))
		}
		<-b.sem
		b.m.Set(MPending, int64(len(b.sem)))
	}
}

// Handle 处理一条 MQTT 上行消息（paho 回调里调用）。
func (b *Bridge) Handle(topic string, payload []byte) {
	b.m.Inc(MReceived)
	tp, err := ParseTopic(topic)
	if err != nil {
		b.m.Inc(MBadTopic)
		slog.Warn("bridge bad topic", "topic", topic)
		return
	}
	env := &envelope.Envelope{PK: tp.PK, SN: tp.SN, Kind: tp.Kind, Seq: ExtractSeq(payload),
		RecvTs: b.opt.Now(), Payload: append([]byte(nil), payload...)}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		b.onFailure(env, errors.New("bridge closing"))
		return
	}
	b.sem <- struct{}{} // 背压：窗口满时阻塞 paho 回调
	f, err := b.js.PublishAsync(b.subject(env), env.Marshal())
	if err != nil {
		<-b.sem
		b.onFailure(env, err)
		return
	}
	b.acks <- pending{f: f, env: env}
}

func (b *Bridge) subject(env *envelope.Envelope) string {
	return envelope.Subject(env.Kind, cellmap.CellOf(env.SN, b.opt.Cells))
}

func (b *Bridge) onFailure(env *envelope.Envelope, err error) {
	b.m.Inc(MPubErr)
	if env.Kind == envelope.KindTelemetry {
		b.m.Inc(MDropped)
		return
	}
	if b.buf == nil {
		b.m.Inc(MBufferErr)
		return
	}
	if aerr := b.buf.Append(env); aerr != nil {
		b.m.Inc(MBufferErr)
		slog.Error("bridge buffer append", "err", aerr, "cause", err)
		return
	}
	b.m.Inc(MBuffered)
	slog.Warn("bridge buffered", "sn", env.SN, "kind", env.Kind, "cause", err)
}

// Replay 同步重放一次磁盘缓冲。
func (b *Bridge) Replay(ctx context.Context) (ReplayResult, error) {
	if b.buf == nil {
		return ReplayResult{}, nil
	}
	res, err := b.buf.Replay(ctx, func(ctx context.Context, env *envelope.Envelope) error {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, perr := b.js.Publish(pctx, b.subject(env), env.Marshal())
		return perr
	})
	b.m.Add(MReplayed, int64(res.Replayed))
	b.m.Set(MBufferLen, int64(b.buf.Len()))
	return res, err
}

// RunRetryLoop 每 interval 重放一次缓冲，直到 ctx 取消。
func (b *Bridge) RunRetryLoop(ctx context.Context, interval time.Duration) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				res, err := b.Replay(ctx)
				if err != nil && !errors.Is(err, context.Canceled) {
					slog.Warn("bridge replay", "err", err, "replayed", res.Replayed, "requeued", res.Requeued)
				} else if res.Replayed > 0 {
					slog.Info("bridge replayed", "n", res.Replayed)
				}
			}
		}
	}()
}

// Close 停止接收新消息，等待在途 ack（最多 timeout），然后回收协程。
// 调用方应先断开 MQTT 再调用（保证不再有 Handle 调用）。
func (b *Bridge) Close(timeout time.Duration) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	close(b.acks)
	b.mu.Unlock()

	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		slog.Warn("bridge close timeout", "pending", len(b.sem))
	}
}
