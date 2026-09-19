package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/dedupe"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// Metric names.
const (
	MConsumed      = "consumed"
	MPoison        = "poison"
	MDup           = "dup"
	MDedupeRedis   = "dedupe_redis_err"
	MBatches       = "batches"
	MRowsWritten   = "rows_written"
	MNak           = "nak"
	MLag           = "lag"
	MEvents        = "events"
	MCmdAcks       = "cmd_acks"
	MOTAAcked      = "ota_acked"
	MEnrichErr     = "enrich_err"
	MShadowErr     = "shadow_err"
	MAckErr        = "ack_err"
	MEnrichMissing = "enrich_missing"
	MDLQ           = "dlq"             // 毒消息成功落死信
	MDLQPublishErr = "dlq_publish_err" // 落死信失败（Nak 重投）
)

// AllMetricNames 用于预注册，保证 /metrics 输出稳定。
var AllMetricNames = []string{MConsumed, MPoison, MDup, MDedupeRedis, MBatches, MRowsWritten, MNak, MLag,
	MEvents, MCmdAcks, MOTAAcked, MEnrichErr, MShadowErr, MAckErr, MEnrichMissing, MDLQ, MDLQPublishErr}

// Publisher 是死信发布依赖的 JetStream 子集（jetstream.JetStream 满足）。
type Publisher interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// DLQRecord 是落到 IOT_DLQ 的包装：原始字节 base64 + 来源 subject + 解析错误 + 时间，便于排查与重放。
type DLQRecord struct {
	Subject string `json:"subject"`
	Cell    int    `json:"cell"`
	Error   string `json:"error"`
	Ts      int64  `json:"ts"` // 毫秒
	DataB64 string `json:"data_b64"`
}

// TDWriter 是 Worker 依赖的 TDengine 子集（*tdengine.Client 满足）。
type TDWriter interface {
	BatchInsert(ctx context.Context, rows []tdengine.TelemetryRow) error
	InsertEvents(ctx context.Context, rows []tdengine.EventRow) error
}

// Config 是攒批参数。
type Config struct {
	BatchSize    int
	Window       time.Duration
	Flushers     int
	FlushTimeout time.Duration // 单批 TDengine+Redis 写入上限，需 < AckWait(30s)
	OpTimeout    time.Duration // 单条 Redis 操作上限
	NakDelay     time.Duration // Nak 后延迟重投，避免 TDengine 故障时热循环
}

func (c *Config) defaults() {
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.Window <= 0 {
		c.Window = 120 * time.Millisecond
	}
	if c.Flushers <= 0 {
		c.Flushers = 1
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = 20 * time.Second
	}
	if c.OpTimeout <= 0 {
		c.OpTimeout = 2 * time.Second
	}
	if c.NakDelay < 0 {
		c.NakDelay = 0
	} else if c.NakDelay == 0 {
		c.NakDelay = 2 * time.Second
	}
}

// ConsumerConfig 返回 cell 对应 durable pull consumer 的配置。
func ConsumerConfig(cell int) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable: ConsumerName(cell),
		FilterSubjects: []string{
			envelope.Subject(envelope.KindTelemetry, cell),
			envelope.Subject(envelope.KindEvent, cell),
			envelope.Subject(envelope.KindCmdAck, cell),
			envelope.Subject(envelope.KindOTAProgress, cell),
		},
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxAckPending: 5000,
	}
}

// ConsumerName = "pipeline-<cell>"。
func ConsumerName(cell int) string { return "pipeline-" + strconv.Itoa(cell) }

type item struct {
	msg    jetstream.Msg
	env    *envelope.Envelope
	row    tdengine.TelemetryRow
	extras map[string]any // v1.1 未建模标量属性（module_model 等）
}

// Worker 消费一个 cell 的 durable consumer，四步处理 + 攒批刷写。
type Worker struct {
	cell int
	cons jetstream.Consumer
	rdb  *redis.Client
	td   TDWriter
	m    *Metrics
	cfg  Config
	// DLQ 非 nil 时解析失败的消息发布到 envelope.SubjectDLQ 后再 Ack；nil 退回"直接 Ack 丢弃"的旧行为。
	DLQ Publisher
}

func NewWorker(cell int, cons jetstream.Consumer, rdb *redis.Client, td TDWriter, m *Metrics, cfg Config) *Worker {
	cfg.defaults()
	if m == nil {
		m = NewMetrics(AllMetricNames...)
	}
	return &Worker{cell: cell, cons: cons, rdb: rdb, td: td, m: m, cfg: cfg}
}

// Run 阻塞直到 ctx 取消且在途批次刷完。
func (w *Worker) Run(ctx context.Context) error {
	in := make(chan item, w.cfg.BatchSize)
	batches := make(chan []item, w.cfg.Flushers)
	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Flushers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batches {
				w.flush(b)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(batches)
		w.batchLoop(in, batches)
	}()
	err := w.consume(ctx, in) // 返回前关闭 in
	wg.Wait()
	return err
}

func (w *Worker) consume(ctx context.Context, in chan<- item) error {
	defer close(in)
	iter, err := w.cons.Messages(jetstream.PullMaxMessages(w.cfg.BatchSize))
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
			slog.Warn("pipeline next", "cell", w.cell, "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		w.process(msg, in)
	}
}

func (w *Worker) opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), w.cfg.OpTimeout)
}

// process 四步：解析校验 → 富化 → 幂等 → 分发。
func (w *Worker) process(msg jetstream.Msg, in chan<- item) {
	w.m.Inc(MConsumed)
	p, err := ParseAndValidate(msg.Data())
	if err != nil {
		w.m.Inc(MPoison)
		slog.Warn("pipeline poison", "cell", w.cell, "subject", msg.Subject(), "err", err)
		w.handlePoison(msg, err)
		return
	}
	env := p.Env
	if env.Kind == envelope.KindOTAProgress {
		// ota-svc 有自己的 consumer 处理进度；这里不做 dedupe（避免占用同一 dedupe key 影响 ota-svc）。
		w.m.Inc(MOTAAcked)
		w.ack(msg)
		return
	}

	ctx, cancel := w.opCtx()
	defer cancel()
	fields, err := w.rdb.HGetAll(ctx, "device:"+env.SN).Result()
	if err != nil {
		w.m.Inc(MEnrichErr)
		fields = nil
	} else if len(fields) == 0 {
		w.m.Inc(MEnrichMissing)
	}
	cell := CellFromSubject(msg.Subject())
	if cell == 0 {
		cell = w.cell
	}
	dev := MergeDevice(env, cell, fields)

	dup, err := dedupe.Seen(ctx, w.rdb, env.SN, env.Seq)
	if err != nil {
		w.m.Inc(MDedupeRedis) // 放行
	} else if dup {
		w.m.Inc(MDup)
		w.ack(msg)
		return
	}

	switch env.Kind {
	case envelope.KindTelemetry:
		in <- item{msg: msg, env: env, row: ToTelemetryRow(env, dev, p.Telemetry), extras: p.Extras}
	case envelope.KindEvent:
		w.handleEvent(ctx, msg, env, dev, p.Event)
	case envelope.KindCmdAck:
		w.handleCmdAck(ctx, msg, env, p.CmdAck)
	}
}

// handlePoison：有 DLQ 则先落死信（成功 Ack，失败 Nak 重投）；无 DLQ 直接 Ack 丢弃。
func (w *Worker) handlePoison(msg jetstream.Msg, perr error) {
	if w.DLQ == nil {
		w.ack(msg)
		return
	}
	ctx, cancel := w.opCtx()
	defer cancel()
	rec := DLQRecord{Subject: msg.Subject(), Cell: w.cell, Error: perr.Error(), Ts: time.Now().UnixMilli(),
		DataB64: base64.StdEncoding.EncodeToString(msg.Data())}
	body, _ := json.Marshal(rec)
	if _, err := w.DLQ.Publish(ctx, envelope.SubjectDLQ(KindFromSubject(msg.Subject())), body); err != nil {
		w.m.Inc(MDLQPublishErr)
		slog.Error("pipeline dlq publish", "cell", w.cell, "subject", msg.Subject(), "err", err)
		var nerr error
		if w.cfg.NakDelay > 0 {
			nerr = msg.NakWithDelay(w.cfg.NakDelay)
		} else {
			nerr = msg.Nak()
		}
		if nerr != nil {
			w.m.Inc(MAckErr)
		}
		w.m.Inc(MNak)
		return
	}
	w.m.Inc(MDLQ)
	w.ack(msg)
}

// KindFromSubject 从 "iot.up.<kind>.<cell>" 取 kind；格式不符返回空。
func KindFromSubject(subject string) envelope.Kind {
	parts := strings.Split(subject, ".")
	if len(parts) >= 3 && parts[0] == "iot" && parts[1] == "up" {
		return envelope.Kind(parts[2])
	}
	return ""
}

func (w *Worker) handleEvent(ctx context.Context, msg jetstream.Msg, env *envelope.Envelope, dev DeviceInfo, e *EventPayload) {
	fctx, cancel := context.WithTimeout(context.Background(), w.cfg.FlushTimeout)
	defer cancel()
	row := ToEventRow(env, dev, e)
	if err := w.td.InsertEvents(fctx, []tdengine.EventRow{row}); err != nil {
		slog.Error("pipeline event insert", "cell", w.cell, "sn", env.SN, "err", err)
		w.nakAll([]item{{msg: msg, env: env}})
		return
	}
	pipe := w.rdb.Pipeline()
	shadow.WriteReported(ctx, pipe, env.SN, EventShadowFields(e))
	if _, err := pipe.Exec(ctx); err != nil {
		w.m.Inc(MShadowErr)
		slog.Warn("pipeline event shadow", "sn", env.SN, "err", err)
	}
	w.m.Inc(MEvents)
	w.ack(msg)
}

func (w *Worker) handleCmdAck(ctx context.Context, msg jetstream.Msg, env *envelope.Envelope, a *CmdAckPayload) {
	if err := w.rdb.Set(ctx, "cmdres:"+a.CmdID, []byte(env.Payload), time.Hour).Err(); err != nil {
		slog.Error("pipeline cmdres set", "cmd_id", a.CmdID, "err", err)
		w.nakAll([]item{{msg: msg, env: env}})
		return
	}
	w.m.Inc(MCmdAcks)
	w.ack(msg)
}

// batchLoop 按 Size/Window 攒批；in 关闭时把尾批刷出并返回。
func (w *Worker) batchLoop(in <-chan item, batches chan<- []item) {
	b := NewBatcher[item](w.cfg.BatchSize, w.cfg.Window, time.Now)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		var timerC <-chan time.Time
		if b.Len() > 0 {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(b.TimeToFlush())
			timerC = timer.C
		}
		select {
		case it, ok := <-in:
			if !ok {
				if items := b.Drain(); items != nil {
					batches <- items
				}
				return
			}
			b.Add(it)
			if b.ShouldFlush() {
				batches <- b.Drain()
			}
		case <-timerC:
			if b.ShouldFlush() {
				batches <- b.Drain()
			}
		}
	}
}

// flush：一次 BatchInsert + 一次 Redis pipeline（每 SN 最新）+ 批量 Ack；TDengine 失败整批 Nak。
func (w *Worker) flush(batch []item) {
	if len(batch) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.FlushTimeout)
	defer cancel()
	rows := make([]tdengine.TelemetryRow, len(batch))
	// 未建模属性按 SN 在批内合并（后到覆盖先到）：属性只随上线第一帧/变更帧上报，不能因为它不是批内最新一行就丢掉。
	extras := map[string]map[string]any{}
	for i, it := range batch {
		rows[i] = it.row
		for k, v := range it.extras {
			if extras[it.row.SN] == nil {
				extras[it.row.SN] = map[string]any{}
			}
			extras[it.row.SN][k] = v
		}
	}
	if err := w.td.BatchInsert(ctx, rows); err != nil {
		slog.Error("pipeline batch insert", "cell", w.cell, "rows", len(rows), "err", err)
		w.nakAll(batch)
		return
	}
	pipe := w.rdb.Pipeline()
	for _, r := range LatestPerSN(rows) {
		shadow.WriteReported(ctx, pipe, r.SN, ShadowFieldsWithExtras(r, extras[r.SN]))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		w.m.Inc(MShadowErr)
		slog.Warn("pipeline shadow pipeline", "cell", w.cell, "err", err)
	}
	for _, it := range batch {
		w.ack(it.msg)
	}
	w.m.Inc(MBatches)
	w.m.Add(MRowsWritten, int64(len(batch)))
}

func (w *Worker) ack(msg jetstream.Msg) {
	if err := msg.Ack(); err != nil {
		w.m.Inc(MAckErr)
	}
}

// nakAll 整批 Nak，并删除已占用的 dedupe key（否则重投会被判重复而丢失）。
func (w *Worker) nakAll(batch []item) {
	ctx, cancel := w.opCtx()
	defer cancel()
	keys := make([]string, 0, len(batch))
	for _, it := range batch {
		if it.env != nil {
			keys = append(keys, dedupe.Key(it.env.SN, it.env.Seq))
		}
	}
	if len(keys) > 0 {
		if err := w.rdb.Del(ctx, keys...).Err(); err != nil {
			slog.Warn("pipeline undedupe", "err", err)
		}
	}
	for _, it := range batch {
		var err error
		if w.cfg.NakDelay > 0 {
			err = it.msg.NakWithDelay(w.cfg.NakDelay)
		} else {
			err = it.msg.Nak()
		}
		if err != nil {
			w.m.Inc(MAckErr)
		}
	}
	w.m.Add(MNak, int64(len(batch)))
}

// RunLagRefresher 每 interval 汇总各 consumer 的 NumPending 到 lag。
func RunLagRefresher(ctx context.Context, consumers []jetstream.Consumer, m *Metrics, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var lag int64
			for _, c := range consumers {
				ictx, cancel := context.WithTimeout(ctx, interval)
				info, err := c.Info(ictx)
				cancel()
				if err != nil {
					continue
				}
				lag += int64(info.NumPending)
			}
			m.Set(MLag, lag)
		}
	}
}
