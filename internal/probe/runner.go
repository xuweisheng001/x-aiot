package probe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// Emitter 把一条合成安全事件送进链路。两种实现对应两种覆盖范围：
//   - MQTTEmitter：走 conn-gate → EMQX → bridge → JetStream，覆盖最全（默认）
//   - JetStreamEmitter：直接投 JetStream，绕开接入面，用于在接入面已知故障时定位后半段
type Emitter interface {
	Emit(ctx context.Context, sn, pk, code string, seq int64, ts time.Time) error
	Close()
	Name() string
}

// Options 是探针配置。
type Options struct {
	SN       string        // 探针设备 SN，建议以 PROBE 开头便于运营识别
	PK       string        // product_key
	Code     string        // 安全事件码，默认 FLAME_DETECTED
	SLO      time.Duration // 端到端上限，默认 3 s
	Interval time.Duration // 探测间隔，必须大于 squelch 窗口
	Timeout  time.Duration // 单轮等待告警的上限，默认 max(2×SLO, 10 s)
	Poll     time.Duration // 轮询 PG 的间隔，默认 100 ms
	// NoCleanup 关掉「探针关闭自己造的告警」。用反向开关是因为零值必须是安全默认：
	// 正向的 Cleanup bool 在部分构造 Options{SN: x} 时会静默变成 false，
	// 于是每 10 分钟往运营的告警列表里堆一条合成火警——探针反而制造了告警疲劳。
	NoCleanup bool
}

// DefaultOptions 与 spec §6 缺省值一致。
func DefaultOptions() Options {
	return Options{SN: "PROBE00001", PK: "LM_S1", Code: "FLAME_DETECTED",
		SLO: DefaultSLO, Interval: 10 * time.Minute, Poll: 100 * time.Millisecond}
}

func (o *Options) normalise() {
	d := DefaultOptions()
	if o.SN == "" {
		o.SN = d.SN
	}
	if o.PK == "" {
		o.PK = d.PK
	}
	if o.Code == "" {
		o.Code = d.Code
	}
	if o.SLO <= 0 {
		o.SLO = d.SLO
	}
	if o.Interval <= 0 {
		o.Interval = d.Interval
	}
	if o.Poll <= 0 {
		o.Poll = d.Poll
	}
	if o.Timeout <= 0 {
		o.Timeout = 2 * o.SLO
		if o.Timeout < 10*time.Second {
			o.Timeout = 10 * time.Second
		}
	}
}

// Prober 跑探测循环。
type Prober struct {
	DB *pgxpool.Pool
	// RDB 只用来清探针自己那一个 squelch 键（见 clearSquelch）。为 nil 时退化为
	// 「靠间隔大于 squelch 窗口」，此时重启或按需触发若落在窗口内会误报 missing。
	RDB *redis.Client
	Em  Emitter
	M   *Metrics
	Opt Options
	Now func() time.Time

	mu  sync.Mutex
	seq int64
}

func New(db *pgxpool.Pool, rdb *redis.Client, em Emitter, m *Metrics, opt Options) *Prober {
	opt.normalise()
	if m == nil {
		m = NewMetrics()
	}
	return &Prober{DB: db, RDB: rdb, Em: em, M: m, Opt: opt, Now: time.Now, seq: time.Now().UnixMilli() * 1000}
}

func (p *Prober) nextSeq() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seq++
	return p.seq
}

// RunOnce 打一发合成事件并等待告警落库且已推送，返回判定。
// 探针自身出错（发不出去、查不了库）返回 StatusError：这说明探针坏了，不是链路坏了，
// 两者必须分开，否则探针故障会把全员叫起来查一个不存在的告警事故。
func (p *Prober) RunOnce(ctx context.Context) (Result, error) {
	p.M.Inc(MRuns)
	ts := p.Now().UTC().Truncate(time.Millisecond)
	res := Result{SN: p.Opt.SN, Code: p.Opt.Code, EventTs: ts}

	// 记下发射前库里该 SN 该 code 的最大 alarm id，只认这之后新建的那条，
	// 避免把上一轮或真实告警当成本轮结果。
	baseID, err := p.maxAlarmID(ctx)
	if err != nil {
		p.M.Inc(MErrors)
		res.Status, res.Reason = StatusError, "read baseline alarm id: "+err.Error()
		return res, err
	}

	// 清掉上一发留下的聚合窗口，否则重启或按需触发落在 5 分钟内时事件会被 alarm-svc
	// 正常聚合掉，探针却报「漏告警」——误报比漏报更快让人不再相信探针。
	// 只删探针自己这一个 SN+code 的键，不碰任何真实设备的聚合状态。
	p.clearSquelch(ctx)

	if err := p.Em.Emit(ctx, p.Opt.SN, p.Opt.PK, p.Opt.Code, p.nextSeq(), ts); err != nil {
		p.M.Inc(MErrors)
		res.Status, res.Reason = StatusError, "emit: "+err.Error()
		return res, err
	}

	id, notifiedAt, found, err := p.waitNotified(ctx, baseID)
	if err != nil {
		p.M.Inc(MErrors)
		res.Status, res.Reason = StatusError, "wait alarm: "+err.Error()
		return res, err
	}
	res.AlarmID = id
	if found {
		res.NotifiedA = &notifiedAt
		res.Latency = notifiedAt.Sub(ts)
	}
	res.Status, res.Reason = Verdict(found, res.Latency, p.Opt.SLO)

	if found && !p.Opt.NoCleanup {
		// 清理失败不改判定：告警确实到了，只是探针没打扫干净
		if err := p.cleanup(ctx, id); err != nil {
			p.M.Inc(MCleanupErr)
			slog.Warn("probe cleanup failed; synthetic alarm left open", "alarm_id", id, "err", err)
		} else {
			res.CleanedUp = true
		}
	}

	switch res.Status {
	case StatusOK:
		p.M.Inc(MOK)
		p.M.Observe(res.Latency)
		slog.Info("probe ok", "sn", res.SN, "alarm_id", id, "latency_ms", res.Latency.Milliseconds(), "path", p.Em.Name())
	case StatusSlow:
		p.M.Inc(MSlow)
		p.M.Observe(res.Latency)
		slog.Warn("probe slow: safety alarm exceeded SLO", "sn", res.SN, "alarm_id", id,
			"latency_ms", res.Latency.Milliseconds(), "slo_ms", p.Opt.SLO.Milliseconds(), "path", p.Em.Name())
	case StatusMissing:
		p.M.Inc(MMissing)
		// S1：安全告警链路没把合成火焰事件送到
		slog.Error("PROBE MISSING: synthetic safety alarm never notified (S1)",
			"sn", res.SN, "code", res.Code, "waited", p.Opt.Timeout.String(), "path", p.Em.Name())
	}
	return res, nil
}

// Run 按 Interval 循环，直到 ctx 取消。
func (p *Prober) Run(ctx context.Context) {
	if ok, why := IntervalSane(p.Opt.Interval, SquelchWindow); !ok {
		slog.Warn("probe interval unsafe", "reason", why)
	}
	t := time.NewTicker(p.Opt.Interval)
	defer t.Stop()
	for {
		if _, err := p.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("probe run failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// SquelchKey 与 alarm.SquelchKey 同构；这里复制一份而不是 import internal/alarm，
// 理由同 SquelchWindow：探针应当像外部观察者一样只通过链路、库与约定的 key 观察系统。
func SquelchKey(sn, code string) string { return "alarm:squelch:" + sn + ":" + code }

// clearSquelch 删除探针自己的聚合键；失败只计数不影响本轮（大不了这一发被聚合，
// 下一发在窗口外仍会成功，不会造成持续误报）。
func (p *Prober) clearSquelch(ctx context.Context) {
	if p.RDB == nil {
		return
	}
	if err := p.RDB.Del(ctx, SquelchKey(p.Opt.SN, p.Opt.Code)).Err(); err != nil {
		p.M.Inc(MSquelchErr)
		slog.Warn("probe could not clear its squelch key; this round may be aggregated away",
			"sn", p.Opt.SN, "err", err)
		return
	}
	p.M.Inc(MSquelchCleared)
}

// SquelchWindow 与 alarm.SquelchTTL 同值；这里复制一份常量而不是 import internal/alarm，
// 避免探针依赖被探测方（探针应当像外部观察者一样只通过链路与库观察系统）。
const SquelchWindow = 5 * time.Minute

func (p *Prober) maxAlarmID(ctx context.Context) (int64, error) {
	var id *int64
	err := p.DB.QueryRow(ctx,
		`SELECT max(id) FROM iot_shard.alarm WHERE sn=$1 AND code=$2`, p.Opt.SN, p.Opt.Code).Scan(&id)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	if id == nil {
		return 0, nil
	}
	return *id, nil
}

// waitNotified 轮询到「新建且已 notified」的告警为止；超时返回 found=false。
func (p *Prober) waitNotified(ctx context.Context, baseID int64) (int64, time.Time, bool, error) {
	deadline := p.Now().Add(p.Opt.Timeout)
	t := time.NewTicker(p.Opt.Poll)
	defer t.Stop()
	var lastID int64
	for {
		var id int64
		var at *time.Time
		err := p.DB.QueryRow(ctx,
			`SELECT id, notified_at FROM iot_shard.alarm
			  WHERE sn=$1 AND code=$2 AND id>$3 ORDER BY id DESC LIMIT 1`,
			p.Opt.SN, p.Opt.Code, baseID).Scan(&id, &at)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// 还没落库
		case err != nil:
			return lastID, time.Time{}, false, err
		default:
			lastID = id
			if at != nil {
				return id, at.UTC(), true, nil
			}
		}
		if !p.Now().Before(deadline) {
			return lastID, time.Time{}, false, nil
		}
		select {
		case <-ctx.Done():
			return lastID, time.Time{}, false, ctx.Err()
		case <-t.C:
		}
	}
}

// cleanup 把探针自己造的告警走完状态机（notified → acked → closed），
// 让运营的告警列表里不残留合成告警。
func (p *Prober) cleanup(ctx context.Context, id int64) error {
	if _, err := p.DB.Exec(ctx,
		`UPDATE iot_shard.alarm SET status='acked', acked_at=now()
		  WHERE id=$1 AND status IN ('open','notified')`, id); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	if _, err := p.DB.Exec(ctx,
		`UPDATE iot_shard.alarm SET status='closed', closed_by='probe'
		  WHERE id=$1 AND status='acked'`, id); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	p.M.Inc(MCleaned)
	return nil
}

// eventPayload 是探针发出的 event 载荷，与 spec §4 的设备上行同形。
func eventPayload(code string, seq int64, ts time.Time) ([]byte, error) {
	return json.Marshal(map[string]any{
		"seq": seq, "ts": ts.UnixMilli(), "code": code, "msg": "synthetic probe",
	})
}

// envelopeFor 构造与 bridge 同形的信封（JetStreamEmitter 用）。
func envelopeFor(pk, sn, code string, seq int64, ts time.Time) ([]byte, error) {
	p, err := eventPayload(code, seq, ts)
	if err != nil {
		return nil, err
	}
	e := envelope.Envelope{PK: pk, SN: sn, Kind: envelope.KindEvent, Seq: seq,
		RecvTs: ts.UnixMilli(), Payload: p}
	return e.Marshal(), nil
}
