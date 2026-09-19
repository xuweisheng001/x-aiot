package accessory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
)

// ConsumerName 是 IOT_UP 上的 durable 名；与 pipeline-<cell>、alarm、job 并列，互不阻塞。
const ConsumerName = "accessory"

// ConsumerConfig：事件 + 遥测两类 subject。
func ConsumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:        ConsumerName,
		FilterSubjects: []string{envelope.SubjectAll(envelope.KindEvent), envelope.SubjectAll(envelope.KindTelemetry)},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxDeliver:     -1,
		DeliverPolicy:  jetstream.DeliverNewPolicy, // 联动只对新事件有意义；历史重放只会下发过期建议
	}
}

// Actuator 是动作执行器（deviceapi HTTP；测试注入 fake）。
type Actuator interface {
	SetDesired(ctx context.Context, sn string, fields map[string]any) error
	StopHost(ctx context.Context, hostSN string) error
}

// DeviceAPI 通过 deviceapi 的 PATCH /desired 与 POST /cmd 执行动作；失败重试 3 次退避。
type DeviceAPI struct {
	Base   string
	Client *http.Client
}

func NewDeviceAPI(base string) *DeviceAPI {
	return &DeviceAPI{Base: base, Client: &http.Client{Timeout: 5 * time.Second}}
}

var retryBackoff = []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}

func (d *DeviceAPI) do(ctx context.Context, method, path string, body any) error {
	b, _ := json.Marshal(body)
	var last error
	for i := 0; i <= len(retryBackoff); i++ {
		req, err := http.NewRequestWithContext(ctx, method, d.Base+path, bytes.NewReader(b))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := d.Client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 300 {
				return nil
			}
			last = fmt.Errorf("deviceapi %s %s: status %d", method, path, resp.StatusCode)
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return last // 4xx 不重试
			}
		} else {
			last = err
		}
		if i < len(retryBackoff) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryBackoff[i]):
			}
		}
	}
	return last
}

func (d *DeviceAPI) SetDesired(ctx context.Context, sn string, fields map[string]any) error {
	return d.do(ctx, http.MethodPatch, "/api/v1/devices/"+sn+"/desired", fields)
}

func (d *DeviceAPI) StopHost(ctx context.Context, hostSN string) error {
	return d.do(ctx, http.MethodPost, "/api/v1/devices/"+hostSN+"/cmd",
		map[string]any{"action": "stop", "params": map[string]any{"reason": "FIRE_SUPPRESSED"}, "operator": "accessory-svc", "source": "system"})
}

// Redis key（技术方案 §10.5）
const (
	KeyOffZSet   = "linkage:off"
	KeyOffMeta   = "linkage:offmeta:" // + acc_sn
	KeySeen      = "linkage:seen:"    // + host:seq
	KeySafety    = "linkage:safety:"  // + acc_sn
	SeenTTL      = 10 * time.Minute
	OffRetryWait = 10 * time.Second
)

// OffMeta 是待关闭定时器的附属信息。
type OffMeta struct {
	HostSN      string    `json:"host_sn"`
	ScheduledAt time.Time `json:"scheduled_at"`
	Trigger     Trigger   `json:"trigger"`
}

// Options 是引擎可调参数。
type Options struct {
	AllowOff       bool
	StopHostOnFire bool
	DefaultLevel   int
	RulesProduct   string // linkage_rule 的 product_key（配件机型）
	Now            func() time.Time
}

// Engine 是联动规则引擎：消费 → 归一化 → Decide → 执行 → 留痕。
type Engine struct {
	Store Store
	RDB   *redis.Client
	Act   Actuator
	M     *Metrics
	Opt   Options

	mu            sync.Mutex
	prevWorkState map[string]int          // host → 上次 work_state
	lastJobStart  map[string]time.Time    // host → 最近 JOB_START
	current       map[string]DesiredState // acc → 最近下发的目标
	rules         map[string]int
	rulesAt       time.Time
}

func NewEngine(st Store, rdb *redis.Client, act Actuator, m *Metrics, opt Options) *Engine {
	if m == nil {
		m = NewMetrics()
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.DefaultLevel < 1 || opt.DefaultLevel > MaxFanLevel {
		opt.DefaultLevel = DefaultFanLevel
	}
	if opt.RulesProduct == "" {
		opt.RulesProduct = "ACC_PURIFIER"
	}
	return &Engine{Store: st, RDB: rdb, Act: act, M: m, Opt: opt,
		prevWorkState: map[string]int{}, lastJobStart: map[string]time.Time{}, current: map[string]DesiredState{}}
}

// eventPayload 是 event 载荷里引擎关心的字段。
type eventPayload struct {
	Seq        int64  `json:"seq"`
	Ts         int64  `json:"ts"`
	Code       string `json:"code"`
	MaterialID string `json:"material_id"`
	JobID      string `json:"job_id"`
}

type telemetryPayload struct {
	Seq       int64 `json:"seq"`
	Ts        int64 `json:"ts"`
	WorkState *int  `json:"work_state"`
}

// Normalize 把信封转成规则输入；返回 ok=false 表示与联动无关（非 event/telemetry、缺字段）。
func Normalize(env *envelope.Envelope) (Event, string, bool) {
	switch env.Kind {
	case envelope.KindEvent:
		var p eventPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil || p.Code == "" {
			return Event{}, "", false
		}
		ts := p.Ts
		if ts == 0 {
			ts = env.RecvTs
		}
		seq := p.Seq
		if seq == 0 {
			seq = env.Seq
		}
		return Event{SN: env.SN, Code: p.Code, MaterialID: p.MaterialID, Seq: seq, Ts: ts, RecvTs: env.RecvTs}, p.JobID, true
	case envelope.KindTelemetry:
		var p telemetryPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil || p.WorkState == nil {
			return Event{}, "", false
		}
		ts := p.Ts
		if ts == 0 {
			ts = env.RecvTs
		}
		return Event{SN: env.SN, IsTelemetry: true, WorkState: *p.WorkState, Seq: env.Seq, Ts: ts, RecvTs: env.RecvTs}, "", true
	}
	return Event{}, "", false
}

// Outcome 供日志与测试。
type Outcome string

const (
	OutcomeNoPairing Outcome = "no_pairing"
	OutcomeStale     Outcome = "stale"
	OutcomeDedup     Outcome = "dedup"
	OutcomeNoAction  Outcome = "no_action"
	OutcomeActed     Outcome = "acted"
	OutcomeIgnored   Outcome = "ignored"
)

// Handle 处理一条信封。返回 err 表示瞬时故障需 Nak（只有 alarm_context 写失败会这样，见 §7.2）。
func (e *Engine) Handle(ctx context.Context, env *envelope.Envelope) (Outcome, error) {
	e.M.Inc("consumed")
	evt, jobID, ok := Normalize(env)
	if !ok {
		e.M.Inc("payload_bad")
		return OutcomeIgnored, nil
	}
	now := e.Opt.Now()

	// 1 解析角色：配件事件（有配对）或主机事件
	var pairings []Pairing
	isAcc := false
	if !evt.IsTelemetry && envelope.AccessorySafetyCodes[evt.Code] {
		p, err := e.Store.PairingByAcc(ctx, evt.SN)
		if err == nil && p != nil {
			isAcc = true
			pairings, _ = CachedPairings(ctx, e.RDB, e.Store, p.HostSN)
		}
	}
	if !isAcc {
		var err error
		pairings, err = CachedPairings(ctx, e.RDB, e.Store, evt.SN)
		if err != nil {
			e.M.Inc("db_errors")
			slog.Warn("pairings lookup", "sn", evt.SN, "err", err)
		}
	}
	if len(pairings) == 0 {
		if evt.IsTelemetry {
			e.rememberWorkState(evt.SN, evt.WorkState)
		}
		e.M.Inc("no_pairing")
		return OutcomeNoPairing, nil
	}
	host := pairings[0].HostSN

	// 2 配件安全事件：关联主机留痕（失败 Nak 重试，INC-2-07）
	if isAcc {
		if err := e.writeContext(ctx, evt, host, jobID); err != nil {
			e.M.Inc("context_failed")
			return "", err
		}
		e.M.Inc("context_written")
	}

	// 3 过期与幂等
	if IsStale(evt.RecvTs, now, MaxEventAge) {
		e.M.Inc("stale_dropped")
		if evt.IsTelemetry {
			e.rememberWorkState(host, evt.WorkState)
		}
		return OutcomeStale, nil
	}
	if !evt.IsTelemetry && e.RDB != nil {
		ok, err := e.RDB.SetNX(ctx, KeySeen+evt.SN+":"+strconv.FormatInt(evt.Seq, 10), 1, SeenTTL).Result()
		if err == nil && !ok {
			e.M.Inc("dedup_skipped")
			return OutcomeDedup, nil
		}
	}

	// 4 决策
	st := e.snapshot(ctx, host, pairings, now)
	actions := Decide(evt, pairings, st)
	if evt.IsTelemetry {
		prev, known := e.prevWorkStateOf(host)
		e.rememberWorkState(host, evt.WorkState)
		if known && prev == evt.WorkState {
			e.M.Inc("work_state_debounced")
		}
	}
	if !evt.IsTelemetry && evt.Code == "JOB_START" {
		e.mu.Lock()
		e.lastJobStart[host] = now
		e.mu.Unlock()
	}
	if len(actions) == 0 {
		return OutcomeNoAction, nil
	}

	// 5 执行
	e.execute(ctx, evt, actions, now)
	return OutcomeActed, nil
}

func (e *Engine) prevWorkStateOf(host string) (int, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := e.prevWorkState[host]
	return v, ok
}

func (e *Engine) rememberWorkState(host string, ws int) {
	e.mu.Lock()
	e.prevWorkState[host] = ws
	e.mu.Unlock()
}

// snapshot 组装 Decide 的上下文。
func (e *Engine) snapshot(ctx context.Context, host string, pairings []Pairing, now time.Time) EngineState {
	e.mu.Lock()
	st := EngineState{
		Now: now, AllowOff: e.Opt.AllowOff, StopHostOnFire: e.Opt.StopHostOnFire, DefaultLevel: e.Opt.DefaultLevel,
		PrevWorkState: map[string]int{}, LastJobStart: map[string]time.Time{}, SafetyVentUntil: map[string]time.Time{},
		Current: map[string]DesiredState{}, HostWorkState: map[string]int{},
	}
	if v, ok := e.prevWorkState[host]; ok {
		st.PrevWorkState[host] = v
	}
	if v, ok := e.lastJobStart[host]; ok {
		st.LastJobStart[host] = v
	}
	for _, p := range pairings {
		if v, ok := e.current[p.AccSN]; ok {
			st.Current[p.AccSN] = v
		}
	}
	e.mu.Unlock()

	st.Rules = e.loadRules(ctx)
	if e.RDB != nil {
		for _, p := range pairings {
			if ttl, err := e.RDB.TTL(ctx, KeySafety+p.AccSN).Result(); err == nil && ttl > 0 {
				st.SafetyVentUntil[p.AccSN] = now.Add(ttl)
			}
		}
		if rep, err := shadow.Read(ctx, e.RDB, host); err == nil {
			if ws, err := strconv.Atoi(rep["work_state"]); err == nil {
				st.HostWorkState[host] = ws
				if _, known := st.PrevWorkState[host]; !known {
					// 进程刚启动：以影子为上一次已知状态，避免第一帧遥测被当成变化
					st.PrevWorkState[host] = ws
					e.rememberWorkState(host, ws)
				}
			}
		}
	}
	return st
}

// loadRules 每 60 s 从 PG 刷新一次材料档位映射。
func (e *Engine) loadRules(ctx context.Context) map[string]int {
	e.mu.Lock()
	if e.rules != nil && time.Since(e.rulesAt) < time.Minute {
		r := e.rules
		e.mu.Unlock()
		return r
	}
	e.mu.Unlock()
	r, err := e.Store.Rules(ctx, e.Opt.RulesProduct)
	if err != nil {
		e.M.Inc("db_errors")
		return map[string]int{}
	}
	e.mu.Lock()
	e.rules, e.rulesAt = r, time.Now()
	e.mu.Unlock()
	return r
}

func (e *Engine) writeContext(ctx context.Context, evt Event, host, jobID string) error {
	c := AlarmContext{AccSN: evt.SN, Code: evt.Code, EventTs: time.UnixMilli(evt.Ts).UTC(), HostSN: host, JobID: jobID}
	if e.RDB != nil {
		if rep, err := shadow.Read(ctx, e.RDB, host); err == nil {
			if ws, err := strconv.Atoi(rep["work_state"]); err == nil {
				c.HostWorkState = &ws
			}
			if jobID == "" {
				c.JobID = rep["job_id"]
			}
		}
	}
	return e.Store.InsertAlarmContext(ctx, c)
}

// execute 逐条执行动作并留痕；下发失败不 Nak（本地兜底已在），只记 failed。
func (e *Engine) execute(ctx context.Context, evt Event, actions []Action, now time.Time) {
	for _, a := range actions {
		e.Apply(ctx, a, evt.Ts, now)
	}
}

// Apply 执行单个动作（定时器与对账也复用）。
func (e *Engine) Apply(ctx context.Context, a Action, eventTs int64, now time.Time) {
	var err error
	result := "ok"
	switch a.Type {
	case ActSetDesired:
		e.M.Inc("linkage_triggered")
		err = e.Act.SetDesired(ctx, a.AccSN, map[string]any{"power_on": a.Desired.PowerOn, "fan_level": a.Desired.FanLevel})
		if err == nil {
			e.M.Inc("linkage_succeeded")
			e.mu.Lock()
			e.current[a.AccSN] = a.Desired
			e.mu.Unlock()
			if eventTs > 0 {
				e.M.ObserveLatency(now.UnixMilli() - eventTs)
			}
		} else {
			e.M.Inc("linkage_failed")
		}
	case ActScheduleOff:
		err = e.ScheduleOff(ctx, a.AccSN, OffMeta{HostSN: a.HostSN, ScheduledAt: now, Trigger: a.Trigger}, now.Add(a.Delay))
		if err == nil {
			e.M.Inc("off_scheduled")
		}
	case ActCancelOff:
		var removed bool
		removed, err = e.CancelOff(ctx, a.AccSN)
		if removed {
			e.M.Inc("off_cancelled")
		}
		if err == nil && !removed {
			return // 没有待关闭定时器：不留痕
		}
	case ActSafetyVent:
		if e.RDB != nil {
			err = e.RDB.Set(ctx, KeySafety+a.AccSN, 1, a.Delay).Err()
		}
		if err == nil {
			e.M.Inc("safety_vent")
		}
	case ActStopHost:
		err = e.Act.StopHost(ctx, a.HostSN)
		if err == nil {
			e.M.Inc("host_stop_sent")
		} else {
			e.M.Inc("host_stop_failed")
		}
	}
	if err != nil {
		result = "failed"
		slog.Warn("linkage action failed", "type", a.Type, "acc", a.AccSN, "host", a.HostSN, "err", err)
	}
	body, _ := json.Marshal(a)
	var lat *int64
	if eventTs > 0 {
		l := now.UnixMilli() - eventTs
		lat = &l
	}
	if aerr := e.Store.InsertAudit(ctx, LinkageAudit{HostSN: a.HostSN, AccSN: a.AccSN, Trigger: string(a.Trigger), Action: body, Result: result, LatencyMs: lat, CreatedAt: now}); aerr != nil {
		e.M.Inc("db_errors")
		slog.Warn("linkage audit", "err", aerr)
	}
}

// ---- 待关闭定时器：ZSET linkage:off（score=到期 Unix 秒）+ meta ----

func (e *Engine) ScheduleOff(ctx context.Context, acc string, meta OffMeta, due time.Time) error {
	if e.RDB == nil {
		return errors.New("redis required for off timer")
	}
	b, _ := json.Marshal(meta)
	pipe := e.RDB.TxPipeline()
	pipe.ZAdd(ctx, KeyOffZSet, redis.Z{Score: float64(due.Unix()), Member: acc})
	pipe.Set(ctx, KeyOffMeta+acc, b, MaxOffDelay+SafetyVentWindow+time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

// CancelOff 返回是否确有定时器被取消。
func (e *Engine) CancelOff(ctx context.Context, acc string) (bool, error) {
	if e.RDB == nil {
		return false, nil
	}
	n, err := e.RDB.ZRem(ctx, KeyOffZSet, acc).Result()
	if err != nil {
		return false, err
	}
	_ = e.RDB.Del(ctx, KeyOffMeta+acc).Err()
	return n > 0, nil
}

// HasOffTimer 供对账。
func (e *Engine) HasOffTimer(ctx context.Context, acc string) bool {
	if e.RDB == nil {
		return false
	}
	_, err := e.RDB.ZScore(ctx, KeyOffZSet, acc).Result()
	return err == nil
}

// PopDue 两步删除：ZRANGEBYSCORE 取到期项，逐个 ZREM 成功才算抢到（多副本安全）。
func (e *Engine) PopDue(ctx context.Context, now time.Time) []string {
	if e.RDB == nil {
		return nil
	}
	members, err := e.RDB.ZRangeByScore(ctx, KeyOffZSet, &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.Unix(), 10), Count: 100}).Result()
	if err != nil {
		return nil
	}
	var owned []string
	for _, m := range members {
		if n, err := e.RDB.ZRem(ctx, KeyOffZSet, m).Result(); err == nil && n > 0 {
			owned = append(owned, m)
		}
	}
	return owned
}

// FireOff 处理一个到期的关闭建议：AllowOff 关闭 → 抑制；期间有新作业开始 → 取消；否则下发 power_on:false。
// 下发失败 → 重新排定 10 s 后（不丢）。
func (e *Engine) FireOff(ctx context.Context, acc string, now time.Time) {
	var meta OffMeta
	if b, err := e.RDB.Get(ctx, KeyOffMeta+acc).Bytes(); err == nil {
		_ = json.Unmarshal(b, &meta)
	}
	_ = e.RDB.Del(ctx, KeyOffMeta+acc).Err()
	if !e.Opt.AllowOff {
		e.M.Inc("off_suppressed")
		return
	}
	e.mu.Lock()
	ls := e.lastJobStart[meta.HostSN]
	e.mu.Unlock()
	if ShouldCancelOff(ls, meta.ScheduledAt) {
		e.M.Inc("off_cancelled")
		return
	}
	if ttl, err := e.RDB.TTL(ctx, KeySafety+acc).Result(); err == nil && ttl > 0 {
		// 安全排烟窗口仍在：推到窗口结束
		_ = e.ScheduleOff(ctx, acc, meta, now.Add(ttl))
		e.M.Inc("off_requeued")
		return
	}
	want := DesiredState{PowerOn: false, FanLevel: 0}
	if err := e.Act.SetDesired(ctx, acc, map[string]any{"power_on": false, "fan_level": 0}); err != nil {
		e.M.Inc("linkage_failed")
		e.M.Inc("off_requeued")
		_ = e.ScheduleOff(ctx, acc, meta, now.Add(OffRetryWait))
		return
	}
	e.M.Inc("off_sent")
	e.mu.Lock()
	e.current[acc] = want
	e.mu.Unlock()
	body, _ := json.Marshal(Action{Type: ActSetDesired, AccSN: acc, HostSN: meta.HostSN, Desired: want, Trigger: TrigOffTimer})
	if err := e.Store.InsertAudit(ctx, LinkageAudit{HostSN: meta.HostSN, AccSN: acc, Trigger: string(TrigOffTimer), Action: body, Result: "ok", CreatedAt: now}); err != nil {
		e.M.Inc("db_errors")
	}
}

// RunOffTimer 每秒扫描到期项。
func (e *Engine) RunOffTimer(ctx context.Context, tick time.Duration) {
	if tick <= 0 {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := e.Opt.Now()
			for _, acc := range e.PopDue(ctx, now) {
				e.FireOff(ctx, acc, now)
			}
		}
	}
}

// ---- 对账兜底（INC-2-03 / INC-2-04）----

// ReconcileOnce 遍历全部有效配对，对「作业中关着」与「空闲长开」两种状态纠偏。
func (e *Engine) ReconcileOnce(ctx context.Context) (fixed int, err error) {
	e.M.Inc("reconcile_runs")
	pairings, err := e.Store.AllActivePairings(ctx)
	if err != nil {
		e.M.Inc("db_errors")
		return 0, err
	}
	now := e.Opt.Now()
	byHost := map[string][]Pairing{}
	for _, p := range pairings {
		byHost[p.HostSN] = append(byHost[p.HostSN], p)
	}
	for host, ps := range byHost {
		st := e.snapshot(ctx, host, ps, now)
		hv := HostView{WorkState: hostState(st, host)}
		for _, p := range ps {
			av := e.accView(ctx, p.AccSN)
			actions, findings := ReconcileDecide(p, hv, av, e.HasOffTimer(ctx, p.AccSN), st)
			for _, f := range findings {
				e.M.Inc("reconcile_" + f)
			}
			for _, a := range actions {
				e.Apply(ctx, a, 0, now)
				fixed++
			}
		}
	}
	return fixed, nil
}

func (e *Engine) accView(ctx context.Context, acc string) AccView {
	if e.RDB == nil {
		return AccView{}
	}
	rep, err := shadow.Read(ctx, e.RDB, acc)
	if err != nil || len(rep) == 0 {
		return AccView{}
	}
	v, ok := rep["power_on"]
	if !ok {
		return AccView{}
	}
	on := v == "1" || v == "true"
	lv, _ := strconv.Atoi(rep["fan_level"])
	return AccView{Known: true, PowerOn: on, FanLevel: lv, TriggerSource: rep["trigger_source"]}
}

// RunReconciler 按 interval 常驻。
func (e *Engine) RunReconciler(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := e.ReconcileOnce(ctx); err != nil {
				slog.Warn("reconcile", "err", err)
			} else if n > 0 {
				slog.Info("reconcile fixed", "actions", n)
			}
		}
	}
}

// RunConsumer 阻塞消费直到 ctx 取消。
func RunConsumer(ctx context.Context, js jetstream.JetStream, e *Engine) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, envelope.StreamUp, ConsumerConfig())
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		env, err := envelope.Unmarshal(msg.Data())
		if err != nil {
			e.M.Inc("envelope_bad")
			_ = msg.Ack()
			return
		}
		if _, err := e.Handle(ctx, env); err != nil {
			if ctx.Err() == nil {
				slog.Error("accessory event failed, nak", "sn", env.SN, "err", err)
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
