package simulator

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtool/xtool-aiot/internal/simulator/mqtt"
)

// Stats 是全体设备的聚合计数。
type Stats struct {
	Connected  atomic.Int64
	Reconnects atomic.Int64
	Published  atomic.Int64
	Acks       atomic.Int64
	Events     atomic.Int64
	PubErrors  atomic.Int64
}

// Device 是一台虚拟设备。
type Device struct {
	cfg   *Config
	SN    string
	Bad   bool
	stats *Stats
	log   *slog.Logger

	seq       atomic.Int64
	tick      atomic.Int64
	eventSent atomic.Bool
	start     time.Time

	// v1.1：opt-in 状态、属性是否需要随下一帧遥测回报、当前任务
	optin      atomic.Bool
	attrsDirty atomic.Bool
	jobMu      sync.Mutex
	prevState  int
	jobIdx     int64
	curJob     *JobFields
	curJobTs   int64
	rnd        *rand.Rand
	rmu        sync.Mutex
	pubMu      sync.Mutex // 设备只有一条上行链路：seq 分配与发送在同一临界区，保证到达顺序 == seq 顺序

	// BL2 -accessory：净化器运行态
	purMu sync.Mutex
	pur   PurifierState

	// 测试钩子
	otaStep time.Duration
}

func NewDevice(cfg *Config, idx int, stats *Stats) *Device {
	sn := SNFor(cfg.SNPrefix, idx)
	d := &Device{cfg: cfg, SN: sn, Bad: IsBadFirmware(idx-1, cfg.N, cfg.BadFirmware), stats: stats,
		log:     slog.With("sn", sn),
		rnd:     rand.New(rand.NewPCG(uint64(idx), 0xC0FFEE)),
		otaStep: 300 * time.Millisecond}
	d.seq.Store(SeqBase(cfg.SeqBase, time.Now()))
	d.optin.Store(cfg.JobOptin)
	d.pur = PurifierState{FanLevel: 2}
	return d
}

// Purifier 返回当前净化器状态（测试用）。
func (d *Device) Purifier() PurifierState {
	d.purMu.Lock()
	defer d.purMu.Unlock()
	return d.pur
}

// Optin 返回当前 job_feedback_optin（测试用）。
func (d *Device) Optin() bool { return d.optin.Load() }

// SeqBase 纯函数：base>0 原样；base==0 按 now 推导 UnixMilli×1000（跨重启单调）；base<0 → 0。
func SeqBase(base int64, now time.Time) int64 {
	switch {
	case base > 0:
		return base
	case base == 0:
		return now.UnixMilli() * 1000
	default:
		return 0
	}
}

func (d *Device) NextSeq() int64 { return d.seq.Add(1) }

func (d *Device) jitter() float64 {
	d.rmu.Lock()
	defer d.rmu.Unlock()
	return d.rnd.Float64()
}

// Run 是设备主循环：resolve → connect → session；断开后按重连纪律等待再来。
func (d *Device) Run(ctx context.Context) {
	d.start = time.Now()
	fallback, _ := HostPort(d.cfg.MQTTURL)
	attempt := 0
	for ctx.Err() == nil {
		addr, viaBootstrap := ResolveBroker(ctx, d.cfg, d.SN, fallback)
		if ctx.Err() != nil {
			return
		}
		cli, err := mqtt.Dial(ctx, mqtt.Options{
			Addr: addr, ClientID: d.SN, Username: d.SN, CleanSession: false, KeepAlive: 60 * time.Second,
			OnPubAck: func() { d.stats.Acks.Add(1) },
		})
		if err != nil {
			d.log.Warn("connect failed", "addr", addr, "bootstrap", viaBootstrap, "attempt", attempt, "err", err)
		} else {
			d.stats.Connected.Add(1)
			if attempt > 0 {
				d.stats.Reconnects.Add(1)
			}
			d.log.Info("connected", "addr", addr, "bootstrap", viaBootstrap, "bad_fw", d.Bad)
			serr := d.session(ctx, cli)
			d.stats.Connected.Add(-1)
			if ctx.Err() != nil {
				cli.Disconnect()
				return
			}
			d.log.Warn("disconnected", "err", serr)
		}
		attempt++
		delay := ReconnectDelay(d.Bad, attempt-1)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// session 在一条连接上跑：订阅下行、遥测/心跳定时、事件一次；连接结束或 ctx 取消返回。
func (d *Device) session(ctx context.Context, cli *mqtt.Client) error {
	// 下行回调在 mqtt 读协程里触发。wg.Add 必须与 closing 判定在同一临界区，
	// 否则会与退出时的 wg.Wait 竞争（Add 晚于 Wait 是未定义行为）。
	var wg sync.WaitGroup
	var hmu sync.Mutex
	closing := false
	sctx, cancel := context.WithCancel(ctx)
	defer func() {
		hmu.Lock()
		closing = true
		hmu.Unlock()
		cli.SetOnMessage(nil)
		cancel()
		wg.Wait()
	}()

	handler := func(m mqtt.Message) {
		hmu.Lock()
		if closing {
			hmu.Unlock()
			return
		}
		wg.Add(1)
		hmu.Unlock()
		go func() {
			defer wg.Done()
			d.handleDown(sctx, cli, m)
		}()
	}
	cli.SetOnMessage(handler)
	if err := cli.Subscribe(ctx, TopicDown(d.SN), 1); err != nil {
		cli.Disconnect()
		return err
	}
	// 上线先报一帧，并携带 v1.1 属性
	d.attrsDirty.Store(true)
	d.publishTelemetry(cli)

	work := time.NewTicker(d.cfg.Work)
	defer work.Stop()
	hb := time.NewTicker(d.cfg.HB)
	defer hb.Stop()
	var eventC <-chan time.Time
	if d.cfg.Event.Enabled() && !d.eventSent.Load() {
		remain := d.cfg.Event.At - time.Since(d.start)
		if remain < 0 {
			remain = 0
		}
		t := time.NewTimer(remain)
		defer t.Stop()
		eventC = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cli.Done():
			return cli.Err()
		case <-work.C:
			d.publishTelemetry(cli)
		case <-hb.C:
			d.publish(cli, "event", func(seq int64) any {
				return Event{Seq: seq, Ts: now(), Code: "HEARTBEAT", Msg: "uptime=" + time.Since(d.start).Truncate(time.Second).String()}
			})
		case <-eventC:
			eventC = nil
			if d.eventSent.CompareAndSwap(false, true) {
				d.publish(cli, "event", func(seq int64) any {
					return Event{Seq: seq, Ts: now(), Code: d.cfg.Event.Code, Msg: "simulated " + d.cfg.Event.Code}
				})
				d.stats.Events.Add(1)
				d.log.Info("event published", "code", d.cfg.Event.Code)
			}
		}
	}
}

func (d *Device) publishTelemetry(cli *mqtt.Client) {
	if d.cfg.Accessory {
		d.publishPurifier(cli)
		return
	}
	tick, jit := d.tick.Add(1), d.jitter()
	withAttrs := d.attrsDirty.CompareAndSwap(true, false)
	optin := d.optin.Load()
	ts := now()
	d.publish(cli, "telemetry", func(seq int64) any {
		t := BuildTelemetry(seq, ts, tick, jit)
		if withAttrs {
			t.ModuleModel = d.cfg.Module
			o := optin
			t.JobFeedbackOptin = &o
		}
		return t
	})
	if d.cfg.Jobs {
		d.emitJobEvents(cli, tick, ts, optin)
	}
}

// publishPurifier 发一帧净化器遥测（-accessory 模式）；每帧推进累计运行时长。
func (d *Device) publishPurifier(cli *mqtt.Client) {
	jit := d.jitter()
	d.purMu.Lock()
	if d.tick.Add(1) > 1 {
		d.pur = d.pur.Advance(d.cfg.Work)
	}
	st := d.pur
	d.purMu.Unlock()
	ts := now()
	d.publish(cli, "telemetry", func(seq int64) any { return BuildPurifierTelemetry(seq, ts, st, jit) })
}

// emitJobEvents 按 WorkCycle 的状态边界发 JOB_START / JOB_DONE；四个任务字段只在 opt-in 时出现。
func (d *Device) emitJobEvents(cli *mqtt.Client, tick, ts int64, optin bool) {
	state, _ := WorkCycle(tick)
	d.jobMu.Lock()
	start, done := JobTransition(d.prevState, state)
	d.prevState = state
	var startFields, doneFields *JobFields
	var dur int64
	if start {
		d.jobIdx++
		f := BuildJobFields(optin, NewJobID(), MaterialFor(d.jobIdx), DefaultParamProfileID, DefaultJobParams)
		d.curJob, d.curJobTs = &f, ts
		startFields = &f
	}
	if done && d.curJob != nil {
		f := *d.curJob
		doneFields = &f
		dur = (ts - d.curJobTs) / 1000
		d.curJob = nil
	}
	d.jobMu.Unlock()
	if startFields != nil {
		d.publish(cli, "event", func(seq int64) any {
			return JobEvent{Event: Event{Seq: seq, Ts: ts, Code: "JOB_START", Msg: "simulated job"}, JobFields: *startFields}
		})
	}
	if doneFields != nil {
		d.publish(cli, "event", func(seq int64) any {
			return JobEvent{Event: Event{Seq: seq, Ts: ts, Code: "JOB_DONE", Msg: "simulated job"}, JobFields: *doneFields, DurationS: dur}
		})
	}
}

// publish 在同一临界区内分配 seq 并发送，模拟真实设备单上行链路的顺序语义。
func (d *Device) publish(cli *mqtt.Client, kind string, build func(seq int64) any) {
	d.pubMu.Lock()
	defer d.pubMu.Unlock()
	v := build(d.NextSeq())
	b, _ := json.Marshal(v)
	if err := cli.Publish(TopicUp(d.cfg.PK, d.SN, kind), 1, b); err != nil {
		d.stats.PubErrors.Add(1)
		return
	}
	d.stats.Published.Add(1)
}

// handleDown 处理 down/{sn}/cmd 与 down/{sn}/desired。
func (d *Device) handleDown(ctx context.Context, cli *mqtt.Client, m mqtt.Message) {
	switch DownKind(m.Topic, d.SN) {
	case "cmd":
		var c Cmd
		if err := json.Unmarshal(m.Payload, &c); err != nil || c.CmdID == "" {
			d.log.Warn("bad cmd payload", "payload", string(m.Payload))
			return
		}
		result, detail := AckFor(c.Action)
		d.publish(cli, "cmd_ack", func(seq int64) any {
			return CmdAck{Seq: seq, Ts: now(), CmdID: c.CmdID, Result: result, Detail: detail}
		})
		d.log.Info("cmd acked", "cmd_id", c.CmdID, "action", c.Action, "result", result)
		if c.Action == "ota" {
			d.runOTA(ctx, cli, BatchIDFrom(c.Params))
		}
	case "desired":
		var ds Desired
		if err := json.Unmarshal(m.Payload, &ds); err != nil {
			d.log.Warn("bad desired payload", "payload", string(m.Payload))
			return
		}
		d.log.Info("desired received", "version", ds.Version, "desired", ds.Desired)
		if d.cfg.Accessory {
			d.purMu.Lock()
			next, changed := ApplyPurifierDesired(d.pur, ds.Desired)
			d.pur = next
			d.purMu.Unlock()
			if changed {
				d.log.Info("purifier desired applied", "power_on", next.PowerOn, "fan_level", next.FanLevel)
				d.publishPurifier(cli) // 立即回报，联动延迟可观测
			}
		}
		if v, ok := OptinFromDesired(ds.Desired); ok {
			d.optin.Store(v)
			d.attrsDirty.Store(true) // 下一帧遥测回报新值
			d.log.Info("job_feedback_optin applied", "value", v)
		}
	default:
		d.log.Debug("ignored downlink", "topic", m.Topic)
	}
}

func (d *Device) runOTA(ctx context.Context, cli *mqtt.Client, batchID int64) {
	fail := d.cfg.OTAFailRate > 0 && d.jitter() < d.cfg.OTAFailRate
	for i, ph := range OTAPhases(fail) {
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-cli.Done():
				return
			case <-time.After(d.otaStep):
			}
		}
		d.publish(cli, "ota_progress", func(seq int64) any {
			return OTAProgress{Seq: seq, Ts: now(), BatchID: batchID, Phase: ph.Phase, Pct: ph.Pct, ErrorCode: ph.ErrorCode}
		})
	}
	d.log.Info("ota finished", "batch_id", batchID, "failed", fail)
}

func now() int64 { return time.Now().UnixMilli() }
