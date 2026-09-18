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
	rnd       *rand.Rand
	rmu       sync.Mutex
	pubMu     sync.Mutex // 设备只有一条上行链路：seq 分配与发送在同一临界区，保证到达顺序 == seq 顺序

	// 测试钩子
	otaStep time.Duration
}

func NewDevice(cfg *Config, idx int, stats *Stats) *Device {
	sn := SNFor(cfg.SNPrefix, idx)
	return &Device{cfg: cfg, SN: sn, Bad: IsBadFirmware(idx-1, cfg.N, cfg.BadFirmware), stats: stats,
		log:     slog.With("sn", sn),
		rnd:     rand.New(rand.NewPCG(uint64(idx), 0xC0FFEE)),
		otaStep: 300 * time.Millisecond}
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
	var wg sync.WaitGroup
	sctx, cancel := context.WithCancel(ctx)
	defer func() { cancel(); wg.Wait() }()

	handler := func(m mqtt.Message) {
		wg.Add(1)
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
	// 上线先报一帧
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
	tick, jit := d.tick.Add(1), d.jitter()
	d.publish(cli, "telemetry", func(seq int64) any { return BuildTelemetry(seq, now(), tick, jit) })
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
