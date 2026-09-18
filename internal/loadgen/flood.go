package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/cellmap"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// FloodOpts 是泄洪剧本参数。
type FloodOpts struct {
	Pre      int           // 预灌条数
	Cells    int           // IOT_CELLS
	SNs      int           // 合成 SN 数，默认 100
	PK       string        // 默认 LM_S1
	SNPrefix string        // 默认 FLOOD
	Timeout  time.Duration // 排空超时，默认 10min
	Poll     time.Duration // 轮询间隔，默认 500ms
	Chunk    int           // 异步发布分块，默认 2000
}

func (o *FloodOpts) defaults() {
	if o.SNs <= 0 {
		o.SNs = 100
	}
	if o.PK == "" {
		o.PK = "LM_S1"
	}
	if o.SNPrefix == "" {
		o.SNPrefix = "FLOOD"
	}
	if o.Timeout <= 0 {
		o.Timeout = 10 * time.Minute
	}
	if o.Poll <= 0 {
		o.Poll = 500 * time.Millisecond
	}
	if o.Chunk <= 0 {
		o.Chunk = 2000
	}
	if o.Cells <= 0 {
		o.Cells = 1
	}
}

// FloodMsg 是一条待灌入的信封及其 subject。
type FloodMsg struct {
	Subject string
	Env     envelope.Envelope
}

type floodTelemetry struct {
	Seq        int64   `json:"seq"`
	Ts         int64   `json:"ts"`
	WorkState  int     `json:"work_state"`
	PowerLevel int     `json:"power_level"`
	TempCavity float64 `json:"temp_cavity"`
	TempWater  float64 `json:"temp_water"`
	FanRPM     int     `json:"fan_rpm"`
	LaserHours float64 `json:"laser_hours"`
	Progress   int     `json:"progress"`
}

// BuildFloodEnvelopes 生成 pre 条合成遥测信封：sns 个 SN 轮转，每 SN seq 单调，ts 以 baseTs 起每条 +1ms，
// subject = envelope.Subject(telemetry, cellmap.CellOf(sn, cells))。纯函数。
func BuildFloodEnvelopes(pre, cells, sns int, pk, prefix string, baseTs int64) []FloodMsg {
	if sns <= 0 {
		sns = 1
	}
	out := make([]FloodMsg, 0, pre)
	seqs := make([]int64, sns)
	for i := 0; i < pre; i++ {
		k := i % sns
		seqs[k]++
		sn := fmt.Sprintf("%s%05d", prefix, k+1)
		ts := baseTs + int64(i)
		payload, _ := json.Marshal(floodTelemetry{Seq: seqs[k], Ts: ts, WorkState: 2, PowerLevel: 80,
			TempCavity: 40 + float64(i%10)/10, TempWater: 25, FanRPM: 3000, LaserHours: 100, Progress: int(seqs[k] % 101)})
		out = append(out, FloodMsg{
			Subject: envelope.Subject(envelope.KindTelemetry, cellmap.CellOf(sn, cells)),
			Env:     envelope.Envelope{PK: pk, SN: sn, Kind: envelope.KindTelemetry, Seq: seqs[k], RecvTs: ts, Payload: payload},
		})
	}
	return out
}

// RowCounter 抽象 TDengine 行数（便于单测）。
type RowCounter interface {
	CountRows(ctx context.Context, stable string) (int64, error)
}

type FloodSummary struct {
	Pre          int
	Before       int64
	After        int64
	PublishTook  time.Duration
	DrainTook    time.Duration
	MsgPerSec    float64
	Drained      bool
	PendingAtEnd int64
}

func (s FloodSummary) String() string {
	delta := s.After - s.Before - int64(s.Pre)
	recon := "RECONCILED: before + pre == after (zero loss)"
	if delta != 0 {
		recon = fmt.Sprintf("DELTA %+d: before(%d) + pre(%d) != after(%d)", delta, s.Before, s.Pre, s.After)
	}
	return fmt.Sprintf("flood summary: pre=%d publish=%s drain=%s throughput=%.0f msg/s drained=%v pending_at_end=%d rows_before=%d rows_after=%d\n%s",
		s.Pre, s.PublishTook.Truncate(time.Millisecond), s.DrainTook.Truncate(time.Millisecond), s.MsgPerSec, s.Drained, s.PendingAtEnd,
		s.Before, s.After, recon)
}

// PendingTotal 汇总各 cell 的 pipeline-<cell> 消费者 NumPending + NumAckPending。
func PendingTotal(ctx context.Context, js jetstream.JetStream, cells int) (int64, error) {
	var total int64
	for c := 1; c <= cells; c++ {
		name := fmt.Sprintf("pipeline-%d", c)
		cons, err := js.Consumer(ctx, envelope.StreamUp, name)
		if err != nil {
			return 0, fmt.Errorf("consumer %s: %w", name, err)
		}
		info, err := cons.Info(ctx)
		if err != nil {
			return 0, fmt.Errorf("consumer %s info: %w", name, err)
		}
		total += int64(info.NumPending) + int64(info.NumAckPending)
	}
	return total, nil
}

// RunFlood：记 TDengine 行数 → 预灌 JetStream → 计时排空（轮询 pipeline-<cell> pending 至 0）→ 行数对账。
// CSV：t_ms,pending_total。
func RunFlood(ctx context.Context, opts FloodOpts, js jetstream.JetStream, td RowCounter, csvOut, log io.Writer) (FloodSummary, error) {
	opts.defaults()
	cw := NewCSVWriter(csvOut, "t_ms", "pending_total")
	sum := FloodSummary{Pre: opts.Pre}

	before, err := td.CountRows(ctx, "telemetry")
	if err != nil {
		return sum, fmt.Errorf("count before: %w", err)
	}
	sum.Before = before
	fmt.Fprintf(log, "rows before: %d\n", before)

	msgs := BuildFloodEnvelopes(opts.Pre, opts.Cells, opts.SNs, opts.PK, opts.SNPrefix, time.Now().UnixMilli())
	pubStart := time.Now()
	for i := 0; i < len(msgs); i += opts.Chunk {
		end := min(i+opts.Chunk, len(msgs))
		for _, m := range msgs[i:end] {
			if _, err := js.PublishAsync(m.Subject, m.Env.Marshal()); err != nil {
				return sum, fmt.Errorf("publish: %w", err)
			}
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(30 * time.Second):
			return sum, fmt.Errorf("publish async: timeout waiting for acks")
		case <-ctx.Done():
			return sum, ctx.Err()
		}
	}
	sum.PublishTook = time.Since(pubStart)
	fmt.Fprintf(log, "pre-published %d envelopes across %d cells in %s\n", len(msgs), opts.Cells, sum.PublishTook.Truncate(time.Millisecond))

	drainStart := time.Now()
	deadline := drainStart.Add(opts.Timeout)
	t := time.NewTicker(opts.Poll)
	defer t.Stop()
	var pending int64
	for {
		pending, err = PendingTotal(ctx, js, opts.Cells)
		if err != nil {
			return sum, err
		}
		cw.Row(time.Since(drainStart).Milliseconds(), pending)
		if pending == 0 {
			sum.Drained = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return sum, ctx.Err()
		case <-t.C:
		}
	}
	sum.DrainTook = time.Since(drainStart)
	sum.PendingAtEnd = pending
	if sum.DrainTook > 0 {
		sum.MsgPerSec = float64(opts.Pre-int(pending)) / sum.DrainTook.Seconds()
	}
	// 给 pipeline 最后一批落库留一点余量再数
	time.Sleep(time.Second)
	after, err := td.CountRows(ctx, "telemetry")
	if err != nil {
		return sum, fmt.Errorf("count after: %w", err)
	}
	sum.After = after
	return sum, nil
}
