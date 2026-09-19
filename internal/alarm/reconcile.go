package alarm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// 对账（INC-10 检测手段）：TDengine iot.events 里的安全事件必须在 PG alarm 里有对应行。
// 缺失即"漏告警"，计数 reconcile_missing 并绕过 squelch 补录（reconcile_created）。

const (
	DefaultReconcileInterval  = 5 * time.Minute
	DefaultReconcileWindow    = 30 * time.Minute
	DefaultReconcileTolerance = time.Second
)

// Key 是一条安全事件 / 告警的对账键。
type Key struct {
	SN   string
	Code string
	Ts   time.Time
}

func (k Key) pair() string { return k.SN + "\x00" + k.Code }

// Missing 返回 events 中没有被任何 alarm 覆盖的事件（TDengine 有、PG 无）。
//
// 覆盖规则：同 (sn, code) 的 alarm.event_ts ∈ [ev.Ts − SquelchTTL, ev.Ts + tolerance]。
// 右侧容差吸收设备时间与服务端纠偏的毫秒级误差；左侧回看 SquelchTTL 是因为正常链路里
// 5 分钟窗口内的重复事件会被 squelch 吞掉、本来就不该有告警，不能当作漏报。
// 输出按同样规则去重（两条相互覆盖的缺失事件只报一条），并按 Ts 升序、稳定。
func Missing(events, alarms []Key, tolerance time.Duration) []Key {
	if tolerance < 0 {
		tolerance = 0
	}
	byPair := map[string][]time.Time{}
	for _, a := range alarms {
		byPair[a.pair()] = append(byPair[a.pair()], a.Ts)
	}
	for _, ts := range byPair {
		sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	}
	evs := make([]Key, len(events))
	copy(evs, events)
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].Ts.Before(evs[j].Ts) })

	covered := func(list []time.Time, ev time.Time) bool {
		lo, hi := ev.Add(-SquelchTTL), ev.Add(tolerance)
		i := sort.Search(len(list), func(i int) bool { return !list[i].Before(lo) })
		return i < len(list) && !list[i].After(hi)
	}
	var out []Key
	emitted := map[string][]time.Time{}
	for _, ev := range evs {
		p := ev.pair()
		if covered(byPair[p], ev.Ts) || covered(emitted[p], ev.Ts) {
			continue
		}
		emitted[p] = append(emitted[p], ev.Ts)
		out = append(out, ev)
	}
	return out
}

// safetyCodeList 生成 SQL IN 列表（稳定顺序）。
func safetyCodeList() string {
	codes := make([]string, 0, len(envelope.SafetyCodes))
	for c := range envelope.SafetyCodes {
		codes = append(codes, "'"+c+"'")
	}
	sort.Strings(codes)
	return strings.Join(codes, ",")
}

// BuildEventsQuery 生成 TDengine 查询：窗口内的安全事件 (ts, sn, code)。
func BuildEventsQuery(since time.Time) string {
	return fmt.Sprintf("SELECT ts, sn, code FROM iot.events WHERE ts >= %d AND code IN (%s)", since.UnixMilli(), safetyCodeList())
}

// ParseEventKeys 把 TDengine REST 结果解析成 Key。按 column_meta 名字定位列，
// ts 兼容 RFC3339(Nano) 字符串与毫秒 epoch 数值；坏行跳过并计数（返回 skipped）。
func ParseEventKeys(res *tdengine.Result) (keys []Key, skipped int) {
	if res == nil {
		return nil, 0
	}
	idx := map[string]int{}
	for i, m := range res.ColumnMeta {
		if len(m) > 0 {
			if name, ok := m[0].(string); ok {
				idx[strings.ToLower(name)] = i
			}
		}
	}
	ti, si, ci := colIndex(idx, "ts", 0), colIndex(idx, "sn", 1), colIndex(idx, "code", 2)
	for _, row := range res.Data {
		if len(row) <= ti || len(row) <= si || len(row) <= ci {
			skipped++
			continue
		}
		ts, ok := parseTs(row[ti])
		sn, ok2 := row[si].(string)
		code, ok3 := row[ci].(string)
		if !ok || !ok2 || !ok3 || sn == "" || code == "" {
			skipped++
			continue
		}
		keys = append(keys, Key{SN: strings.TrimSpace(sn), Code: code, Ts: ts})
	}
	return keys, skipped
}

func colIndex(idx map[string]int, name string, def int) int {
	if i, ok := idx[name]; ok {
		return i
	}
	return def
}

func parseTs(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t.UTC(), true
			}
		}
		return time.Time{}, false
	case float64:
		return time.UnixMilli(int64(x)).UTC(), true
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return time.UnixMilli(n).UTC(), true
		}
	case int64:
		return time.UnixMilli(x).UTC(), true
	}
	return time.Time{}, false
}

// TDQuerier 是对账依赖的 TDengine 子集（*tdengine.Client 满足）。
type TDQuerier interface {
	Query(ctx context.Context, sql string) (*tdengine.Result, error)
}

// Reconciler 周期性对账并补录。
type Reconciler struct {
	TD        TDQuerier
	Svc       *Service
	Window    time.Duration
	Tolerance time.Duration
	Now       func() time.Time
}

func NewReconciler(td TDQuerier, svc *Service, window, tolerance time.Duration) *Reconciler {
	if window <= 0 {
		window = DefaultReconcileWindow
	}
	if tolerance <= 0 {
		tolerance = DefaultReconcileTolerance
	}
	return &Reconciler{TD: td, Svc: svc, Window: window, Tolerance: tolerance, Now: time.Now}
}

// Report 是一轮对账的结果。
type Report struct {
	Events, Alarms, Missing, Created int
}

// RunOnce 执行一轮：查 TDengine → 查 PG → Missing → 补录。TDengine/PG 查询失败返回 err（不补录）。
func (r *Reconciler) RunOnce(ctx context.Context) (Report, error) {
	var rep Report
	since := r.Now().Add(-r.Window)
	res, err := r.TD.Query(ctx, BuildEventsQuery(since))
	if err != nil {
		r.Svc.M.Inc("reconcile_errors")
		return rep, fmt.Errorf("reconcile: query events: %w", err)
	}
	events, skipped := ParseEventKeys(res)
	if skipped > 0 {
		r.Svc.M.Add("reconcile_bad_rows", int64(skipped))
	}
	alarms, err := r.Svc.alarmKeysSince(ctx, since)
	if err != nil {
		r.Svc.M.Inc("reconcile_errors")
		return rep, fmt.Errorf("reconcile: query alarms: %w", err)
	}
	missing := Missing(events, alarms, r.Tolerance)
	rep.Events, rep.Alarms, rep.Missing = len(events), len(alarms), len(missing)
	r.Svc.M.Inc("reconcile_runs")
	r.Svc.M.Add("reconcile_missing", int64(len(missing)))
	for _, k := range missing {
		payload, _ := json.Marshal(map[string]any{"seq": 0, "ts": k.Ts.UnixMilli(), "code": k.Code, "msg": "reconciled: alarm missing for event"})
		env := &envelope.Envelope{SN: k.SN, Kind: envelope.KindEvent, RecvTs: r.Now().UnixMilli(), Payload: payload}
		out, err := r.Svc.HandleEventNoSquelch(ctx, env)
		if err != nil {
			r.Svc.M.Inc("reconcile_errors")
			slog.Error("reconcile: create alarm failed", "sn", k.SN, "code", k.Code, "event_ts", k.Ts, "err", err)
			continue
		}
		if out == OutcomeNotified || out == OutcomeOpen {
			rep.Created++
			slog.Warn("reconcile: missing alarm re-created", "sn", k.SN, "code", k.Code, "event_ts", k.Ts, "outcome", out)
		}
	}
	r.Svc.M.Add("reconcile_created", int64(rep.Created))
	slog.Info("alarm reconcile", "events", rep.Events, "alarms", rep.Alarms, "missing", rep.Missing, "created", rep.Created, "window", r.Window)
	return rep, nil
}

// Run 每 interval 跑一轮，直到 ctx 取消。
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultReconcileInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
				slog.Error("reconcile round failed", "err", err)
			}
		}
	}
}

// alarmKeysSince 查 PG 窗口内的告警键。
func (s *Service) alarmKeysSince(ctx context.Context, since time.Time) ([]Key, error) {
	rows, err := s.DB.Query(ctx, `SELECT sn, code, event_ts FROM iot_shard.alarm WHERE event_ts >= $1`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		var k Key
		if err := rows.Scan(&k.SN, &k.Code, &k.Ts); err != nil {
			return nil, err
		}
		k.Ts = k.Ts.UTC()
		out = append(out, k)
	}
	return out, rows.Err()
}
