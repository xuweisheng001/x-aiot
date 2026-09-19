package support

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

// 批次缺陷早发现（§9）：同 (product_key, fw_version, code) 24 h 内 distinct 设备数 ≥ 阈值 → defect_alert，冷却 24 h。

const (
	DefaultDefectInterval  = 10 * time.Minute
	DefaultDefectWindow    = 24 * time.Hour
	DefaultDefectThreshold = 20
	DefaultDefectCooldown  = 24 * time.Hour
	// UnknownFW 空 fw_version 回填值（INC-6-13）。
	UnknownFW = "unknown"
)

// excludedFromDefect：安全码走告警链路，任务 / 系统事件不是错误码。
var excludedFromDefect = func() map[string]bool {
	m := map[string]bool{"JOB_START": true, "JOB_DONE": true, "JOB_FAIL": true, "JOB_PAUSE": true, "CMD_ACK": true, "OTA_PROGRESS": true, "HEARTBEAT": true}
	for c := range envelope.SafetyCodes {
		m[c] = true
	}
	return m
}()

func excludedCodeList() string {
	codes := make([]string, 0, len(excludedFromDefect))
	for c := range excludedFromDefect {
		codes = append(codes, "'"+c+"'")
	}
	sort.Strings(codes)
	return strings.Join(codes, ",")
}

// BuildDefectQuery：events 超级表只有 sn / product_key 两个标签，没有 fw_version，
// 所以按 (sn, product_key, code) 聚合后在 Go 里用 PG device.fw_version 关联（§9.1 的实现修正）。
func BuildDefectQuery(since time.Time) string {
	return fmt.Sprintf("SELECT sn, product_key, code, count(*) AS events, min(ts) AS first_seen FROM iot.events WHERE ts >= %d AND code NOT IN (%s) GROUP BY sn, product_key, code",
		since.UnixMilli(), excludedCodeList())
}

// EventAgg 是 TDengine 聚合结果的一行。
type EventAgg struct {
	SN, ProductKey, Code string
	Events               int
	FirstSeen            time.Time
}

// ParseDefectRows 按列名解析 TDengine 结果；坏行跳过。
func ParseDefectRows(res *tdengine.Result) (rows []EventAgg, skipped int) {
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
	get := func(name string, def int) int {
		if i, ok := idx[name]; ok {
			return i
		}
		return def
	}
	si, pi, ci, ei, fi := get("sn", 0), get("product_key", 1), get("code", 2), get("events", 3), get("first_seen", 4)
	for _, r := range res.Data {
		if len(r) <= si || len(r) <= pi || len(r) <= ci || len(r) <= ei {
			skipped++
			continue
		}
		sn, ok1 := r[si].(string)
		pk, ok2 := r[pi].(string)
		code, ok3 := r[ci].(string)
		n, ok4 := toInt(r[ei])
		if !ok1 || !ok2 || !ok3 || !ok4 || sn == "" || code == "" {
			skipped++
			continue
		}
		var fs time.Time
		if len(r) > fi {
			fs, _ = parseTs(r[fi])
		}
		rows = append(rows, EventAgg{SN: strings.TrimSpace(sn), ProductKey: pk, Code: code, Events: n, FirstSeen: fs})
	}
	return rows, skipped
}

func toInt(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case int:
		return x, true
	case int64:
		return int(x), true
	case json.Number:
		n, err := x.Int64()
		return int(n), err == nil
	}
	return 0, false
}

func parseTs(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t.UTC(), true
			}
		}
	case float64:
		return time.UnixMilli(int64(x)).UTC(), true
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return time.UnixMilli(n).UTC(), true
		}
	}
	return time.Time{}, false
}

// Combo 是 (product_key, fw_version, code) 的聚合。
type Combo struct {
	ProductKey, FWVersion, Code string
	Devices, Events             int
	FirstSeen                   time.Time
}

// AggregateDefects 纯函数：按设备的 fw_version 关联（缺失回填 UnknownFW 并计数），distinct SN 计数。输出按键排序稳定。
func AggregateDefects(rows []EventAgg, fwBySN map[string]string) (combos []Combo, backfilled int) {
	type key struct{ pk, fw, code string }
	acc := map[key]*Combo{}
	seen := map[key]map[string]bool{}
	for _, r := range rows {
		fw := fwBySN[r.SN]
		if fw == "" {
			fw = UnknownFW
			backfilled++
		}
		k := key{r.ProductKey, fw, r.Code}
		c := acc[k]
		if c == nil {
			c = &Combo{ProductKey: r.ProductKey, FWVersion: fw, Code: r.Code, FirstSeen: r.FirstSeen}
			acc[k] = c
			seen[k] = map[string]bool{}
		}
		if !seen[k][r.SN] {
			seen[k][r.SN] = true
			c.Devices++
		}
		c.Events += r.Events
		if !r.FirstSeen.IsZero() && (c.FirstSeen.IsZero() || r.FirstSeen.Before(c.FirstSeen)) {
			c.FirstSeen = r.FirstSeen
		}
	}
	for _, c := range acc {
		combos = append(combos, *c)
	}
	sort.Slice(combos, func(i, j int) bool {
		a, b := combos[i], combos[j]
		if a.ProductKey != b.ProductKey {
			return a.ProductKey < b.ProductKey
		}
		if a.FWVersion != b.FWVersion {
			return a.FWVersion < b.FWVersion
		}
		return a.Code < b.Code
	})
	return combos, backfilled
}

// ShouldAlert 纯函数：distinct 设备数达到阈值，且不在冷却期内（lastAlertAt+cooldown > now → 抑制）。
func ShouldAlert(distinctSN, threshold int, lastAlertAt *time.Time, now time.Time, cooldown time.Duration) bool {
	if threshold <= 0 {
		threshold = DefaultDefectThreshold
	}
	if distinctSN < threshold {
		return false
	}
	if lastAlertAt != nil && cooldown > 0 && lastAlertAt.Add(cooldown).After(now) {
		return false
	}
	return true
}

// DefectAlert 是 defect_alert 的一行。
type DefectAlert struct {
	ID            int64      `json:"id"`
	ProductKey    string     `json:"product_key"`
	FWVersion     string     `json:"fw_version"`
	ErrorCode     string     `json:"error_code"`
	DeviceCount   int        `json:"device_count"`
	EventCount    int        `json:"event_count"`
	OnlineCount   int        `json:"online_count"`
	FirstSeen     *time.Time `json:"first_seen,omitempty"`
	WindowStart   time.Time  `json:"window_start"`
	TicketID      string     `json:"ticket_id,omitempty"`
	NotifiedAt    *time.Time `json:"notified_at,omitempty"`
	CooldownUntil time.Time  `json:"cooldown_until"`
	CreatedAt     time.Time  `json:"created_at"`
}

// DefectReport 是一轮聚合的三数输出（INC-6-13：每轮必须报 rows / combos / alerts）。
type DefectReport struct {
	Rows, Combos, Alerts, Suppressed, Backfilled, Skipped int
}

// RunDefectOnce 执行一轮聚合。TDengine 不可用返回 err（调用方 WARN，下轮再试）。
func (s *Service) RunDefectOnce(ctx context.Context) (DefectReport, error) {
	var rep DefectReport
	if s.TDQ == nil {
		return rep, ErrUnavailable
	}
	now := s.now()
	res, err := s.TDQ.Query(ctx, BuildDefectQuery(now.Add(-DefaultDefectWindow)))
	if err != nil {
		s.M.Inc(MDefectErrors)
		return rep, err
	}
	rows, skipped := ParseDefectRows(res)
	rep.Rows, rep.Skipped = len(rows), skipped
	sns := make([]string, 0, len(rows))
	for _, r := range rows {
		sns = append(sns, r.SN)
	}
	fw, err := s.Store.FWBySN(ctx, sns)
	if err != nil {
		s.M.Inc(MDefectErrors)
		return rep, err
	}
	combos, backfilled := AggregateDefects(rows, fw)
	rep.Combos, rep.Backfilled = len(combos), backfilled
	s.M.Add(MDefectFWBackfill, int64(backfilled))
	for _, c := range combos {
		threshold, cooldown, err := s.Store.DefectThreshold(ctx, c.ProductKey, c.Code)
		if err != nil {
			s.M.Inc(MDefectErrors)
			continue
		}
		last, err := s.Store.LastDefectAlert(ctx, c.ProductKey, c.FWVersion, c.Code)
		if err != nil {
			s.M.Inc(MDefectErrors)
			continue
		}
		if !ShouldAlert(c.Devices, threshold, last, now, cooldown) {
			if c.Devices >= threshold {
				rep.Suppressed++
				s.M.Inc(MDefectCooldown)
			}
			continue
		}
		online, _ := s.Store.OnlineCount(ctx, c.ProductKey, c.FWVersion, now.Add(-DefaultDefectWindow))
		a := DefectAlert{ProductKey: c.ProductKey, FWVersion: c.FWVersion, ErrorCode: c.Code, DeviceCount: c.Devices, EventCount: c.Events,
			OnlineCount: online, WindowStart: now.UTC().Truncate(24 * time.Hour), CooldownUntil: now.Add(cooldown), CreatedAt: now}
		if !c.FirstSeen.IsZero() {
			fs := c.FirstSeen
			a.FirstSeen = &fs
		}
		created, err := s.Store.UpsertDefectAlert(ctx, a)
		if err != nil {
			s.M.Inc(MDefectErrors)
			continue
		}
		if created {
			rep.Alerts++
			s.M.Inc(MDefectAlerts)
			// 原型：日志模拟通知产品与固件团队 / xpilot 批次工单
			slog.Warn("DEFECT ALERT (simulated notify)", "product_key", c.ProductKey, "fw_version", c.FWVersion,
				"code", c.Code, "devices", c.Devices, "events", c.Events, "online", online)
		}
	}
	s.M.Inc(MDefectRuns)
	s.M.Add(MDefectCombos, int64(len(combos)))
	slog.Info("defect aggregate", "rows", rep.Rows, "combos", rep.Combos, "alerts", rep.Alerts,
		"suppressed", rep.Suppressed, "fw_backfilled", rep.Backfilled, "skipped", rep.Skipped)
	return rep, nil
}

// RunDefectAggregator 周期执行；启动即跑一轮。
func (s *Service) RunDefectAggregator(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultDefectInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := s.RunDefectOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("defect aggregate skipped", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
