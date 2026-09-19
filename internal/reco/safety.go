package reco

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// 安全关联下线（技术方案 §9.3）：使用某推荐档的任务里出现安全事件的比例，
// 显著高于同材料其它任务时，自动把该档 version_removed / status=removed。

const (
	SafetyRateMultiplier = 2.0 // 推荐档事故率 > 官方档 × 2
	SafetyMinAbs         = 3   // 且绝对数 ≥ 3
	SafetyWindow         = 7 * 24 * time.Hour
	// JobFallbackSpan 是任务无 finished_at 时用于关联事件的时间窗。
	JobFallbackSpan = 2 * time.Hour
)

// SafetyUnpublish 纯函数：recoRate > 2 × officialRate 且 recoAbs ≥ 3。
// officialRate 为 0 时只要 recoAbs ≥ 3 且 recoRate > 0 即下线（没有基线也不能放过绝对数）。
func SafetyUnpublish(recoRate, officialRate float64, recoAbs int) bool {
	if recoAbs < SafetyMinAbs {
		return false
	}
	return recoRate > officialRate*SafetyRateMultiplier
}

// SafetyEvent 是 TDengine 中的一条安全事件。
type SafetyEvent struct {
	SN string
	At time.Time
}

// JobSpan 是一条任务的时间窗。
type JobSpan struct {
	SN         string
	Start, End time.Time
}

// CountJobsWithSafety 纯函数：返回时间窗内出现同 SN 安全事件的任务数。End 为零值时用 Start+JobFallbackSpan。
func CountJobsWithSafety(jobs []JobSpan, events []SafetyEvent) int {
	bySN := map[string][]time.Time{}
	for _, e := range events {
		bySN[e.SN] = append(bySN[e.SN], e.At)
	}
	for _, ts := range bySN {
		sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	}
	n := 0
	for _, j := range jobs {
		end := j.End
		if end.IsZero() {
			end = j.Start.Add(JobFallbackSpan)
		}
		ts := bySN[j.SN]
		i := sort.Search(len(ts), func(i int) bool { return !ts[i].Before(j.Start) })
		if i < len(ts) && !ts[i].After(end) {
			n++
		}
	}
	return n
}

// Rate 返回 hits/total，total 为 0 返回 0。
func Rate(hits, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// BuildSafetyEventsQuery 生成 TDengine 查询：窗口内安全事件 (ts, sn)。
func BuildSafetyEventsQuery(since time.Time) string {
	codes := make([]string, 0, len(envelope.SafetyCodes))
	for c := range envelope.SafetyCodes {
		codes = append(codes, "'"+c+"'")
	}
	sort.Strings(codes)
	return fmt.Sprintf("SELECT ts, sn FROM iot.events WHERE ts >= %d AND code IN (%s)", since.UnixMilli(), strings.Join(codes, ","))
}

// ParseSafetyEvents 解析 TDengine REST 结果；按 column_meta 定位列；坏行跳过并计数。
func ParseSafetyEvents(res *tdengine.Result) (events []SafetyEvent, skipped int) {
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
	ti, si := 0, 1
	if i, ok := idx["ts"]; ok {
		ti = i
	}
	if i, ok := idx["sn"]; ok {
		si = i
	}
	for _, row := range res.Data {
		if len(row) <= ti || len(row) <= si {
			skipped++
			continue
		}
		at, ok := parseTs(row[ti])
		sn, ok2 := row[si].(string)
		if !ok || !ok2 || sn == "" {
			skipped++
			continue
		}
		events = append(events, SafetyEvent{SN: strings.TrimSpace(sn), At: at})
	}
	return events, skipped
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
	case int64:
		return time.UnixMilli(x).UTC(), true
	}
	return time.Time{}, false
}
