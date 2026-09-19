package ota

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Window 是批次的维护窗口（policy.window，技术方案 BL5 §08）：只在窗口内下发。
// Start / End 为本地 "HH:MM"（含头不含尾，与 internal/pkg/schedule 同约定）；
// TZ 为 IANA 时区名（空 = UTC）；Weekdays 为 0=周日..6=周六，空表示每天。
type Window struct {
	Start    string `json:"start"`
	End      string `json:"end"`
	TZ       string `json:"tz,omitempty"`
	Weekdays []int  `json:"weekdays,omitempty"`
}

// Empty 报告窗口是否为零值（不限制下发时间）。
func (w Window) Empty() bool {
	return strings.TrimSpace(w.Start) == "" && strings.TrimSpace(w.End) == ""
}

// InWindow 纯函数：now 是否落在维护窗口内。
//   - 零值窗口（Start 与 End 都空）→ true，不限制；
//   - 时区按 TZ 解析，TZ 为空或无效 → UTC（**fail-open**：窗口配错不能让批次永远卡住不下发，
//     格式与时区的真正把关在 ParseWindow，建批时就 400 拒绝）；
//   - Start > End 视为跨午夜（22:00→06:00）；Start == End 视为全天；
//   - Weekdays 非空时按**窗口起始日**的星期判定（跨午夜窗口在 00:00–End 这段算前一天的窗口）。
func InWindow(w Window, now time.Time) bool {
	if w.Empty() {
		return true
	}
	start, ok1 := parseHHMM(w.Start)
	end, ok2 := parseHHMM(w.End)
	if !ok1 || !ok2 {
		return true // 同上：非法时刻不阻塞下发
	}
	t := now.In(windowLoc(w.TZ))
	cur := t.Hour()*60 + t.Minute()

	startDay := t // 窗口起始日：判周几用它，不是 now 的自然日
	switch {
	case start == end: // 退化：全天开放
	case start < end:
		if cur < start || cur >= end {
			return false
		}
	default: // 跨午夜
		switch {
		case cur >= start: // 今天的窗口刚开始
		case cur < end: // 还在昨天开始的窗口里
			startDay = t.AddDate(0, 0, -1)
		default:
			return false
		}
	}
	if len(w.Weekdays) == 0 {
		return true
	}
	wd := int(startDay.Weekday())
	for _, d := range w.Weekdays {
		if d == wd {
			return true
		}
	}
	return false
}

// ParseWindow 从批次 policy JSON 取 window 并校验。policy 为空 / 无 window 键 → 零值窗口（不限制）。
// 校验失败返回 error，调用方翻成 400——错窗口在建批时就该拒绝，而不是等到 DispatchOnce 静默跳过。
func ParseWindow(raw json.RawMessage) (Window, error) {
	var w Window
	if len(raw) == 0 || string(raw) == "null" {
		return w, nil
	}
	var p struct {
		Window *Window `json:"window"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return w, fmt.Errorf("policy: %w", err)
	}
	if p.Window == nil {
		return w, nil
	}
	w = *p.Window
	w.Start, w.End, w.TZ = strings.TrimSpace(w.Start), strings.TrimSpace(w.End), strings.TrimSpace(w.TZ)
	if err := w.Validate(); err != nil {
		return Window{}, err
	}
	return w, nil
}

// Validate 校验窗口：start 与 end 要么都空要么都是合法 "HH:MM"，weekdays ∈ [0,6]，tz 可解析。
func (w Window) Validate() error {
	if w.Empty() {
		if w.Start != "" || w.End != "" {
			return fmt.Errorf("window: start and end must both be set")
		}
		return w.validateRest()
	}
	if _, ok := parseHHMM(w.Start); !ok {
		return fmt.Errorf("window: bad start %q (want HH:MM)", w.Start)
	}
	if _, ok := parseHHMM(w.End); !ok {
		return fmt.Errorf("window: bad end %q (want HH:MM)", w.End)
	}
	return w.validateRest()
}

func (w Window) validateRest() error {
	for _, d := range w.Weekdays {
		if d < 0 || d > 6 {
			return fmt.Errorf("window: weekday %d out of range 0..6", d)
		}
	}
	if w.TZ != "" {
		if _, err := time.LoadLocation(w.TZ); err != nil {
			return fmt.Errorf("window: bad tz %q", w.TZ)
		}
	}
	return nil
}

// windowLoc 解析时区，空或无效一律 UTC。
func windowLoc(tz string) *time.Location {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return time.UTC
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.UTC
	}
	return loc
}

// parseHHMM 严格解析 "HH:MM" 为当日分钟数；非法返回 ok=false。
func parseHHMM(s string) (int, bool) {
	s = strings.TrimSpace(s)
	if len(s) != 5 || s[2] != ':' {
		return 0, false
	}
	for _, i := range []int{0, 1, 3, 4} {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	h, err1 := strconv.Atoi(s[0:2])
	m, err2 := strconv.Atoi(s[3:5])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}
