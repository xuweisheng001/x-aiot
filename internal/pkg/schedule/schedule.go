// Package schedule 是课表与锁判定的共享纯函数：云端（fleet-svc 对账/下发）与设备端（模拟器本地兜底）import 同一份实现，
// 保证两边对「此刻应不应该锁」的判定一致（BL5 技术方案 §6、§17.4；预推演 INC-5-10 夏令时）。
//
// 语义（§6.1）：窗口内 = 允许使用，窗口外 = locked；空 weekly 表示始终允许；窗口含头不含尾；
// overrides 按日期整体替换当天窗口，空 windows 表示当天全天 locked；跨午夜窗口（end < start）覆盖到次日 end。
package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"
	_ "time/tzdata" // 容器与设备镜像可能没有系统 zoneinfo；内嵌保证 IANA 时区可解析
)

// Window 是 "HH:MM"-"HH:MM" 的一段。
type Window struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// Override 是某一天的整体替换。
type Override struct {
	Date    string   `json:"date"` // YYYY-MM-DD（按 policy.TZ 解释）
	Windows []Window `json:"windows"`
}

// Policy 是课表快照（desired.schedule 与 schedule_policy 同结构）。
type Policy struct {
	Version   int64               `json:"version"`
	TZ        string              `json:"tz"`
	Weekly    map[string][]Window `json:"weekly"` // 键 mon..sun
	Overrides []Override          `json:"overrides,omitempty"`
}

var weekdayKeys = map[time.Weekday]string{
	time.Monday: "mon", time.Tuesday: "tue", time.Wednesday: "wed", time.Thursday: "thu",
	time.Friday: "fri", time.Saturday: "sat", time.Sunday: "sun",
}

// Location 解析 policy.TZ；空或非法回退 UTC（并返回 error 供上层记录，判定仍可继续）。
func Location(tz string) (*time.Location, error) {
	if strings.TrimSpace(tz) == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.UTC, fmt.Errorf("schedule: bad tz %q: %w", tz, err)
	}
	return loc, nil
}

// Enabled 报告课表是否启用：weekly 为空且无 overrides 表示未启用（始终允许）。
func (p Policy) Enabled() bool {
	for _, ws := range p.Weekly {
		if len(ws) > 0 {
			return true
		}
	}
	return len(p.Overrides) > 0
}

// ShouldLock 报告在 now 时刻设备是否应处于 locked。未启用课表 → false。
func ShouldLock(p Policy, now time.Time) bool {
	if !p.Enabled() {
		return false
	}
	loc, _ := Location(p.TZ)
	t := now.In(loc)
	return !allowedAt(p, t)
}

// allowedAt：今天的窗口（override 优先）覆盖 t，或昨天的跨午夜窗口延伸覆盖 t。
func allowedAt(p Policy, t time.Time) bool {
	minute := t.Hour()*60 + t.Minute()
	today := windowsFor(p, t)
	for _, w := range today {
		s, e, ok := parseWindow(w)
		if !ok {
			continue
		}
		if e > s { // 同日窗口 [s, e)
			if minute >= s && minute < e {
				return true
			}
		} else if e < s { // 跨午夜：今天 [s, 24:00)
			if minute >= s {
				return true
			}
		}
		// e == s 视为空窗口
	}
	yesterday := windowsFor(p, t.AddDate(0, 0, -1))
	for _, w := range yesterday {
		s, e, ok := parseWindow(w)
		if !ok || e >= s {
			continue
		}
		if minute < e { // 昨天跨午夜窗口的次日部分 [00:00, e)
			return true
		}
	}
	return false
}

// windowsFor 返回某一天生效的窗口：有 override 用 override（可为空 = 全天 locked），否则 weekly。
func windowsFor(p Policy, day time.Time) []Window {
	date := day.Format("2006-01-02")
	for _, o := range p.Overrides {
		if o.Date == date {
			return o.Windows
		}
	}
	return p.Weekly[weekdayKeys[day.Weekday()]]
}

// parseWindow 把 "HH:MM" 解析为分钟；"24:00" 允许作为 End。
func parseWindow(w Window) (start, end int, ok bool) {
	s, err1 := parseHM(w.Start)
	e, err2 := parseHM(w.End)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return s, e, true
}

func parseHM(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "24:00" {
		return 24 * 60, nil
	}
	var h, m int
	if _, err := fmt.Sscanf(s, "%d:%d", &h, &m); err != nil {
		return 0, err
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, errors.New("out of range")
	}
	return h*60 + m, nil
}

// Validate 检查课表格式：时区可解析、窗口可解析、weekday 键合法、override 日期合法。
func Validate(p Policy) error {
	if _, err := Location(p.TZ); err != nil {
		return err
	}
	valid := map[string]bool{"mon": true, "tue": true, "wed": true, "thu": true, "fri": true, "sat": true, "sun": true}
	for k, ws := range p.Weekly {
		if !valid[k] {
			return fmt.Errorf("schedule: bad weekday %q", k)
		}
		for _, w := range ws {
			if _, _, ok := parseWindow(w); !ok {
				return fmt.Errorf("schedule: bad window %s-%s on %s", w.Start, w.End, k)
			}
		}
	}
	for _, o := range p.Overrides {
		if _, err := time.Parse("2006-01-02", o.Date); err != nil {
			return fmt.Errorf("schedule: bad override date %q", o.Date)
		}
		for _, w := range o.Windows {
			if _, _, ok := parseWindow(w); !ok {
				return fmt.Errorf("schedule: bad window %s-%s on %s", w.Start, w.End, o.Date)
			}
		}
	}
	return nil
}

// ParseCompact 解析模拟器 flag 风格的紧凑课表，如 "mon-fri 08:00-17:00,sat 09:00-12:00"。
// 空串返回未启用的 Policy。
func ParseCompact(s, tz string) (Policy, error) {
	p := Policy{TZ: tz, Weekly: map[string][]Window{}}
	s = strings.TrimSpace(s)
	if s == "" {
		return p, nil
	}
	order := []string{"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
	idx := map[string]int{}
	for i, d := range order {
		idx[d] = i
	}
	for _, part := range strings.Split(s, ",") {
		fields := strings.Fields(part)
		if len(fields) != 2 {
			return p, fmt.Errorf("schedule: bad segment %q (want 'days HH:MM-HH:MM')", part)
		}
		days, span := fields[0], fields[1]
		st, en, ok := strings.Cut(span, "-")
		if !ok {
			return p, fmt.Errorf("schedule: bad window %q", span)
		}
		w := Window{Start: st, End: en}
		if _, _, ok := parseWindow(w); !ok {
			return p, fmt.Errorf("schedule: bad window %q", span)
		}
		from, to := days, days
		if a, b, ok := strings.Cut(days, "-"); ok {
			from, to = a, b
		}
		fi, ok1 := idx[strings.ToLower(from)]
		ti, ok2 := idx[strings.ToLower(to)]
		if !ok1 || !ok2 || fi > ti {
			return p, fmt.Errorf("schedule: bad days %q", days)
		}
		for i := fi; i <= ti; i++ {
			p.Weekly[order[i]] = append(p.Weekly[order[i]], w)
		}
	}
	return p, Validate(p)
}
