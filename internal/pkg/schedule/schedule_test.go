package schedule

import (
	"testing"
	"time"
)

func at(loc *time.Location, y, mo, d, h, mi int) time.Time {
	return time.Date(y, time.Month(mo), d, h, mi, 0, 0, loc)
}

func TestShouldLockTable(t *testing.T) {
	la, _ := time.LoadLocation("America/Los_Angeles")
	sh, _ := time.LoadLocation("Asia/Shanghai")
	school := Policy{TZ: "America/Los_Angeles", Weekly: map[string][]Window{
		"mon": {{"08:00", "12:00"}, {"13:30", "17:00"}}, "tue": {{"08:00", "17:00"}}, "wed": {{"08:00", "17:00"}},
		"thu": {{"08:00", "17:00"}}, "fri": {{"08:00", "17:00"}},
	}, Overrides: []Override{{Date: "2026-11-27", Windows: nil}, {Date: "2026-11-28", Windows: []Window{{"10:00", "12:00"}}}}}
	night := Policy{TZ: "Asia/Shanghai", Weekly: map[string][]Window{"fri": {{"22:00", "02:00"}}}}
	empty := Policy{TZ: "UTC"}

	cases := []struct {
		name string
		p    Policy
		now  time.Time
		want bool
	}{
		// 2026-09-21 是周一
		{"周一上午窗口内", school, at(la, 2026, 9, 21, 9, 0), false},
		{"窗口起点含头", school, at(la, 2026, 9, 21, 8, 0), false},
		{"窗口终点不含尾", school, at(la, 2026, 9, 21, 12, 0), true},
		{"午休间隙 locked", school, at(la, 2026, 9, 21, 12, 30), true},
		{"下午窗口", school, at(la, 2026, 9, 21, 16, 59), false},
		{"放学后 locked", school, at(la, 2026, 9, 21, 17, 0), true},
		{"周六无窗口 locked", school, at(la, 2026, 9, 26, 10, 0), true},
		{"override 空窗口全天 locked（感恩节）", school, at(la, 2026, 11, 27, 10, 0), true},
		{"override 自定窗口内", school, at(la, 2026, 11, 28, 11, 0), false},
		{"override 自定窗口外", school, at(la, 2026, 11, 28, 9, 0), true},
		// 时区：上海 00:00 = 洛杉矶前一日 09:00（周一 9 月 21 日 UTC+8 → LA 9 月 20 日 09:00 周日）
		{"按 policy 时区解释而非 now 的时区", school, at(sh, 2026, 9, 22, 0, 0), false}, // LA 9/21 09:00 周一 → 允许
		// 夏令时切换日 2026-11-01（LA 凌晨 2 点回拨）：周日无窗口 → locked；次日周一 8 点允许
		{"DST 切换日周日 locked", school, at(la, 2026, 11, 1, 3, 0), true},
		{"DST 切换后周一正常", school, at(la, 2026, 11, 2, 8, 30), false},
		// 跨午夜：周五 22:00 到周六 02:00 允许
		{"跨午夜窗口当日部分", night, at(sh, 2026, 9, 25, 23, 30), false},
		{"跨午夜窗口次日部分", night, at(sh, 2026, 9, 26, 1, 59), false},
		{"跨午夜窗口次日结束不含尾", night, at(sh, 2026, 9, 26, 2, 0), true},
		{"跨午夜窗口前 locked", night, at(sh, 2026, 9, 25, 21, 59), true},
		{"未启用课表永不锁", empty, at(la, 2026, 9, 26, 3, 0), false},
	}
	for _, c := range cases {
		if got := ShouldLock(c.p, c.now); got != c.want {
			t.Errorf("%s: ShouldLock=%v want %v", c.name, got, c.want)
		}
	}
}

func TestBadTZFallsBackToUTC(t *testing.T) {
	p := Policy{TZ: "Mars/Olympus", Weekly: map[string][]Window{"mon": {{"08:00", "17:00"}}}}
	if err := Validate(p); err == nil {
		t.Fatal("Validate must reject bad tz")
	}
	// 判定仍可继续（UTC 解释）：2026-09-21 10:00Z 周一 → 允许
	if ShouldLock(p, time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)) {
		t.Fatal("bad tz should fall back to UTC and allow at 10:00Z Monday")
	}
}

func TestValidate(t *testing.T) {
	bad := []Policy{
		{TZ: "UTC", Weekly: map[string][]Window{"funday": {{"08:00", "09:00"}}}},
		{TZ: "UTC", Weekly: map[string][]Window{"mon": {{"8am", "09:00"}}}},
		{TZ: "UTC", Weekly: map[string][]Window{"mon": {{"25:00", "26:00"}}}},
		{TZ: "UTC", Overrides: []Override{{Date: "2026/01/01"}}},
	}
	for i, p := range bad {
		if Validate(p) == nil {
			t.Errorf("case %d must fail", i)
		}
	}
	if err := Validate(Policy{TZ: "UTC", Weekly: map[string][]Window{"mon": {{"08:00", "24:00"}}}}); err != nil {
		t.Fatalf("24:00 end must be valid: %v", err)
	}
}

func TestParseCompact(t *testing.T) {
	p, err := ParseCompact("mon-fri 08:00-17:00,sat 09:00-12:00", "UTC")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Weekly["mon"]) != 1 || len(p.Weekly["fri"]) != 1 || len(p.Weekly["sat"]) != 1 || len(p.Weekly["sun"]) != 0 {
		t.Fatalf("weekly=%v", p.Weekly)
	}
	if !p.Enabled() {
		t.Fatal("enabled")
	}
	if e, _ := ParseCompact("", "UTC"); e.Enabled() {
		t.Fatal("empty must be disabled")
	}
	for _, s := range []string{"mon", "mon 08:00", "fri-mon 08:00-09:00", "mon 8-9", "xyz 08:00-09:00"} {
		if _, err := ParseCompact(s, "UTC"); err == nil {
			t.Errorf("%q must fail", s)
		}
	}
}
