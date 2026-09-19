package alarm

import (
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

func TestMissing(t *testing.T) {
	t0 := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	ev := func(sn, code string, off time.Duration) Key { return Key{SN: sn, Code: code, Ts: t0.Add(off)} }
	tol := time.Second
	cases := []struct {
		name   string
		events []Key
		alarms []Key
		want   []Key
	}{
		{"完全匹配", []Key{ev("A", "FLAME_DETECTED", 0)}, []Key{ev("A", "FLAME_DETECTED", 0)}, nil},
		{"缺一条", []Key{ev("A", "FLAME_DETECTED", 0), ev("B", "OVER_TEMP", time.Minute)}, []Key{ev("A", "FLAME_DETECTED", 0)}, []Key{ev("B", "OVER_TEMP", time.Minute)}},
		{"容差内 +800ms", []Key{ev("A", "TILT", 0)}, []Key{ev("A", "TILT", 800*time.Millisecond)}, nil},
		{"容差外 +2s", []Key{ev("A", "TILT", 0)}, []Key{ev("A", "TILT", 2*time.Second)}, []Key{ev("A", "TILT", 0)}},
		{"被更早告警 squelch 覆盖（回看 5min）", []Key{ev("A", "ESTOP", 3*time.Minute)}, []Key{ev("A", "ESTOP", 0)}, nil},
		{"超过 squelch 窗口", []Key{ev("A", "ESTOP", 6*time.Minute)}, []Key{ev("A", "ESTOP", 0)}, []Key{ev("A", "ESTOP", 6*time.Minute)}},
		{"重复事件只报一条", []Key{ev("A", "WATER_FLOW", 0), ev("A", "WATER_FLOW", 500*time.Millisecond), ev("A", "WATER_FLOW", 2*time.Minute)}, nil, []Key{ev("A", "WATER_FLOW", 0)}},
		{"同 SN 不同 code 互不覆盖", []Key{ev("A", "FLAME_DETECTED", 0), ev("A", "OVER_TEMP", 0)}, []Key{ev("A", "FLAME_DETECTED", 0)}, []Key{ev("A", "OVER_TEMP", 0)}},
		{"空输入", nil, []Key{ev("A", "TILT", 0)}, nil},
		{"乱序输入输出按 ts 排序", []Key{ev("B", "TILT", time.Minute), ev("A", "TILT", 0)}, nil, []Key{ev("A", "TILT", 0), ev("B", "TILT", time.Minute)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Missing(c.events, c.alarms, tol)
			if len(got) != len(c.want) {
				t.Fatalf("got %v want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("[%d] got %v want %v", i, got[i], c.want[i])
				}
			}
		})
	}
}

func TestParseEventKeys(t *testing.T) {
	res := &tdengine.Result{
		ColumnMeta: [][]any{{"code", "VARCHAR", 32}, {"ts", "TIMESTAMP", 8}, {"sn", "VARCHAR", 32}}, // 乱序列
		Data: [][]any{
			{"FLAME_DETECTED", "2026-09-18T10:00:00.123Z", "SN1"},
			{"OVER_TEMP", float64(1758189600000), "SN2 "},
			{"TILT", "not-a-time", "SN3"},
			{"ESTOP", "2026-09-18T10:00:00Z"}, // 短行
			{"", "2026-09-18T10:00:00Z", "SN4"},
		},
	}
	keys, skipped := ParseEventKeys(res)
	if skipped != 3 || len(keys) != 2 {
		t.Fatalf("keys=%v skipped=%d", keys, skipped)
	}
	if keys[0] != (Key{SN: "SN1", Code: "FLAME_DETECTED", Ts: time.Date(2026, 9, 18, 10, 0, 0, 123_000_000, time.UTC)}) {
		t.Fatalf("keys[0]=%v", keys[0])
	}
	if keys[1].SN != "SN2" || keys[1].Ts.UnixMilli() != 1758189600000 {
		t.Fatalf("keys[1]=%v", keys[1])
	}
	if k, s := ParseEventKeys(nil); k != nil || s != 0 {
		t.Fatal("nil result")
	}
}

func TestBuildEventsQuery(t *testing.T) {
	q := BuildEventsQuery(time.UnixMilli(1000))
	if !strings.HasPrefix(q, "SELECT ts, sn, code FROM iot.events WHERE ts >= 1000 AND code IN (") {
		t.Fatalf("q=%s", q)
	}
	for _, c := range []string{"'FLAME_DETECTED'", "'OVER_TEMP'", "'TILT'", "'ESTOP'", "'WATER_FLOW'"} {
		if !strings.Contains(q, c) {
			t.Fatalf("missing %s in %s", c, q)
		}
	}
}
