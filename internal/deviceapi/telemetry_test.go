package deviceapi

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

func TestParseTelemetryRange(t *testing.T) {
	now := time.UnixMilli(1_726_300_000_000)
	cases := []struct {
		name  string
		q     string
		want  TelemetryRange
		isErr bool
	}{
		{"defaults", "", TelemetryRange{From: now.UnixMilli() - 3_600_000, To: now.UnixMilli(), Limit: 100}, false},
		{"explicit", "from=100&to=200&limit=5", TelemetryRange{100, 200, 5}, false},
		{"limit capped", "from=100&to=200&limit=5000", TelemetryRange{100, 200, 1000}, false},
		{"only to → 1h before", "to=7200000", TelemetryRange{3600000, 7200000, 100}, false},
		{"only from", "from=1", TelemetryRange{1, now.UnixMilli(), 100}, false},
		{"from > to", "from=200&to=100", TelemetryRange{}, true},
		{"bad from", "from=abc", TelemetryRange{}, true},
		{"bad to", "to=-1", TelemetryRange{}, true},
		{"bad limit", "limit=0", TelemetryRange{}, true},
		{"limit not int", "limit=x", TelemetryRange{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, _ := url.ParseQuery(c.q)
			got, err := ParseTelemetryRange(q, now)
			if (err != nil) != c.isErr {
				t.Fatalf("err=%v", err)
			}
			if !c.isErr && got != c.want {
				t.Fatalf("got %+v want %+v", got, c.want)
			}
		})
	}
}

func TestBuildTelemetryQuery(t *testing.T) {
	got := BuildTelemetryQuery("XT-001", TelemetryRange{From: 10, To: 20, Limit: 3})
	want := "SELECT ts,seq,work_state,power_level,temp_cavity,temp_water,fan_rpm,laser_hours,progress FROM iot.t_xt_001 WHERE ts >= 10 AND ts <= 20 ORDER BY ts DESC LIMIT 3"
	if got != want {
		t.Fatalf("got %q", got)
	}
	if q := BuildTelemetryQuery("x'; DROP TABLE t; --", TelemetryRange{Limit: 1}); !strings.Contains(q, "FROM iot.t_x___drop_table_t____ WHERE") || strings.ContainsAny(q, "';") {
		t.Fatalf("injection not sanitized: %q", q)
	}
}

func TestRowsToMaps(t *testing.T) {
	res := &tdengine.Result{
		ColumnMeta: [][]any{{"ts", "TIMESTAMP", 8}, {"seq", "BIGINT", 8}, {"work_state", "TINYINT", 1}},
		Data:       [][]any{{"2024-09-14T08:00:00.000Z", float64(5), float64(2)}, {"2024-09-14T07:59:59.000Z", float64(4), float64(1)}},
	}
	rows := RowsToMaps(res)
	if len(rows) != 2 || rows[0]["seq"] != float64(5) || rows[1]["work_state"] != float64(1) || rows[0]["ts"] == nil {
		t.Fatalf("rows=%v", rows)
	}
	// 无 column_meta 时用固定列序
	rows = RowsToMaps(&tdengine.Result{Data: [][]any{{"t", float64(1)}}})
	if rows[0]["ts"] != "t" || rows[0]["seq"] != float64(1) {
		t.Fatalf("rows=%v", rows)
	}
	if rows := RowsToMaps(&tdengine.Result{}); rows == nil || len(rows) != 0 {
		t.Fatal("empty result should give empty (non-nil) slice")
	}
}
