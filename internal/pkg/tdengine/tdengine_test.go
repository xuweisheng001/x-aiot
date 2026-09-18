package tdengine

import (
	"strings"
	"testing"
)

func TestBuildTelemetryInsertsGroupsBySNAndOrders(t *testing.T) {
	rows := []TelemetryRow{
		{SN: "XT2", PK: "LM_S1", FW: "1.0", Region: "US", Cell: 1, Ts: 20, Seq: 2},
		{SN: "XT1", PK: "LM_S1", FW: "1.0", Region: "US", Cell: 1, Ts: 10, Seq: 1},
		{SN: "XT2", PK: "LM_S1", FW: "1.0", Region: "US", Cell: 1, Ts: 10, Seq: 1},
	}
	sqls := BuildTelemetryInserts(rows)
	if len(sqls) != 1 {
		t.Fatalf("want 1 sql, got %d", len(sqls))
	}
	s := sqls[0]
	if strings.Count(s, "USING iot.telemetry") != 2 {
		t.Fatalf("want 2 subtables: %s", s)
	}
	if strings.Index(s, "iot.t_xt1") > strings.Index(s, "iot.t_xt2") {
		t.Fatal("subtables not sorted")
	}
	i1, i2 := strings.Index(s, "(10,1,"), strings.Index(s, "(20,2,")
	if i1 < 0 || i2 < 0 || i1 > i2 {
		t.Fatalf("rows of XT2 not ordered by ts: %s", s)
	}
}

func TestBuildTelemetryInsertsSplitsLargeBatch(t *testing.T) {
	var rows []TelemetryRow
	for i := 0; i < 40000; i++ {
		rows = append(rows, TelemetryRow{SN: "SN" + strings.Repeat("X", 20) + string(rune('A'+i%26)) + string(rune('A'+(i/26)%26)), PK: "LM_S1", FW: "1.0", Region: "US", Ts: int64(i), Seq: int64(i)})
	}
	sqls := BuildTelemetryInserts(rows)
	if len(sqls) < 2 {
		t.Fatalf("expected split, got %d", len(sqls))
	}
	for _, s := range sqls {
		if len(s) > maxSQLBytes+64*1024 {
			t.Fatalf("sql too large: %d", len(s))
		}
		if !strings.HasPrefix(s, "INSERT INTO iot.t_") {
			t.Fatalf("bad prefix: %.40s", s)
		}
	}
}

func TestSanitize(t *testing.T) {
	if sanitize("XT-001.A") != "xt_001_a" {
		t.Fatal(sanitize("XT-001.A"))
	}
}
