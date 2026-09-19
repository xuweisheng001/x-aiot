package tdengine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBuildEventInsertsNullableJobFields(t *testing.T) {
	sqls := BuildEventInserts([]EventRow{
		{SN: "S1", PK: "LM_S1", Ts: 1, Seq: 2, Code: "JOB_START", Msg: "m"},
		{SN: "S1", PK: "LM_S1", Ts: 3, Seq: 4, Code: "JOB_DONE", Msg: "m", JobID: "j", MaterialID: "BASSWOOD_3MM", ParamProfileID: "official-1", ParamsHash: "h"},
	})
	if len(sqls) != 1 {
		t.Fatalf("want 1 sql, got %d", len(sqls))
	}
	s := sqls[0]
	if !strings.Contains(s, "VALUES (1,2,'JOB_START','m',NULL,NULL,NULL,NULL)") {
		t.Errorf("empty fields must be NULL: %s", s)
	}
	if !strings.Contains(s, "VALUES (3,4,'JOB_DONE','m','j','BASSWOOD_3MM','official-1','h')") {
		t.Errorf("job fields must be quoted: %s", s)
	}
}

func TestSchemaErrorIsBenign(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, true},
		{errors.New("tdengine code 875: Column already exists"), true},
		{errors.New("tdengine code 9999: Table already exists"), true},
		{errors.New("tdengine code 9728: syntax error near x"), false},
		{errors.New("tdengine http: dial tcp: connection refused"), false},
	}
	for _, c := range cases {
		if got := SchemaErrorIsBenign(c.err); got != c.want {
			t.Errorf("SchemaErrorIsBenign(%v)=%v want %v", c.err, got, c.want)
		}
	}
}

// 集成：写带 v1.1 字段的事件并回查（IOT_IT 门控，TDengine 已升级 events 列）。
func TestInsertEventsJobFieldsIT(t *testing.T) {
	if os.Getenv("IOT_IT") == "" {
		t.Skip("IOT_IT not set")
	}
	url := os.Getenv("IOT_TD_URL")
	if url == "" {
		url = "http://127.0.0.1:6041"
	}
	c := New(url, "root", "taosdata")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// 确保列存在（幂等）
	for _, s := range []string{"ALTER STABLE iot.events ADD COLUMN job_id BINARY(36)", "ALTER STABLE iot.events ADD COLUMN material_id BINARY(32)",
		"ALTER STABLE iot.events ADD COLUMN param_profile_id BINARY(32)", "ALTER STABLE iot.events ADD COLUMN params_hash BINARY(64)"} {
		if err := c.Exec(ctx, s); !SchemaErrorIsBenign(err) {
			t.Fatal(err)
		}
	}
	sn := fmt.Sprintf("ITEV%d", time.Now().UnixNano()%1_000_000)
	ts := time.Now().UnixMilli()
	rows := []EventRow{
		{SN: sn, PK: "LM_S1", Ts: ts, Seq: 1, Code: "JOB_START", Msg: "a", JobID: "11111111-2222-4333-8444-555555555555", MaterialID: "BASSWOOD_3MM", ParamProfileID: "official-1", ParamsHash: "deadbeef"},
		{SN: sn, PK: "LM_S1", Ts: ts + 1, Seq: 2, Code: "HEARTBEAT", Msg: "b"},
	}
	if err := c.InsertEvents(ctx, rows); err != nil {
		t.Fatal(err)
	}
	r, err := c.Query(ctx, fmt.Sprintf("SELECT code, job_id, material_id, param_profile_id, params_hash FROM iot.events WHERE sn='%s' ORDER BY ts", sn))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Data) != 2 {
		t.Fatalf("rows=%d want 2: %+v", len(r.Data), r.Data)
	}
	if r.Data[0][1] != "11111111-2222-4333-8444-555555555555" || r.Data[0][2] != "BASSWOOD_3MM" || r.Data[0][3] != "official-1" || r.Data[0][4] != "deadbeef" {
		t.Errorf("job row mismatch: %v", r.Data[0])
	}
	for i := 1; i <= 4; i++ {
		if r.Data[1][i] != nil {
			t.Errorf("heartbeat col %d should be NULL, got %v", i, r.Data[1][i])
		}
	}
}
