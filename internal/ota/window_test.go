package ota

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

// wat 构造某时区的一个时刻（表驱动用例的可读入口）。
func wat(tz string, y, mo, d, h, mi int) time.Time {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		panic(err)
	}
	return time.Date(y, time.Month(mo), d, h, mi, 0, 0, loc)
}

// 维护窗口判定：零值、普通窗口、跨午夜、周几（含跨午夜按起始日判周）、无效时区回退 UTC、边界分钟。
func TestInWindow(t *testing.T) {
	night := Window{Start: "22:00", End: "06:00", TZ: "Asia/Shanghai"}
	day := Window{Start: "01:00", End: "05:00", TZ: "America/Los_Angeles"}
	// 2026-09-21 是周一（1）；跨午夜窗口只在周一、周二夜里开
	nightMon := Window{Start: "22:00", End: "06:00", TZ: "Asia/Shanghai", Weekdays: []int{1, 2}}
	weekend := Window{Start: "08:00", End: "12:00", TZ: "UTC", Weekdays: []int{0, 6}}
	badTZ := Window{Start: "10:00", End: "11:00", TZ: "Mars/Olympus"}
	allDay := Window{Start: "00:00", End: "00:00", TZ: "UTC"}

	cases := []struct {
		name string
		w    Window
		now  time.Time
		want bool
	}{
		{"零值窗口不限制", Window{}, wat("UTC", 2026, 9, 21, 3, 0), true},
		{"只有 tz 的零值窗口不限制", Window{TZ: "Asia/Shanghai"}, wat("UTC", 2026, 9, 21, 3, 0), true},
		{"普通窗口内", day, wat("America/Los_Angeles", 2026, 9, 21, 2, 0), true},
		{"普通窗口起点含头", day, wat("America/Los_Angeles", 2026, 9, 21, 1, 0), true},
		{"普通窗口终点不含尾", day, wat("America/Los_Angeles", 2026, 9, 21, 5, 0), false},
		{"普通窗口终点前一分钟", day, wat("America/Los_Angeles", 2026, 9, 21, 4, 59), true},
		{"普通窗口起点前一分钟", day, wat("America/Los_Angeles", 2026, 9, 21, 0, 59), false},
		{"按窗口时区解释而非 now 的时区", day, wat("UTC", 2026, 9, 21, 9, 0), true}, // UTC 09:00 = LA 02:00
		{"跨午夜：当日部分", night, wat("Asia/Shanghai", 2026, 9, 21, 23, 30), true},
		{"跨午夜：次日部分", night, wat("Asia/Shanghai", 2026, 9, 22, 5, 59), true},
		{"跨午夜：起点含头", night, wat("Asia/Shanghai", 2026, 9, 21, 22, 0), true},
		{"跨午夜：终点不含尾", night, wat("Asia/Shanghai", 2026, 9, 22, 6, 0), false},
		{"跨午夜：白天不在窗口", night, wat("Asia/Shanghai", 2026, 9, 21, 12, 0), false},
		{"跨午夜 + 周几：周一夜里（起始日周一）", nightMon, wat("Asia/Shanghai", 2026, 9, 21, 23, 0), true},
		{"跨午夜 + 周几：周二凌晨算周一的窗口", nightMon, wat("Asia/Shanghai", 2026, 9, 22, 2, 0), true},
		{"跨午夜 + 周几：周三凌晨算周二的窗口", nightMon, wat("Asia/Shanghai", 2026, 9, 23, 2, 0), true},
		{"跨午夜 + 周几：周四凌晨（起始日周三）不在名单", nightMon, wat("Asia/Shanghai", 2026, 9, 24, 2, 0), false},
		{"跨午夜 + 周几：周三夜里（起始日周三）不在名单", nightMon, wat("Asia/Shanghai", 2026, 9, 23, 23, 0), false},
		{"周几：周日在名单", weekend, wat("UTC", 2026, 9, 20, 9, 0), true},
		{"周几：周六在名单", weekend, wat("UTC", 2026, 9, 26, 9, 0), true},
		{"周几：周一不在名单", weekend, wat("UTC", 2026, 9, 21, 9, 0), false},
		{"周几命中但时刻不在窗口", weekend, wat("UTC", 2026, 9, 26, 13, 0), false},
		{"无效时区回退 UTC：窗口内", badTZ, wat("UTC", 2026, 9, 21, 10, 30), true},
		{"无效时区回退 UTC：窗口外", badTZ, wat("UTC", 2026, 9, 21, 12, 0), false},
		{"start==end 视为全天", allDay, wat("UTC", 2026, 9, 21, 17, 45), true},
		{"非法时刻不阻塞下发", Window{Start: "9:00", End: "17:00"}, wat("UTC", 2026, 9, 21, 22, 0), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := InWindow(c.w, c.now); got != c.want {
				t.Errorf("InWindow(%+v, %s)=%v want %v", c.w, c.now.Format(time.RFC3339), got, c.want)
			}
		})
	}
}

func TestParseWindow(t *testing.T) {
	ok := []struct {
		name string
		raw  string
		want Window
	}{
		{"空 policy", "", Window{}},
		{"null", "null", Window{}},
		{"无 window 键", `{"idle_only":true}`, Window{}},
		{"完整窗口", `{"idle_only":true,"window":{"start":"22:00","end":"06:00","tz":"Asia/Shanghai","weekdays":[1,2]}}`,
			Window{Start: "22:00", End: "06:00", TZ: "Asia/Shanghai", Weekdays: []int{1, 2}}},
		{"无 tz", `{"window":{"start":"01:00","end":"05:00"}}`, Window{Start: "01:00", End: "05:00"}},
		{"空窗口对象", `{"window":{}}`, Window{}},
		{"只给 weekdays 视为不限时刻", `{"window":{"weekdays":[0]}}`, Window{Weekdays: []int{0}}},
	}
	for _, c := range ok {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseWindow(json.RawMessage(c.raw))
			if err != nil {
				t.Fatalf("ParseWindow(%s): %v", c.raw, err)
			}
			if got.Start != c.want.Start || got.End != c.want.End || got.TZ != c.want.TZ || len(got.Weekdays) != len(c.want.Weekdays) {
				t.Fatalf("ParseWindow(%s)=%+v want %+v", c.raw, got, c.want)
			}
		})
	}
	bad := map[string]string{
		"坏 json":    `{"window":`,
		"坏 start":   `{"window":{"start":"9:00","end":"17:00"}}`,
		"坏 end":     `{"window":{"start":"09:00","end":"25:00"}}`,
		"只给 start":  `{"window":{"start":"09:00"}}`,
		"只给 end":    `{"window":{"end":"09:00"}}`,
		"坏 weekday": `{"window":{"start":"09:00","end":"17:00","weekdays":[7]}}`,
		"负 weekday": `{"window":{"start":"09:00","end":"17:00","weekdays":[-1]}}`,
		"坏 tz":      `{"window":{"start":"09:00","end":"17:00","tz":"Mars/Olympus"}}`,
		"非数字 HH:MM": `{"window":{"start":"ab:cd","end":"17:00"}}`,
	}
	for name, raw := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseWindow(json.RawMessage(raw)); err == nil {
				t.Fatalf("ParseWindow(%s) must fail", raw)
			}
		})
	}
}

func TestDedupSNs(t *testing.T) {
	got := dedupSNs([]string{"b", "a", "b", "", "a", "c"})
	if fmt.Sprint(got) != "[b a c]" {
		t.Fatalf("dedupSNs=%v", got)
	}
	if len(dedupSNs(nil)) != 0 {
		t.Fatal("nil input")
	}
}

// 集成（需 PG）：子批次 explicit_sns 分支——只圈显式 SN、不存在的忽略并计数、跳过档位顺序链、
// 熔断阈值强制继承父批次、维护窗口外 DispatchOnce 跳过。
func TestIT_SubBatchExplicitSNs(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	svc := NewService(db, &MemPublisher{}, NewMetrics(), 1000)
	fwID, tag, cleanup := itFixture(t, ctx, db, svc)
	defer cleanup()

	// 父批次：平台 0.1 档（走正常抽样圈选）
	thr := 0.05
	parent, err := svc.CreateBatch(ctx, BatchReq{FirmwareID: fwID, Stage: FirstStage, CreatedBy: "platform", FailRatioFuse: &thr})
	if err != nil {
		t.Fatalf("parent batch: %v", err)
	}

	// 子批次：显式 3 台真实 SN + 2 个不存在的 SN，请求里的宽松阈值必须被忽略
	loose := 0.9
	sns := []string{
		fmt.Sprintf("ITOTA_%s_%03d", tag, 50),
		fmt.Sprintf("ITOTA_%s_%03d", tag, 51),
		fmt.Sprintf("ITOTA_%s_%03d", tag, 52),
		"NOSUCHSN_1", "NOSUCHSN_2",
	}
	missBefore := svc.M.Get("explicit_sns_unknown")
	// 窗口：只在周日 03:00–04:00 UTC 开——测试时钟固定在窗口外
	policy := json.RawMessage(`{"idle_only":true,"window":{"start":"03:00","end":"04:00","tz":"UTC","weekdays":[0]}}`)
	sub, err := svc.CreateBatch(ctx, BatchReq{FirmwareID: fwID, CreatedBy: "org:1", ExplicitSNs: sns,
		ParentBatchID: &parent.ID, Policy: policy, FailRatioFuse: &loose})
	if err != nil {
		t.Fatalf("sub batch: %v", err)
	}
	if sub.ParentBatchID == nil || *sub.ParentBatchID != parent.ID {
		t.Fatalf("parent_batch_id not persisted: %+v", sub)
	}
	if sub.Stage != parent.Stage {
		t.Fatalf("sub batch stage %q must inherit parent %q", sub.Stage, parent.Stage)
	}
	if sub.FailRatioFuse != parent.FailRatioFuse {
		t.Fatalf("fuse threshold %v must inherit parent %v (request asked for %v)", sub.FailRatioFuse, parent.FailRatioFuse, loose)
	}
	if sub.TargetTotal != 3 {
		t.Fatalf("target_total=%d want 3 (only existing SNs)", sub.TargetTotal)
	}
	if got := svc.M.Get("explicit_sns_unknown") - missBefore; got != 2 {
		t.Fatalf("explicit_sns_unknown +%d want +2", got)
	}
	got := batchSNs(t, ctx, db, sub.ID)
	if len(got) != 3 || got[0] != sns[0] {
		t.Fatalf("sub batch SNs=%v", got)
	}

	// 平台链不受子批次影响：下一档仍然是 1（若子批次入链，这里会被要求 0.1 的下一档之外的东西）
	if _, err := svc.CreateBatch(ctx, BatchReq{FirmwareID: fwID, Stage: "1", CreatedBy: "platform"}); err != nil {
		t.Fatalf("platform stage 1 after sub-batch: %v", err)
	}

	// 维护窗口：窗口外整批跳过并计数
	svc.Now = func() time.Time { return time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC) } // 周一中午，窗口外
	before := svc.M.Get("window_skipped")
	if _, err := svc.DispatchOnce(ctx); err != nil {
		t.Fatalf("dispatch outside window: %v", err)
	}
	if svc.M.Get("window_skipped")-before < 1 {
		t.Fatal("window_skipped not counted")
	}
	if pending := taskStatusCount(t, ctx, db, sub.ID, "pending"); pending != 3 {
		t.Fatalf("tasks dispatched outside window: pending=%d want 3", pending)
	}

	// 窗口内：正常下发
	svc.Now = func() time.Time { return time.Date(2026, 9, 20, 3, 30, 0, 0, time.UTC) } // 周日 03:30 UTC
	if _, err := svc.DispatchOnce(ctx); err != nil {
		t.Fatalf("dispatch inside window: %v", err)
	}
	if pending := taskStatusCount(t, ctx, db, sub.ID, "pending"); pending != 0 {
		t.Fatalf("tasks still pending inside window: %d", pending)
	}
}

func taskStatusCount(t *testing.T, ctx context.Context, db *pgxpool.Pool, batchID int64, status string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.ota_device_task WHERE batch_id=$1 AND status=$2`, batchID, status).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
