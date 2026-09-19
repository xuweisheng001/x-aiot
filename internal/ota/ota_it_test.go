package ota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// itFixture：60 台待升级设备 + 一个已登记固件；返回清理函数。
func itFixture(t *testing.T, ctx context.Context, db *pgxpool.Pool, svc *Service) (fwID int64, tag string, cleanup func()) {
	t.Helper()
	tag = fmt.Sprintf("it%d", time.Now().UnixNano()%1e9)
	ver := "9.9." + tag
	// 独立 product_key：圈选按 product_key 过滤，用共享的 LM_S1 会把冒烟/模拟器留下的设备一起圈进来（测试隔离）
	pk := "ITPK_" + tag
	if _, err := db.Exec(ctx, `INSERT INTO iot_global.product(product_key,name,category) VALUES($1,'it product','laser_diode')`, pk); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		sn := fmt.Sprintf("ITOTA_%s_%03d", tag, i)
		if _, err := db.Exec(ctx, `INSERT INTO iot_shard.device(sn,product_key,region,cell_id,fw_version) VALUES($1,$2,'US',1,'1.0.0')`, sn, pk); err != nil {
			t.Fatal(err)
		}
	}
	cleanup = func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_shard.ota_device_task WHERE sn LIKE $1`, "ITOTA_"+tag+"%")
		_, _ = db.Exec(c, `DELETE FROM iot_global.ota_batch WHERE firmware_id IN (SELECT id FROM iot_global.firmware WHERE version=$1)`, ver)
		_, _ = db.Exec(c, `DELETE FROM iot_global.firmware WHERE version=$1`, ver)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.device WHERE sn LIKE $1`, "ITOTA_"+tag+"%")
		_, _ = db.Exec(c, `DELETE FROM iot_global.product WHERE product_key=$1`, pk)
	}
	var err error
	fwID, err = svc.CreateFirmware(ctx, FirmwareReq{ProductKey: pk, Version: ver, FullURL: "https://cdn/x.bin", FullSize: 1024,
		SHA256: strings.Repeat("ab", 32), Signature: "sig"})
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	return fwID, tag, cleanup
}

func batchSNs(t *testing.T, ctx context.Context, db *pgxpool.Pool, batchID int64) []string {
	t.Helper()
	rows, err := db.Query(ctx, `SELECT sn FROM iot_shard.ota_device_task WHERE batch_id=$1 ORDER BY sn`, batchID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var sn string
		_ = rows.Scan(&sn)
		out = append(out, sn)
	}
	return out
}

func reportFn(t *testing.T, ctx context.Context, svc *Service, batchID int64) func(sn, phase string) ProgressOutcome {
	return func(sn, phase string) ProgressOutcome {
		pl, _ := json.Marshal(map[string]any{"seq": 1, "ts": time.Now().UnixMilli(), "batch_id": batchID, "phase": phase, "pct": 100})
		out, err := svc.HandleProgress(ctx, &envelope.Envelope{PK: "LM_S1", SN: sn, Kind: envelope.KindOTAProgress, Payload: pl})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
}

// 逐档推进到 100：每档下发、全部上报 success 后 Advance。返回 100 档批次。
func advanceTo100(t *testing.T, ctx context.Context, db *pgxpool.Pool, svc *Service, fwID int64, thr float64) *Batch {
	t.Helper()
	b, err := svc.CreateBatch(ctx, BatchReq{FirmwareID: fwID, Stage: FirstStage, CreatedBy: "a", FailRatioFuse: &thr})
	if err != nil {
		t.Fatal(err)
	}
	for b.Stage != "100" {
		if _, err := svc.DispatchOnce(ctx); err != nil {
			t.Fatal(err)
		}
		rep := reportFn(t, ctx, svc, b.ID)
		for _, sn := range batchSNs(t, ctx, db, b.ID) {
			rep(sn, TaskSuccess)
		}
		next, _ := NextStage(b.Stage)
		approved := ""
		if next == "100" {
			// 全量档无审批 → CHECK 拒绝（约束即护栏，代码不重复判断）
			if _, err := svc.Advance(ctx, b.ID, "a", ""); !errors.Is(err, ErrNeedsApproval) {
				t.Fatalf("advance to 100 without approval: want ErrNeedsApproval, got %v", err)
			}
			approved = "b"
		}
		nb, err := svc.Advance(ctx, b.ID, "a", approved)
		if err != nil {
			t.Fatalf("advance from %s: %v", b.Stage, err)
		}
		b = nb
	}
	return b
}

// 集成：档位顺序 → 逐档推进 → 100 档下发 → 进度回流 → 重复终态不双计 → 比例熔断 → 只有 resume 能恢复。
func TestIntegration_BatchLifecycle(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	pub := &MemPublisher{}
	svc := NewService(db, pub, NewMetrics(), 1000)
	svc.MinSamples = 4 // 让比例熔断在小样本可测
	svc.MinAbsFail = 0 // 本用例只验证比例规则

	fwID, _, cleanup := itFixture(t, ctx, db, svc)
	defer cleanup()

	// 越级：没有任何批次时直接建 100 / 1 都被拒（ErrConflict，不是 CHECK 的 ErrNeedsApproval）
	for _, st := range []string{"100", "1", "50"} {
		if _, err := svc.CreateBatch(ctx, BatchReq{FirmwareID: fwID, Stage: st, CreatedBy: "a", ApprovedBy: "b"}); !errors.Is(err, ErrConflict) {
			t.Fatalf("first batch stage %s: want ErrConflict, got %v", st, err)
		}
	}
	thr := 0.5
	b := advanceTo100(t, ctx, db, svc, fwID, thr)
	// 100 档再建任何档位都不行
	if _, err := svc.CreateBatch(ctx, BatchReq{FirmwareID: fwID, Stage: "100", CreatedBy: "a", ApprovedBy: "b"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("after stage 100: want ErrConflict, got %v", err)
	}
	// 五档累计圈选 = 全部 60 台
	var total int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.ota_device_task t JOIN iot_global.ota_batch b ON b.id=t.batch_id WHERE b.firmware_id=$1`, fwID).Scan(&total); err != nil || total != 60 {
		t.Fatalf("cumulative targets=%d err=%v", total, err)
	}
	sns := batchSNs(t, ctx, db, b.ID)
	if len(sns) < 5 {
		t.Fatalf("stage-100 batch too small for the test: %d", len(sns))
	}
	before := len(pub.Msgs)
	n, err := svc.DispatchOnce(ctx)
	if err != nil || n != len(sns) || len(pub.Msgs)-before != len(sns) {
		t.Fatalf("dispatch n=%d msgs=%d want %d err=%v", n, len(pub.Msgs)-before, len(sns), err)
	}
	if !strings.HasPrefix(pub.Msgs[before].Topic, "down/") || !strings.Contains(pub.Msgs[before].Payload, `"action":"ota"`) {
		t.Fatalf("bad publish: %+v", pub.Msgs[before])
	}
	if n, _ := svc.DispatchOnce(ctx); n != 0 {
		t.Fatalf("second dispatch should be 0, got %d", n)
	}

	report := reportFn(t, ctx, svc, b.ID)
	if out := report(sns[0], "downloading"); out != ProgressUpdated {
		t.Fatalf("downloading: %v", out)
	}
	if out := report(sns[0], "success"); out != ProgressTerminal {
		t.Fatalf("success: %v", out)
	}
	if out := report(sns[0], "success"); out != ProgressIgnored {
		t.Fatalf("duplicate success must be ignored: %v", out)
	}
	if out := report(sns[0], "failed"); out != ProgressIgnored {
		t.Fatalf("terminal→terminal must be ignored: %v", out)
	}
	v, _ := svc.GetBatchView(ctx, b.ID)
	if v.OkCount != 1 || v.FailCount != 0 {
		t.Fatalf("counts ok=%d fail=%d", v.OkCount, v.FailCount)
	}
	// 3 台失败：样本 4，失败率 0.75 > 0.5 → 熔断（第 4 个样本触发）
	report(sns[1], "failed")
	report(sns[2], "failed")
	if out := report(sns[3], "rolled_back"); out != ProgressFused {
		t.Fatalf("expected fuse, got %v", out)
	}
	v, _ = svc.GetBatchView(ctx, b.ID)
	if v.Status != BatchFused || v.FusedAt == nil || v.FailCount != 3 {
		t.Fatalf("status=%s fail=%d", v.Status, v.FailCount)
	}
	if err := svc.Pause(ctx, b.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("pause fused should conflict: %v", err)
	}
	if err := svc.Resume(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	v, _ = svc.GetBatchView(ctx, b.ID)
	if v.Status != BatchRunning {
		t.Fatalf("after resume status=%s", v.Status)
	}
}

// 集成：绝对数熔断——样本远低于 MinSamples，fail 达到 MinAbsFail 即 fused。
func TestIntegration_AbsFuse(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	svc := NewService(db, &MemPublisher{}, NewMetrics(), 1000)
	svc.MinSamples = 1000 // 比例规则永不触发
	svc.MinAbsFail = 3

	fwID, _, cleanup := itFixture(t, ctx, db, svc)
	defer cleanup()
	b := advanceTo100(t, ctx, db, svc, fwID, 0.5)
	if _, err := svc.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	sns := batchSNs(t, ctx, db, b.ID)
	report := reportFn(t, ctx, svc, b.ID)
	report(sns[0], "failed")
	if out := report(sns[1], "failed"); out != ProgressTerminal {
		t.Fatalf("2 fails must not fuse: %v", out)
	}
	if out := report(sns[2], "failed"); out != ProgressFused {
		t.Fatalf("3rd fail must fuse by absolute count: %v", out)
	}
}

// 集成：stale sweeper——下发后停在 notified 超过阈值的任务被置 failed/STALE_TIMEOUT 并计入批次失败与熔断；
// 刚刚更新过的任务不受影响；paused 批次任务置失败但仍计数、不熔断；重复 sweep 幂等。
func TestIntegration_StaleSweep(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	svc := NewService(db, &MemPublisher{}, NewMetrics(), 1000)
	svc.MinSamples = 1000
	svc.MinAbsFail = 3

	fwID, _, cleanup := itFixture(t, ctx, db, svc)
	defer cleanup()
	b := advanceTo100(t, ctx, db, svc, fwID, 0.5)
	if _, err := svc.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	sns := batchSNs(t, ctx, db, b.ID)
	if len(sns) < 4 {
		t.Fatalf("need >= 4 tasks, got %d", len(sns))
	}
	// 全部任务都 notified；把前 3 台的 updated_at 拨回 1 小时前，其余保持"新鲜"
	if _, err := db.Exec(ctx, `UPDATE iot_shard.ota_device_task SET updated_at = now() - interval '1 hour' WHERE batch_id=$1 AND sn = ANY($2)`, b.ID, sns[:3]); err != nil {
		t.Fatal(err)
	}
	n, err := svc.SweepStale(ctx, 30*time.Minute)
	if err != nil || n != 3 {
		t.Fatalf("sweep n=%d err=%v", n, err)
	}
	var st, ec string
	var retry int
	if err := db.QueryRow(ctx, `SELECT status, coalesce(error_code,''), retry FROM iot_shard.ota_device_task WHERE batch_id=$1 AND sn=$2`, b.ID, sns[0]).Scan(&st, &ec, &retry); err != nil {
		t.Fatal(err)
	}
	if st != TaskFailed || ec != StaleErrorCode || retry != 0 {
		t.Fatalf("stale task status=%s error_code=%s retry=%d", st, ec, retry)
	}
	if err := db.QueryRow(ctx, `SELECT status FROM iot_shard.ota_device_task WHERE batch_id=$1 AND sn=$2`, b.ID, sns[3]).Scan(&st); err != nil || st != TaskNotified {
		t.Fatalf("fresh task must stay notified, got %s err=%v", st, err)
	}
	v, _ := svc.GetBatchView(ctx, b.ID)
	if v.FailCount != 3 || v.Status != BatchFused {
		t.Fatalf("after sweep fail=%d status=%s (3 stale fails must fuse by abs)", v.FailCount, v.Status)
	}
	if svc.M.Get("stale_failed") != 3 {
		t.Fatalf("stale_failed=%d", svc.M.Get("stale_failed"))
	}
	// 幂等：再扫一遍没有新任务
	if n, err := svc.SweepStale(ctx, 30*time.Minute); err != nil || n != 0 {
		t.Fatalf("second sweep n=%d err=%v", n, err)
	}
	// fused 批次里再有 stale：任务落 failed 但不计数
	if _, err := db.Exec(ctx, `UPDATE iot_shard.ota_device_task SET updated_at = now() - interval '1 hour' WHERE batch_id=$1 AND sn=$2`, b.ID, sns[3]); err != nil {
		t.Fatal(err)
	}
	if n, err := svc.SweepStale(ctx, 30*time.Minute); err != nil || n != 1 {
		t.Fatalf("third sweep n=%d err=%v", n, err)
	}
	v, _ = svc.GetBatchView(ctx, b.ID)
	if v.FailCount != 3 || svc.M.Get("stale_terminal_uncounted") != 1 {
		t.Fatalf("fused batch must not count: fail=%d uncounted=%d", v.FailCount, svc.M.Get("stale_terminal_uncounted"))
	}
	// olderThan<=0 关闭
	if n, err := svc.SweepStale(ctx, 0); err != nil || n != 0 {
		t.Fatalf("disabled sweep n=%d err=%v", n, err)
	}
}
