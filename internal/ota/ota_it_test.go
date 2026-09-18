package ota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// 集成：建固件 → 圈选 100% → 下发到内存 publisher → 进度回流 → 重复终态不双计 → 熔断。
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
	svc.MinSamples = 4 // 让熔断在小样本可测

	tag := fmt.Sprintf("it%d", time.Now().UnixNano()%1e9)
	ver := "9.9." + tag
	// 60 台待升级设备
	var sns []string
	for i := 0; i < 60; i++ {
		sn := fmt.Sprintf("ITOTA_%s_%03d", tag, i)
		sns = append(sns, sn)
		if _, err := db.Exec(ctx, `INSERT INTO iot_shard.device(sn,product_key,region,cell_id,fw_version) VALUES($1,'LM_S1','US',1,'1.0.0')`, sn); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_shard.ota_device_task WHERE sn LIKE $1`, "ITOTA_"+tag+"%")
		_, _ = db.Exec(c, `DELETE FROM iot_global.ota_batch WHERE firmware_id IN (SELECT id FROM iot_global.firmware WHERE version=$1)`, ver)
		_, _ = db.Exec(c, `DELETE FROM iot_global.firmware WHERE version=$1`, ver)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.device WHERE sn LIKE $1`, "ITOTA_"+tag+"%")
	}()

	fwID, err := svc.CreateFirmware(ctx, FirmwareReq{ProductKey: "LM_S1", Version: ver, FullURL: "https://cdn/x.bin", FullSize: 1024,
		SHA256: strings.Repeat("ab", 32), Signature: "sig"})
	if err != nil {
		t.Fatal(err)
	}
	// 100 档无审批 → CHECK 拒绝
	if _, err := svc.CreateBatch(ctx, BatchReq{FirmwareID: fwID, Stage: "100", CreatedBy: "a"}); !errors.Is(err, ErrNeedsApproval) {
		t.Fatalf("expected ErrNeedsApproval, got %v", err)
	}
	thr := 0.5
	b, err := svc.CreateBatch(ctx, BatchReq{FirmwareID: fwID, Stage: "100", CreatedBy: "a", ApprovedBy: "b", FailRatioFuse: &thr})
	if err != nil {
		t.Fatal(err)
	}
	if b.TargetTotal != 60 {
		t.Fatalf("target_total=%d", b.TargetTotal)
	}
	n, err := svc.DispatchOnce(ctx)
	if err != nil || n != 60 || len(pub.Msgs) != 60 {
		t.Fatalf("dispatch n=%d msgs=%d err=%v", n, len(pub.Msgs), err)
	}
	if !strings.HasPrefix(pub.Msgs[0].Topic, "down/") || !strings.Contains(pub.Msgs[0].Payload, `"action":"ota"`) {
		t.Fatalf("bad publish: %+v", pub.Msgs[0])
	}
	// 再来一轮：没有 pending 了
	if n, _ := svc.DispatchOnce(ctx); n != 0 {
		t.Fatalf("second dispatch should be 0, got %d", n)
	}

	report := func(sn, phase string) ProgressOutcome {
		pl, _ := json.Marshal(map[string]any{"seq": 1, "ts": time.Now().UnixMilli(), "batch_id": b.ID, "phase": phase, "pct": 100})
		out, err := svc.HandleProgress(ctx, &envelope.Envelope{PK: "LM_S1", SN: sn, Kind: envelope.KindOTAProgress, Payload: pl})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
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
	if out := report(sns[3], "failed"); out != ProgressFused {
		t.Fatalf("expected fuse, got %v", out)
	}
	v, _ = svc.GetBatchView(ctx, b.ID)
	if v.Status != BatchFused || v.FusedAt == nil {
		t.Fatalf("status=%s", v.Status)
	}
	// fused 无自动恢复；pause 也不行；只有 resume
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
