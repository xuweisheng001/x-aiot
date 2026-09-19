package support

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

// 集成：打真实 PG（sql/bl6.sql 已建表）。IOT_IT 门控。
func itSetup(t *testing.T) (context.Context, *pgxpool.Pool, *Service, string, func()) {
	t.Helper()
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	db := config.MustPG(ctx)
	sn := fmt.Sprintf("ITSUP%d", time.Now().UnixNano()%1e9)
	if _, err := db.Exec(ctx, `INSERT INTO iot_shard.device(sn,product_key,region,cell_id,fw_version,status,activated_at) VALUES($1,'LM_S1','US',1,'1.0.0','activated',now()-interval '30 days')`, sn); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO iot_shard.device_binding(sn,user_id,role) VALUES($1,4242,'owner')`, sn); err != nil {
		t.Fatal(err)
	}
	svc := NewService(&PGStore{DB: db}, nil, nil, nil, NewMetrics())
	cleanup := func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_shard.cmd_ticket_map WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.agent_call_log WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.support_grant WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.diagnostic_bundle WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.warranty_case WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.device_binding WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.device WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_global.defect_alert WHERE product_key LIKE 'ITSUP%'`)
		_, _ = db.Exec(c, `DELETE FROM iot_global.error_code_dict WHERE code LIKE 'E_ITSUP%'`)
		db.Close()
		cancel()
	}
	return ctx, db, svc, sn, cleanup
}

// 授权全流程 + grantcheck 直查 PG：request → confirm → 有效；过期 → 拒绝；revoke → 拒绝；PG CHECK 拒绝非自检动作。
func TestIntegration_GrantAndGrantCheck(t *testing.T) {
	ctx, db, svc, sn, cleanup := itSetup(t)
	defer cleanup()
	now := time.Now()

	g, err := svc.RequestGrant(ctx, sn, "T-IT", "cs-it", nil)
	if err != nil {
		t.Fatal(err)
	}
	if d, _ := grantcheck.Check(ctx, db, sn, grantcheck.SourceSupport, grantcheck.ActionSelfCheck, now); d.OK {
		t.Fatal("requested grant must not pass grantcheck")
	}
	if _, err := svc.ApproveGrant(ctx, g.GrantID, 9999); !errors.Is(err, ErrDenied) {
		t.Fatalf("non-owner: %v", err)
	}
	g2, err := svc.ApproveGrant(ctx, g.GrantID, 4242)
	if err != nil || g2.Status != GrantGranted || g2.ExpiresAt == nil {
		t.Fatalf("approve: %v %+v", err, g2)
	}
	d, err := grantcheck.Check(ctx, db, sn, grantcheck.SourceSupport, grantcheck.ActionSelfCheck, now)
	if err != nil || !d.OK || d.GrantID != g.GrantID {
		t.Fatalf("grantcheck after approve: %+v %v", d, err)
	}
	if d, _ := grantcheck.Check(ctx, db, sn, grantcheck.SourceAgent, "pause", now); d.OK {
		t.Fatal("agent pause must be refused")
	}
	if d, _ := grantcheck.Check(ctx, db, sn, grantcheck.SourceAgent, grantcheck.ActionSelfCheck, now); !d.OK {
		t.Fatal("agent self_check within grant must pass")
	}
	// 过期：直接把 expires_at 改到过去
	if _, err := db.Exec(ctx, `UPDATE iot_shard.support_grant SET expires_at = now() - interval '1 second' WHERE grant_id=$1`, g.GrantID); err != nil {
		t.Fatal(err)
	}
	if d, _ := grantcheck.Check(ctx, db, sn, grantcheck.SourceSupport, grantcheck.ActionSelfCheck, time.Now()); d.OK {
		t.Fatal("expired grant must be refused")
	}
	// 撤回（granted → revoked 合法）
	if _, err := svc.RevokeGrant(ctx, g.GrantID, 4242); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := svc.RevokeGrant(ctx, g.GrantID, 4242); !errors.Is(err, ErrConflict) {
		t.Fatalf("double revoke must conflict: %v", err)
	}
	// 约束即护栏：actions 含非自检动作被 PG CHECK 拒绝
	_, err = db.Exec(ctx, `INSERT INTO iot_shard.support_grant(grant_id,sn,ticket_id,operator,actions) VALUES($1,$2,'T','op',ARRAY['pause'])`, NewID(), sn)
	if err == nil {
		t.Fatal("ck_grant_actions must reject pause")
	}
}

// 诊断包落库与回读；TDengine / deviceapi 未配置 → 对应源 unavailable + degraded 但仍生成。
func TestIntegration_BundleRoundTrip(t *testing.T) {
	ctx, db, svc, sn, cleanup := itSetup(t)
	defer cleanup()
	svc.SourceTimeoutOverride = 2 * time.Second
	b, err := svc.Generate(ctx, sn, "user", "T-IT", "note")
	if err != nil {
		t.Fatal(err)
	}
	if !b.Content.Degraded || b.Content.Sources[SrcDevice] != SourceOK || b.Content.ProductKey != "LM_S1" {
		t.Fatalf("bundle %+v", b.Content.Sources)
	}
	got, err := svc.GetBundle(ctx, b.BundleID)
	if err != nil || got.SN != sn || got.Content.ProductKey != "LM_S1" || got.TicketID != "T-IT" {
		t.Fatalf("round trip: %v %+v", err, got)
	}
	var raw []byte
	if err := db.QueryRow(ctx, `SELECT content FROM iot_shard.diagnostic_bundle WHERE bundle_id=$1`, b.BundleID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var v any
	_ = json.Unmarshal(raw, &v)
	keys := map[string]bool{}
	CollectKeys(v, keys)
	for k := range keys {
		if !AllowedKeys[k] {
			t.Fatalf("stored content has key outside whitelist: %s", k)
		}
	}
	list, err := svc.Store.ListBundles(ctx, sn, 10)
	if err != nil || len(list) != 1 || !list[0].Degraded {
		t.Fatalf("list %v %+v", err, list)
	}
	// 保修汇总落库且无判定键
	c, err := svc.GenerateWarranty(ctx, sn, "T-IT")
	if err != nil || len(c.Signals) != 4 {
		t.Fatalf("warranty %v %+v", err, c)
	}
}

// 缺陷告警：UpsertDefectAlert 同窗口幂等；LastDefectAlert 驱动冷却；字典双人审批由 PG 守。
func TestIntegration_DefectAlertAndDict(t *testing.T) {
	ctx, _, svc, _, cleanup := itSetup(t)
	defer cleanup()
	pk := fmt.Sprintf("ITSUP%d", time.Now().UnixNano()%1e6)
	now := time.Now()
	a := DefectAlert{ProductKey: pk, FWVersion: "1.0.0", ErrorCode: "E_ITSUP_X", DeviceCount: 25, EventCount: 40, WindowStart: now.UTC().Truncate(24 * time.Hour), CooldownUntil: now.Add(24 * time.Hour), CreatedAt: now}
	created, err := svc.Store.UpsertDefectAlert(ctx, a)
	if err != nil || !created {
		t.Fatalf("first upsert %v %v", created, err)
	}
	a.DeviceCount = 30
	created, err = svc.Store.UpsertDefectAlert(ctx, a)
	if err != nil || created {
		t.Fatalf("second upsert must not create: %v %v", created, err)
	}
	last, err := svc.Store.LastDefectAlert(ctx, pk, "1.0.0", "E_ITSUP_X")
	if err != nil || last == nil {
		t.Fatalf("last alert %v %v", last, err)
	}
	if ShouldAlert(50, 20, last, time.Now(), DefaultDefectCooldown) {
		t.Fatal("must be in cooldown")
	}
	thr, cd, err := svc.Store.DefectThreshold(ctx, pk, "E_ITSUP_X")
	if err != nil || thr != DefaultDefectThreshold || cd != DefaultDefectCooldown {
		t.Fatalf("threshold defaults %d %s %v", thr, cd, err)
	}
	n := 5
	if _, err := svc.PublishDict(ctx, DictEntry{Code: "E_ITSUP_X", Severity: "warn", Cause: "c", Steps: "s", DefectThreshold: &n, CreatedBy: "a", ApprovedBy: "b"}); err != nil {
		t.Fatal(err)
	}
	thr, _, _ = svc.Store.DefectThreshold(ctx, pk, "E_ITSUP_X")
	if thr != 5 {
		t.Fatalf("dict threshold must win: %d", thr)
	}
	// 直接绕过服务校验写同人审批 → PG ck_dict_approved 拒绝
	_, err = svc.Store.DictPublish(ctx, DictEntry{Code: "E_ITSUP_Y", Severity: "warn", Cause: "c", Steps: "s", CreatedBy: "a", ApprovedBy: "a", ReleasedAt: now})
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("ck_dict_approved must reject: %v", err)
	}
}
