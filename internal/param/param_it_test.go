package param

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

// 集成：打真实 PG（sql/bl4.sql 已建表）。IOT_IT 门控。
func itSetup(t *testing.T) (context.Context, *pgxpool.Pool, *Service, string, func()) {
	t.Helper()
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	db := config.MustPG(ctx)
	pk := fmt.Sprintf("ITP_%d", time.Now().UnixNano()%1e9)
	if _, err := db.Exec(ctx, `INSERT INTO iot_global.product(product_key,name,category) VALUES($1,$1,'laser_diode') ON CONFLICT DO NOTHING`, pk); err != nil {
		t.Fatal(err)
	}
	svc := NewService(&PGStore{DB: db}, NewMetrics(), t.TempDir())
	cleanup := func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_global.param_recommendation WHERE product_key=$1`, pk)
		_, _ = db.Exec(c, `DELETE FROM iot_global.param_release WHERE product_key=$1`, pk)
		_, _ = db.Exec(c, `DELETE FROM iot_global.param_profile WHERE product_key=$1`, pk)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.user_param WHERE product_key=$1`, pk)
		_, _ = db.Exec(c, `DELETE FROM iot_global.product WHERE product_key=$1`, pk)
		db.Close()
		cancel()
	}
	return ctx, db, svc, pk, cleanup
}

func addedReq(mod, mat string, power, speed float64) AddedReq {
	p, _ := json.Marshal(map[string]any{"power": power, "speed": speed})
	return AddedReq{ModuleModel: mod, MaterialID: mat, Params: p}
}

func TestIntegration_PublishDeltaRollback(t *testing.T) {
	ctx, db, svc, pk, cleanup := itSetup(t)
	defer cleanup()

	// v1
	r1, err := svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead", Note: "v1",
		Added: []AddedReq{addedReq("M40", "BASSWOOD_3MM", 60, 12), addedReq("M40", "ACRYLIC_3MM", 80, 8)}})
	if err != nil || r1.Version != 1 {
		t.Fatalf("v1: %v %+v", err, r1)
	}
	all, _ := svc.Store.Profiles(ctx, pk)
	if len(all) != 2 {
		t.Fatalf("want 2 profiles, got %d", len(all))
	}
	// v2：删第一条，加一条
	r2, err := svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead", Removed: []int64{all[0].ID},
		Added: []AddedReq{addedReq("M40", "LEATHER_1MM", 30, 20)}})
	if err != nil || r2.Version != 2 {
		t.Fatalf("v2: %v", err)
	}
	// latest 与快照文件
	lt, err := svc.Latest(ctx, pk, 0, false)
	if err != nil || lt.Version != 2 || lt.SnapshotURL != "/snapshots/"+pk+"/v2.json" {
		t.Fatalf("latest: %v %+v", err, lt)
	}
	b, err := svc.ReadSnapshot(pk, "v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var snap Snapshot
	_ = json.Unmarshal(b, &snap)
	if snap.Version != 2 || len(snap.Profiles) != 2 {
		t.Fatalf("snapshot: %+v", snap)
	}
	// 增量 1→2：removed 先、added 后
	d, err := svc.Delta(ctx, pk, 1, 0, false)
	if err != nil || len(d.Removed) != 1 || d.Removed[0] != all[0].ID || len(d.Added) != 1 || d.Added[0].MaterialID != "LEATHER_1MM" {
		t.Fatalf("delta: %v %+v", err, d)
	}
	// 回滚到 v1：有效集合按内容等价
	r3, err := svc.Rollback(ctx, pk, RollbackReq{ToVersion: 1, CreatedBy: "ops", ApprovedBy: "lead"})
	if err != nil || r3.Version != 3 || r3.RolledBackFrom == nil || *r3.RolledBackFrom != 1 {
		t.Fatalf("rollback: %v %+v", err, r3)
	}
	all, _ = svc.Store.Profiles(ctx, pk)
	v1, v3 := EffectiveAt(all, 1), EffectiveAt(all, 3)
	if len(v1) != 2 || len(v3) != 2 {
		t.Fatalf("v1=%d v3=%d", len(v1), len(v3))
	}
	set := func(ps []Profile) map[string]bool {
		m := map[string]bool{}
		for _, p := range ps {
			m[p.MaterialID+"|"+CanonicalHash(p.Params)] = true
		}
		return m
	}
	if fmt.Sprint(set(v1)) != fmt.Sprint(set(v3)) {
		t.Fatalf("rollback not equivalent: %v vs %v", set(v1), set(v3))
	}
	// force_full：落后 > 20 版本
	for i := 0; i < ForceFullBehind; i++ {
		if _, err := svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead"}); err != nil {
			t.Fatal(err)
		}
	}
	d, err = svc.Delta(ctx, pk, 1, 0, false)
	if err != nil || !d.ForceFull || d.SnapshotURL == "" {
		t.Fatalf("force_full expected: %v %+v", err, d)
	}
	// 数据库约束：ck_release_approved（created_by == approved_by）→ ErrDenied
	_, err = svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "ops"})
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("same approver should be denied by PG CHECK, got %v", err)
	}
	// ck_profile_params（power 150）→ ErrBadParam，且不留下 release 行
	var before int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM iot_global.param_release WHERE product_key=$1`, pk).Scan(&before)
	_, err = svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead", Added: []AddedReq{addedReq("M40", "BASSWOOD_3MM", 150, 1)}})
	if !errors.Is(err, ErrBadParam) {
		t.Fatalf("power 150 should be rejected by PG CHECK, got %v", err)
	}
	var after int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM iot_global.param_release WHERE product_key=$1`, pk).Scan(&after)
	if before != after {
		t.Fatalf("failed publish must not leave a release row: %d → %d", before, after)
	}
	// 未知材料（FK）→ ErrBadParam
	_, err = svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead", Added: []AddedReq{addedReq("M40", "NO_SUCH_MAT", 50, 1)}})
	if !errors.Is(err, ErrBadParam) {
		t.Fatalf("unknown material should be bad param, got %v", err)
	}
}

func TestIntegration_DiffValidationAndForce(t *testing.T) {
	ctx, _, svc, pk, cleanup := itSetup(t)
	defer cleanup()
	mats := []string{"BASSWOOD_3MM", "ACRYLIC_3MM", "LEATHER_1MM"}
	var seed []AddedReq
	for i := 0; i < 12; i++ { // 12 个 (module, material) 组合
		seed = append(seed, addedReq(fmt.Sprintf("M%02d", i), mats[i%3], 50, 10))
	}
	if _, err := svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead", Added: seed}); err != nil {
		t.Fatal(err)
	}
	var changed []AddedReq
	for i := 0; i < 12; i++ {
		changed = append(changed, addedReq(fmt.Sprintf("M%02d", i), mats[i%3], 80, 10))
	}
	_, err := svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead", Added: changed})
	var de *DiffError
	if !errors.As(err, &de) || len(de.Violations) != 12 {
		t.Fatalf("diff should reject with 12 violations, got %v", err)
	}
	if _, err := svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead", Added: changed, Force: true}); err != nil {
		t.Fatalf("force: %v", err)
	}
	if svc.M.Get(MDiffRejected) != 1 || svc.M.Get(MDiffForced) != 1 {
		t.Errorf("metrics rejected=%d forced=%d", svc.M.Get(MDiffRejected), svc.M.Get(MDiffForced))
	}
}

func TestIntegration_RolloutAndRecommendation(t *testing.T) {
	ctx, db, svc, pk, cleanup := itSetup(t)
	defer cleanup()
	if _, err := svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead"}); err != nil {
		t.Fatal(err)
	}
	// 一条已发布推荐候选：下一次发布应被写成 recommended 档
	params := json.RawMessage(`{"power":55,"speed":11}`)
	if _, err := db.Exec(ctx, `INSERT INTO iot_global.param_recommendation(product_key,module_model,material_id,params_hash,params,sample_count,distinct_users,good_ratio,status)
		VALUES($1,'M40','BASSWOOD_3MM',$2,$3,42,15,0.9,'published')`, pk, CanonicalHash(params), params); err != nil {
		t.Fatal(err)
	}
	r2, err := svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead", RolloutPct: 10})
	if err != nil || r2.RolloutPct != 10 {
		t.Fatalf("v2 10%%: %v", err)
	}
	all, _ := svc.Store.Profiles(ctx, pk)
	recCount := 0
	for _, p := range EffectiveAt(all, 2) {
		if p.Source == SourceRecommended && p.SampleCount == 42 {
			recCount++
		}
	}
	if recCount != 1 {
		t.Fatalf("want 1 recommended profile, got %d", recCount)
	}
	// 再发一次不重复写推荐
	if _, err := svc.Publish(ctx, PublishReq{ProductKey: pk, CreatedBy: "ops", ApprovedBy: "lead"}); err != nil {
		t.Fatal(err)
	}
	all, _ = svc.Store.Profiles(ctx, pk)
	recCount = 0
	for _, p := range EffectiveAt(all, 3) {
		if p.Source == SourceRecommended {
			recCount++
		}
	}
	if recCount != 1 {
		t.Fatalf("recommended should not duplicate, got %d", recCount)
	}
	// 灰度可见性：v2 只有 10%；v3 100%。用 bucket 判断（v3 已全量，故检查 v2 时先看 releases）
	rels, _ := svc.Store.Releases(ctx, pk)
	if rels[0].Version != 3 || rels[1].RolloutPct != 10 {
		t.Fatalf("releases: %+v", rels)
	}
	sub := []Release{rels[1], rels[2]} // v2(10%) 与 v1(100%)
	if r, _ := VisibleVersion(sub, 0.05, true); r.Version != 2 {
		t.Errorf("bucket 0.05 should see v2")
	}
	if r, _ := VisibleVersion(sub, 0.5, true); r.Version != 1 {
		t.Errorf("bucket 0.5 should see v1")
	}
	if lt, err := svc.Latest(ctx, pk, 0, false); err != nil || lt.Version != 3 {
		t.Errorf("latest without bucket after full rollout should be v3: %v %+v", err, lt)
	}
}

func TestIntegration_UserParamOptimisticLock(t *testing.T) {
	ctx, _, svc, pk, cleanup := itSetup(t)
	defer cleanup()
	up := UserParam{UserID: 900001, ProductKey: pk, ModuleModel: "M40", MaterialID: "BASSWOOD_3MM", Params: json.RawMessage(`{"power":42}`)}
	v, _, conflict, err := svc.PutUserParam(ctx, up, 0)
	if err != nil || conflict || v != 1 {
		t.Fatalf("create: v=%d conflict=%v err=%v", v, conflict, err)
	}
	_, cur, conflict, err := svc.PutUserParam(ctx, up, 0)
	if err != nil || !conflict || cur != 1 {
		t.Fatalf("dup create should conflict with current=1: %v %v %v", cur, conflict, err)
	}
	v, _, conflict, err = svc.PutUserParam(ctx, up, 1)
	if err != nil || conflict || v != 2 {
		t.Fatalf("update: %d %v %v", v, conflict, err)
	}
	_, cur, conflict, _ = svc.PutUserParam(ctx, up, 1)
	if !conflict || cur != 2 {
		t.Fatalf("stale update should conflict with current=2, got %d %v", cur, conflict)
	}
	list, err := svc.Store.UserParams(ctx, 900001, pk)
	if err != nil || len(list) != 1 || list[0].Version != 2 {
		t.Fatalf("list: %v %+v", err, list)
	}
}
