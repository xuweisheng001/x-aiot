package health

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

// itSource 用内存桶喂批计算：TDengine 的 telemetry_1h 是 WINDOW_CLOSE 流，
// 要等整点窗口关闭才有行，集成测试不能依赖它产出数据，因此这里只打真实 PG。
type itSource struct {
	buckets  []Bucket
	overtemp map[string]int
}

func (s *itSource) Buckets(context.Context, time.Time) ([]Bucket, error) { return s.buckets, nil }
func (s *itSource) OvertempCounts(context.Context, time.Time) (map[string]int, error) {
	return s.overtemp, nil
}

type itShadow struct{ m map[string]Reported }

func (s *itShadow) Reported(_ context.Context, sn string) (Reported, error) {
	if r, ok := s.m[sn]; ok {
		return r, nil
	}
	return Reported{}, nil
}

func itDB(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx := context.Background()
	db := config.MustPG(ctx)
	t.Cleanup(db.Close)
	return db, ctx
}

func fptr(v float64) *float64 { return &v }

// 造 n 个小时桶：laser_hours 每桶 +delta，功率全在高档。
func hourlyBuckets(sn, pk string, start time.Time, n int, base, delta float64) []Bucket {
	out := make([]Bucket, 0, n)
	for i := 0; i < n; i++ {
		h := base + delta*float64(i)
		out = append(out, Bucket{SN: sn, ProductKey: pk, Ts: start.Add(time.Duration(i) * time.Hour),
			LaserHours: fptr(h), AvgPower: fptr(90), ShareLow: fptr(0), ShareMid: fptr(0), ShareHigh: fptr(1)})
	}
	return out
}

// 正向路径：有模块型号 + 有桶 → 打分落 consumable_health，并且跨档触发提醒后进入冷却。
func TestIntegration_ScoreAndRemind(t *testing.T) {
	db, ctx := itDB(t)
	tag := fmt.Sprintf("%d", time.Now().UnixNano()%1e9)
	sn := "ITH_" + tag
	t.Cleanup(func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_shard.health_reminder WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.health_explain WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.module_swap WHERE sn=$1`, sn)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.consumable_health WHERE sn=$1`, sn)
	})

	// 独立机型 + 小额定值：每个小时桶最多记 MaxBucketHours(1h) × w_high(1.8) 加权小时，
	// 用默认 rated=10000 需要上千个桶才跨档，测试用 rated=200 让跨档在几十个桶内发生。
	pk, module := "ITPK_"+tag, "ITLM_"+tag
	if _, err := db.Exec(ctx, `INSERT INTO iot_global.product(product_key,name,category) VALUES($1,'it health','laser_diode')`, pk); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO iot_global.health_model_cfg(product_key,module_model,rated_weighted_hours,approved_by) VALUES($1,$2,200,'it')`, pk, module); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_global.health_model_cfg WHERE product_key=$1`, pk)
		_, _ = db.Exec(c, `DELETE FROM iot_global.product WHERE product_key=$1`, pk)
	})

	base := time.Now().UTC().Truncate(time.Hour).Add(-48 * time.Hour)
	// 30 桶 → 29 个增量，每个记 1×1.8 = 52.2 加权小时；rated 200 → health ≈ 100−26.1−2 = 71.9
	src := &itSource{buckets: hourlyBuckets(sn, pk, base, 30, 100, 1), overtemp: map[string]int{sn: 4}}
	sh := &itShadow{m: map[string]Reported{sn: {Found: true, ModuleModel: module, WorkState: 0}}}
	svc := NewService(&PGStore{DB: db}, src, sh, nil, NewMetrics(), Options{MaterialKey: []byte("it")})

	rep, err := svc.RunOnce(ctx, sn)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Fused || rep.Scored != 1 || rep.Skipped != 0 {
		t.Fatalf("first run: %+v", rep)
	}
	var health, used float64
	if err := db.QueryRow(ctx, `SELECT health, used_hours FROM iot_shard.consumable_health WHERE sn=$1 AND part='module'`, sn).Scan(&health, &used); err != nil {
		t.Fatal(err)
	}
	// 29 个增量 × 1h × 1.8 = 52.2 加权小时；扣 4 次 OVER_TEMP × 0.5 = 2 分
	if used < 52 || used > 53 {
		t.Fatalf("weighted hours=%v want ~52.2", used)
	}
	if health < 70 || health > 74 {
		t.Fatalf("health=%v want ~71.9 (100 − 52.2/200×100 − 2)", health)
	}
	// 首轮就跨过 80 档 → 发一条提醒
	if rep.Reminders != 1 {
		t.Fatalf("crossing the 80 level must remind once: %+v", rep)
	}

	// 同样的桶再跑一次：没有新桶 → unchanged，不重复累加
	rep2, err := svc.RunOnce(ctx, sn)
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Scored != 0 || rep2.Unchanged != 1 {
		t.Fatalf("idempotent run: %+v", rep2)
	}
	var used2 float64
	if err := db.QueryRow(ctx, `SELECT used_hours FROM iot_shard.consumable_health WHERE sn=$1 AND part='module'`, sn).Scan(&used2); err != nil {
		t.Fatal(err)
	}
	if used2 != used {
		t.Fatalf("used hours must not double count: %v → %v", used, used2)
	}

	var lvl string
	if err := db.QueryRow(ctx, `SELECT level FROM iot_shard.health_reminder WHERE sn=$1 ORDER BY id DESC LIMIT 1`, sn).Scan(&lvl); err != nil {
		t.Fatal(err)
	}
	if lvl != "80" {
		t.Fatalf("reminder level=%q want 80", lvl)
	}

	// 继续掉到 50 档以下，但还在 7 天冷却期内 → 不再发第二条
	src.buckets = hourlyBuckets(sn, pk, base.Add(30*time.Hour), 40, 130, 1)
	rep4, err := svc.RunOnce(ctx, sn)
	if err != nil {
		t.Fatal(err)
	}
	if rep4.Scored != 1 {
		t.Fatalf("second scoring run: %+v", rep4)
	}
	if rep4.Reminders != 0 {
		t.Fatalf("cooldown must suppress the second reminder: %+v", rep4)
	}
	var cnt int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.health_reminder WHERE sn=$1 AND sent_at IS NOT NULL`, sn).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Fatalf("exactly one reminder must have been sent, got %d", cnt)
	}
}

// 输入缺失既不写 0，整批 skip 过半时还要熔断（INC-3-02）。
func TestIntegration_MissingInputsNeverWriteZero(t *testing.T) {
	db, ctx := itDB(t)
	tag := fmt.Sprintf("%d", time.Now().UnixNano()%1e9)
	good, bad := "ITHG_"+tag, "ITHB_"+tag
	t.Cleanup(func() {
		c := context.Background()
		for _, sn := range []string{good, bad} {
			_, _ = db.Exec(c, `DELETE FROM iot_shard.health_explain WHERE sn=$1`, sn)
			_, _ = db.Exec(c, `DELETE FROM iot_shard.consumable_health WHERE sn=$1`, sn)
		}
	})
	base := time.Now().UTC().Truncate(time.Hour).Add(-10 * time.Hour)
	src := &itSource{
		buckets:  append(hourlyBuckets(good, "LM_S1", base, 5, 10, 1), hourlyBuckets(bad, "LM_S1", base, 5, 10, 1)...),
		overtemp: map[string]int{},
	}
	// bad 没有 module_model → skip；两台里 skip 一台 = 50% > 30% → 整批熔断
	sh := &itShadow{m: map[string]Reported{good: {Found: true, ModuleModel: "LM40"}}}
	svc := NewService(&PGStore{DB: db}, src, sh, nil, NewMetrics(), Options{MaterialKey: []byte("it")})

	rep, err := svc.RunOnce(ctx, good, bad)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Fused || rep.Skipped != 1 {
		t.Fatalf("batch must fuse: %+v", rep)
	}
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.consumable_health WHERE sn = ANY($1)`, []string{good, bad}).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("a fused batch must write nothing, got %d rows", n)
	}

	// 放宽熔断阈值后：good 打分，bad 仍然一行都不写（缺输入绝不落 0）
	svc.Opt.FuseRatio = 0.9
	if rep, err = svc.RunOnce(ctx, good, bad); err != nil || rep.Fused || rep.Scored != 1 {
		t.Fatalf("relaxed run: %+v %v", rep, err)
	}
	if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.consumable_health WHERE sn=$1`, bad).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("device with missing inputs must have no row at all, got %d", n)
	}
}

// 材料码：签发 → 校验一次通过 → 第二次重放被拒且不带参数。
func TestIntegration_MaterialCodeOneShot(t *testing.T) {
	db, ctx := itDB(t)
	tag := time.Now().UnixNano() % 1e6
	idx := uint32(900000 + tag%90000)
	mat := fmt.Sprintf("ITMAT_%d", tag)
	var code string
	t.Cleanup(func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_shard.material_code_scan WHERE code_id=$1`, code)
		_, _ = db.Exec(c, `DELETE FROM iot_global.material_code WHERE code_id=$1`, code)
		_, _ = db.Exec(c, `DELETE FROM iot_global.material_index WHERE material_id=$1`, mat)
		_, _ = db.Exec(c, `DELETE FROM iot_global.material WHERE material_id=$1`, mat)
	})
	if _, err := db.Exec(ctx, `INSERT INTO iot_global.material(material_id,name,category,thickness_mm) VALUES($1,'it material','wood',3.0)`, mat); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO iot_global.material_index(material_idx,material_id) VALUES($1,$2)`, idx, mat); err != nil {
		t.Fatal(err)
	}

	svc := NewService(&PGStore{DB: db}, &itSource{}, &itShadow{}, nil, NewMetrics(),
		Options{MaterialKey: []byte("it-master"), MaxScans: 1})
	var err error
	if code, err = svc.MintCode(ctx, idx, 11, 1); err != nil {
		t.Fatal(err)
	}
	uid := int64(42)
	res, err := svc.Verify(ctx, code, "ITSN1", &uid)
	if err != nil || !res.Valid || res.MaterialID != mat {
		t.Fatalf("first scan: %+v %v", res, err)
	}
	if res.ThicknessMM == nil || *res.ThicknessMM != 3.0 {
		t.Fatalf("thickness not carried: %+v", res)
	}
	// 一次性：第二次是重放，且不得带出任何参数
	res2, err := svc.Verify(ctx, code, "ITSN1", &uid)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Valid || res2.MaterialID != "" || res2.ThicknessMM != nil {
		t.Fatalf("replay must be rejected without params: %+v", res2)
	}
}
