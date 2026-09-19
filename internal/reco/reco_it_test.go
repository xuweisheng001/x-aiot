package reco

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
)

// 集成（IOT_IT）：35 台 SN 的 good 标记 + 匹配的 user_param → candidate；3 台 → 不生成；
// 功率超过官方档 110% → removed/power_cap。TD=nil 跳过安全关联。
func TestIntegration_Candidates(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()

	tag := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	pk, module, material := "LM_S1", "ITMOD"+tag, "BASSWOOD_3MM"
	paramsBig := json.RawMessage(`{"power":80,"speed":12,"passes":1}`)
	paramsSmall := json.RawMessage(`{"power":40,"speed":30,"passes":1}`)
	hashBig, hashSmall := CanonicalHash(paramsBig), CanonicalHash(paramsSmall)
	userID := int64(900000 + time.Now().UnixNano()%100000)

	var sns []string
	cleanup := func() {
		for _, sn := range sns {
			_, _ = db.Exec(context.Background(), `DELETE FROM iot_shard.job_record WHERE sn=$1`, sn)
			_ = rdb.Del(context.Background(), "device:"+sn, shadow.Key(sn)).Err()
		}
		_, _ = db.Exec(context.Background(), `DELETE FROM iot_shard.user_param WHERE user_id IN ($1,$2)`, userID, userID+1)
		_, _ = db.Exec(context.Background(), `DELETE FROM iot_global.param_recommendation WHERE module_model=$1`, module)
		_, _ = db.Exec(context.Background(), `DELETE FROM iot_global.param_profile WHERE module_model=$1`, module)
	}
	defer cleanup()

	seed := func(n int, hash, prefix string) {
		for i := 0; i < n; i++ {
			sn := fmt.Sprintf("IT_RECO_%s_%s_%02d", prefix, tag, i)
			sns = append(sns, sn)
			if err := rdb.HSet(ctx, "device:"+sn, "pk", pk).Err(); err != nil {
				t.Fatal(err)
			}
			if err := rdb.HSet(ctx, shadow.Key(sn), "module_model", module, "laser_hours", "120.5").Err(); err != nil {
				t.Fatal(err)
			}
			jobID := fmt.Sprintf("%08x-%s-4000-8000-%012d", i, prefix[:4], time.Now().UnixNano()%1_000_000_000_000)
			if _, err := db.Exec(ctx, `INSERT INTO iot_shard.job_record(job_id,sn,material_id,params_hash,started_at,finished_at,outcome)
				VALUES ($1,$2,$3,$4,now()-interval '1 day',now()-interval '1 day'+interval '10 min','done')`, jobID, sn, material, hash); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ctx, `INSERT INTO iot_shard.job_feedback(job_id,sn,rating) VALUES ($1,$2,'good')`, jobID, sn); err != nil {
				t.Fatal(err)
			}
		}
	}
	seed(35, hashSmall, "smal") // 35 台 → 候选
	seed(3, hashBig, "bigp")    // 3 台 → 样本不足
	// user_param 对 (user_id, pk, module, material) 唯一：两套参数用两个用户
	for i, p := range []json.RawMessage{paramsBig, paramsSmall} {
		if _, err := db.Exec(ctx, `INSERT INTO iot_shard.user_param(user_id,product_key,module_model,material_id,params) VALUES ($1,$2,$3,$4,$5)`,
			userID+int64(i), pk, module, material, p); err != nil {
			t.Fatal(err)
		}
	}
	// 官方档功率 30 → paramsSmall(40) 超 110% → removed/power_cap
	if _, err := db.Exec(ctx, `INSERT INTO iot_global.param_profile(product_key,module_model,material_id,params,source,version_added,approved_by)
		VALUES ($1,$2,$3,'{"power":30,"speed":30}','official',1,'it')`, pk, module, material); err != nil {
		t.Fatal(err)
	}

	r := NewRunner(db, rdb, nil, 30)
	rep, err := r.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// RunOnce 的计数是全库口径：同一套依赖上跑过冒烟 / 其它测试时，库里会有别的组一起被统计。
	// 断言只看「本测试造的两组是否被正确处理」，不看全局计数，否则测试结果取决于别人留下的数据。
	if rep.Candidates < 1 || rep.Upserted < 1 || rep.PowerCapped < 1 {
		t.Fatalf("seeded group must produce one capped candidate: %+v", rep)
	}
	if rep.Excluded[ReasonFewSamples] < 1 {
		t.Fatalf("3-device group must be excluded as few_samples: %+v", rep)
	}
	// 样本不足的那组绝不能落库
	var fewRows int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_global.param_recommendation WHERE module_model=$1 AND params_hash=$2`,
		module, hashBig).Scan(&fewRows); err != nil {
		t.Fatal(err)
	}
	if fewRows != 0 {
		t.Fatalf("few-sample group must not be stored, got %d rows", fewRows)
	}
	var status, reason string
	var samples int
	if err := db.QueryRow(ctx, `SELECT status, COALESCE(removed_reason,''), sample_count FROM iot_global.param_recommendation WHERE module_model=$1 AND params_hash=$2`,
		module, hashSmall).Scan(&status, &reason, &samples); err != nil {
		t.Fatal(err)
	}
	if status != "removed" || reason != "power_cap" || samples != 35 {
		t.Fatalf("status=%s reason=%s samples=%d", status, reason, samples)
	}
	var n int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM iot_global.param_recommendation WHERE module_model=$1 AND params_hash=$2`, module, hashBig).Scan(&n)
	if n != 0 {
		t.Fatal("3-device group must not produce a recommendation")
	}

	// 抬高官方档功率 → 重跑后仍保持 removed（removed 不回退），但新分组可成为 candidate
	if _, err := db.Exec(ctx, `UPDATE iot_global.param_profile SET params='{"power":90,"speed":30}' WHERE module_model=$1`, module); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	_ = db.QueryRow(ctx, `SELECT status FROM iot_global.param_recommendation WHERE module_model=$1 AND params_hash=$2`, module, hashSmall).Scan(&status)
	if status != "removed" {
		t.Fatalf("removed must not revert to candidate, got %s", status)
	}
	t.Logf("report: %+v", rep)
}
