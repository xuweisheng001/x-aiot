package job

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
)

// 集成（IOT_IT）：真实 PG + Redis。未同意的 SN 的 job_record 被对账删除；
// 已同意的与影子读不出来的都必须原样保留。
func TestIntegration_OptInReconcile(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()

	suffix := time.Now().UnixNano()
	snBad := fmt.Sprintf("IT_OPTIN_N_%d", suffix)
	snOK := fmt.Sprintf("IT_OPTIN_Y_%d", suffix)
	snErr := fmt.Sprintf("IT_OPTIN_E_%d", suffix)
	all := []string{snBad, snOK, snErr}
	cleanup := func() {
		c := context.Background()
		for _, sn := range all {
			_, _ = db.Exec(c, `DELETE FROM iot_shard.job_record WHERE sn=$1`, sn)
			_, _ = db.Exec(c, `DELETE FROM iot_shard.job_feedback_pending WHERE sn=$1`, sn)
			_ = rdb.Del(c, shadow.Key(sn)).Err()
		}
	}
	cleanup()
	defer cleanup()

	if err := rdb.HSet(ctx, shadow.Key(snBad), OptInField, "false").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, shadow.Key(snOK), OptInField, "true").Err(); err != nil {
		t.Fatal(err)
	}
	// snErr 的影子存在与否无所谓：下面的 reader 对它直接返回错误，模拟 Redis 抖动。
	jobIDs := map[string]string{}
	for i, sn := range all {
		id := fmt.Sprintf("%08d-0000-4000-8000-%012d", i, suffix%1e12)
		jobIDs[sn] = id
		if _, err := db.Exec(ctx, `INSERT INTO iot_shard.job_record(job_id, sn, material_id, started_at) VALUES($1,$2,'ACRYLIC',now())`, id, sn); err != nil {
			t.Fatal(err)
		}
	}

	store := &PGStore{Pool: db}
	// DistinctJobSNs 打真实 PG：必须能看到刚插的三个 SN。
	seen := map[string]bool{}
	sns, err := store.DistinctJobSNs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, sn := range sns {
		seen[sn] = true
	}
	for _, sn := range all {
		if !seen[sn] {
			t.Fatalf("DistinctJobSNs missing %s", sn)
		}
	}

	shadowErr := errors.New("redis flapping")
	reader := ShadowReaderFunc(func(ctx context.Context, sn string) (map[string]string, error) {
		if sn == snErr {
			return nil, shadowErr
		}
		return shadow.Read(ctx, rdb, sn)
	})
	svc := NewService(store, reader, NewMetrics())
	// 只喂本用例造的 SN：真实 DistinctJobSNs 已单独验证过，
	// 这里不让一轮对账去动开发库里别人的历史数据。
	r := NewOptInReconciler(svc, &fakeSNs{sns: all})

	rep, err := r.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Scanned != 3 || rep.Violations != 1 || rep.Purged != 1 || rep.PurgedRows != 1 || rep.ShadowErrors != 1 {
		t.Fatalf("report=%+v", rep)
	}
	count := func(sn string) int {
		var n int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.job_record WHERE sn=$1`, sn).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if count(snBad) != 0 {
		t.Fatal("optin=false 的 job_record 必须被删除")
	}
	if count(snOK) != 1 {
		t.Fatal("已同意的数据不得被删")
	}
	if count(snErr) != 1 {
		t.Fatal("影子读不到的 SN 不得被删（Redis 抖动不能变成删库）")
	}
	if svc.M.Get(MOptInViolations) != 1 || svc.M.Get(MOptInPurged) != 1 ||
		svc.M.Get(MOptInPurgedRows) != 1 || svc.M.Get(MOptInShadowErrors) != 1 {
		t.Fatalf("metrics violations=%d purged=%d rows=%d shadow_errors=%d",
			svc.M.Get(MOptInViolations), svc.M.Get(MOptInPurged), svc.M.Get(MOptInPurgedRows), svc.M.Get(MOptInShadowErrors))
	}
}
