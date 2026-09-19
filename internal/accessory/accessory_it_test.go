package accessory

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

// 集成：真实 PG + Redis（IOT_IT 门控）。执行器用 fake 记录 deviceapi 调用（deviceapi 契约见 handler_test 的 fakeAct）。
func TestIntegration_LinkageEndToEnd(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	rdb := config.MustRedis()
	defer rdb.Close()

	tag := fmt.Sprintf("%d", time.Now().UnixNano()%1e9)
	host, acc, acc2 := "ITHOST"+tag, "ITACC"+tag+"A", "ITACC"+tag+"B"
	for _, d := range []struct{ sn, pk string }{{host, "LM_S1"}, {acc, "ACC_PURIFIER"}, {acc2, "ACC_PURIFIER"}} {
		if _, err := db.Exec(ctx, `INSERT INTO iot_shard.device(sn,product_key,region,cell_id,status) VALUES($1,$2,'US',1,'activated')`, d.sn, d.pk); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_shard.linkage_audit WHERE host_sn=$1`, host)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.alarm_context WHERE acc_sn IN ($1,$2)`, acc, acc2)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.accessory_pairing WHERE host_sn=$1`, host)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.device WHERE sn IN ($1,$2,$3)`, host, acc, acc2)
		_ = rdb.Del(c, KeyOffMeta+acc, pairingCacheKey(host), KeySafety+acc).Err()
		_ = rdb.ZRem(c, KeyOffZSet, acc).Err()
	})

	store := &PGStore{Pool: db}
	act := newFakeAct()
	now := time.Now().UTC().Truncate(time.Second)
	clock := now
	eng := NewEngine(store, rdb, act, nil, Options{AllowOff: true, StopHostOnFire: true, Now: func() time.Time { return clock }})
	svc := &Service{Store: store, RDB: rdb, Engine: eng, Act: act, M: eng.M}

	// 配对（无绑定信息 → owner 校验放行）
	p, created, err := svc.Pair(ctx, PairReq{HostSN: host, AccSN: acc})
	if err != nil || !created || p.OffDelayS != 180 {
		t.Fatalf("pair: %+v %v %v", p, created, err)
	}
	if _, created, err := svc.Pair(ctx, PairReq{HostSN: host, AccSN: acc}); err != nil || created {
		t.Fatalf("pair idempotent: %v %v", created, err)
	}
	// 部分唯一索引：同一配件绕过服务直接再插一条有效配对必须被 PG 拒绝
	_, err = db.Exec(ctx, `INSERT INTO iot_shard.accessory_pairing(host_sn,acc_sn,acc_type,pair_key) VALUES($1,$2,'purifier',$3)`, "OTHERHOST", acc, NewPairKey())
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "uk_pairing_active" {
		t.Fatalf("uk_pairing_active must reject second active pairing, got %v", err)
	}
	// off_delay_s 越界由 CHECK 拒绝
	if _, _, err := store.Pair(ctx, host, acc2, "purifier", true, 10, NewPairKey()); !errors.Is(err, ErrBadParam) {
		t.Fatalf("off_delay 10 must be rejected by ck_pairing_off_delay: %v", err)
	}

	// 主机 JOB_START → 净化器 desired power_on=true, fan_level=3（ACRYLIC 规则）
	out, err := eng.Handle(ctx, eventEnv(host, "JOB_START", "ACRYLIC_3MM", 1, clock))
	if err != nil || out != OutcomeActed {
		t.Fatalf("job start: %v %v", out, err)
	}
	// 配对时已下发一条 paired_sn，联动是第二条
	if got := act.desired[acc]; len(got) != 2 || got[0]["paired_sn"] != host || got[1]["power_on"] != true || got[1]["fan_level"] != 3 {
		t.Fatalf("desired after start: %v", got)
	}
	// 重复 seq 幂等（Redis SETNX）
	if out, _ := eng.Handle(ctx, eventEnv(host, "JOB_START", "ACRYLIC_3MM", 1, clock)); out != OutcomeDedup {
		t.Fatalf("dedup: %v", out)
	}
	// JOB_DONE → ZSET 排定 180 s 后关闭（作业跑了 30 s；ScheduledAt 必须晚于 LastJobStart，否则 FireOff 视为「排定后又开工」而取消）
	clock = clock.Add(30 * time.Second)
	if out, err := eng.Handle(ctx, eventEnv(host, "JOB_DONE", "", 2, clock)); err != nil || out != OutcomeActed {
		t.Fatalf("job done: %v %v", out, err)
	}
	score, err := rdb.ZScore(ctx, KeyOffZSet, acc).Result()
	if err != nil || int64(score) != clock.Add(180*time.Second).Unix() {
		t.Fatalf("off timer score=%v err=%v want %d", score, err, clock.Add(180*time.Second).Unix())
	}
	if due := eng.PopDue(ctx, clock.Add(100*time.Second)); len(due) != 0 {
		t.Fatalf("not due yet: %v", due)
	}
	// 到期 → 下发 power_on=false
	clock = clock.Add(181 * time.Second)
	due := eng.PopDue(ctx, clock)
	if len(due) != 1 || due[0] != acc {
		t.Fatalf("due: %v", due)
	}
	eng.FireOff(ctx, acc, clock)
	if got := act.desired[acc]; len(got) != 3 || got[2]["power_on"] != false {
		t.Fatalf("desired after off: %v", got)
	}
	if eng.HasOffTimer(ctx, acc) {
		t.Fatal("timer must be consumed")
	}
	// 留痕：start 的 set_desired、done 的 schedule_off、timer 的 set_desired
	audits, err := store.ListAudit(ctx, host, 50)
	if err != nil || len(audits) < 3 {
		t.Fatalf("audits %d %v", len(audits), err)
	}
	seen := map[string]bool{}
	for _, a := range audits {
		seen[a.Trigger] = true
	}
	for _, tr := range []Trigger{TrigJobStart, TrigJobEnd, TrigOffTimer} {
		if !seen[string(tr)] {
			t.Fatalf("missing audit trigger %s in %v", tr, seen)
		}
	}

	// 配件灭火事件：alarm_context 关联主机
	if out, err := eng.Handle(ctx, eventEnv(acc, "FIRE_SUPPRESSED", "", 3, clock)); err != nil || out != OutcomeActed {
		t.Fatalf("fire: %v %v", out, err)
	}
	c, err := store.GetAlarmContext(ctx, acc, "FIRE_SUPPRESSED", clock)
	if err != nil || c.HostSN != host {
		t.Fatalf("alarm_context %+v %v", c, err)
	}
	if len(act.stops) != 1 || act.stops[0] != host {
		t.Fatalf("host stop %v", act.stops)
	}

	// 解除配对后主机事件不再联动
	if _, err := svc.Unpair(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if out, _ := eng.Handle(ctx, eventEnv(host, "JOB_START", "", 4, clock)); out != OutcomeNoPairing {
		t.Fatalf("after unpair: %v", out)
	}
}

// 集成：滤芯寿命同事务写 filter_life 与 consumable_health(part='filter')。
func TestIntegration_FilterLifeUpsert(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	store := &PGStore{Pool: db}
	acc := fmt.Sprintf("ITFLT%d", time.Now().UnixNano()%1e9)
	t.Cleanup(func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_shard.filter_life WHERE acc_sn=$1`, acc)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.consumable_health WHERE sn=$1`, acc)
	})
	cfg, err := store.FilterModel(ctx, "HEPA-STD")
	if err != nil || cfg.FlowCoeff[2] != 120 || len(cfg.RPMLevels) != 4 {
		t.Fatalf("filter_model %+v %v", cfg, err)
	}
	lh := time.Now().UTC().Truncate(time.Hour)
	if err := store.FilterLifeUpsert(ctx, FilterLife{AccSN: acc, FilterModel: "HEPA-STD", InstalledAt: lh, EqAirVolume: 360000, RunSeconds: 7200, Health: 0.9, LastHour: &lh}); err != nil {
		t.Fatal(err)
	}
	f, err := store.FilterLifeGet(ctx, acc)
	if err != nil || f.Health != 0.9 || f.RunSeconds != 7200 {
		t.Fatalf("filter_life %+v %v", f, err)
	}
	var health, used float64
	if err := db.QueryRow(ctx, `SELECT health::float8, used_hours::float8 FROM iot_shard.consumable_health WHERE sn=$1 AND part='filter'`, acc).Scan(&health, &used); err != nil || health != 90 || used != 2 {
		t.Fatalf("consumable_health health=%v used=%v err=%v", health, used, err)
	}
}
