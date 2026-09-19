package probe

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

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

// fakeEmitter 代替真实链路：可以选择「发出去之后真的会有告警」或「石沉大海」，
// 这样集成测试不必依赖 alarm-svc 在跑也能验证探针的两种判定与清理行为。
type fakeEmitter struct {
	db      *pgxpool.Pool
	deliver bool          // false 模拟链路断掉
	delay   time.Duration // 模拟链路慢
	notify  bool          // 是否写 notified_at（false 模拟「落库了但没推送」）
}

func (f *fakeEmitter) Name() string { return "fake" }
func (f *fakeEmitter) Close()       {}

func (f *fakeEmitter) Emit(ctx context.Context, sn, _, code string, _ int64, ts time.Time) error {
	if !f.deliver {
		return nil
	}
	go func() {
		time.Sleep(f.delay)
		c := context.Background()
		notified := "NULL"
		if f.notify {
			notified = "now()"
		}
		_, _ = f.db.Exec(c, fmt.Sprintf(`INSERT INTO iot_shard.alarm(sn,code,level,event_ts,status,notified_at,notified_channels)
			VALUES($1,$2,'critical',$3,'notified',%s,'app')`, notified), sn, code, ts)
	}()
	return nil
}

func probeSN() string { return fmt.Sprintf("ITPROBE%d", time.Now().UnixNano()%1e6) }

func cleanupSN(t *testing.T, db *pgxpool.Pool, sn string) {
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM iot_shard.alarm WHERE sn=$1`, sn)
	})
}

// 告警按时到达 → ok，并且探针把自己造的告警关掉（不污染运营的告警列表）。
func TestIntegration_ProbeOKAndCleansUp(t *testing.T) {
	db, ctx := itDB(t)
	sn := probeSN()
	cleanupSN(t, db, sn)

	p := New(db, &fakeEmitter{db: db, deliver: true, notify: true, delay: 100 * time.Millisecond},
		NewMetrics(), Options{SN: sn, SLO: 3 * time.Second, Timeout: 10 * time.Second})
	res, err := p.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusOK {
		t.Fatalf("status=%s reason=%s latency=%s", res.Status, res.Reason, res.Latency)
	}
	if res.AlarmID == 0 || res.NotifiedA == nil {
		t.Fatalf("result missing alarm details: %+v", res)
	}
	if !res.CleanedUp {
		t.Fatal("probe must close the alarm it created")
	}
	var status string
	var closedBy *string
	if err := db.QueryRow(ctx, `SELECT status, closed_by FROM iot_shard.alarm WHERE id=$1`, res.AlarmID).Scan(&status, &closedBy); err != nil {
		t.Fatal(err)
	}
	if status != "closed" || closedBy == nil || *closedBy != "probe" {
		t.Fatalf("synthetic alarm left as status=%s closed_by=%v", status, closedBy)
	}
	// 运营的未处理列表里不该出现探针告警
	var open int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.alarm WHERE sn=$1 AND status IN ('open','notified')`, sn).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 0 {
		t.Fatalf("%d synthetic alarms still pending for operators", open)
	}
	if p.M.Get(MOK) != 1 || p.M.Get(MCleaned) != 1 {
		t.Fatalf("counters ok=%d cleaned=%d", p.M.Get(MOK), p.M.Get(MCleaned))
	}
}

// 链路断掉 → missing，并且判定为 S1。
func TestIntegration_ProbeMissingIsFatal(t *testing.T) {
	db, ctx := itDB(t)
	sn := probeSN()
	cleanupSN(t, db, sn)

	p := New(db, &fakeEmitter{db: db, deliver: false}, NewMetrics(),
		Options{SN: sn, SLO: time.Second, Timeout: 1500 * time.Millisecond, Poll: 50 * time.Millisecond})
	res, err := p.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusMissing || !res.Fatal() {
		t.Fatalf("a dead chain must be S1 missing: %+v", res)
	}
	if p.M.Get(MMissing) != 1 {
		t.Fatalf("missing counter=%d", p.M.Get(MMissing))
	}
}

// 告警落库但一直没推送 → 仍然算 missing：用户收不到就是漏告警，
// 只有 notified_at 才代表「送到了」。
func TestIntegration_ProbeStoredButNeverNotified(t *testing.T) {
	db, ctx := itDB(t)
	sn := probeSN()
	cleanupSN(t, db, sn)

	p := New(db, &fakeEmitter{db: db, deliver: true, notify: false}, NewMetrics(),
		Options{SN: sn, SLO: time.Second, Timeout: 1500 * time.Millisecond, Poll: 50 * time.Millisecond})
	res, err := p.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusMissing {
		t.Fatalf("an alarm row without notified_at must not count as delivered: %+v", res)
	}
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM iot_shard.alarm WHERE sn=$1`, sn).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected the un-notified row to still be there, got %d", n)
	}
}

// 超过 SLO 但到达 → slow（S2，不是 S1）。
func TestIntegration_ProbeSlow(t *testing.T) {
	db, ctx := itDB(t)
	sn := probeSN()
	cleanupSN(t, db, sn)

	p := New(db, &fakeEmitter{db: db, deliver: true, notify: true, delay: 400 * time.Millisecond},
		NewMetrics(), Options{SN: sn, SLO: 50 * time.Millisecond, Timeout: 5 * time.Second, Poll: 50 * time.Millisecond})
	res, err := p.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusSlow || res.Fatal() {
		t.Fatalf("over SLO but delivered must be slow, not S1: %+v", res)
	}
	if p.M.Quantile(0.5) <= 0 {
		t.Fatal("slow runs must still feed the latency quantiles")
	}
}

// 上一轮遗留的告警不能被当成本轮结果（baseline id 隔离）。
func TestIntegration_ProbeIgnoresStaleAlarm(t *testing.T) {
	db, ctx := itDB(t)
	sn := probeSN()
	cleanupSN(t, db, sn)

	// 先塞一条「上一轮」的已推送告警
	if _, err := db.Exec(ctx, `INSERT INTO iot_shard.alarm(sn,code,level,event_ts,status,notified_at,notified_channels)
		VALUES($1,'FLAME_DETECTED','critical',now()-interval '1 hour','notified',now()-interval '1 hour','app')`, sn); err != nil {
		t.Fatal(err)
	}
	p := New(db, &fakeEmitter{db: db, deliver: false}, NewMetrics(),
		Options{SN: sn, SLO: time.Second, Timeout: time.Second, Poll: 50 * time.Millisecond})
	res, err := p.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusMissing {
		t.Fatalf("a stale alarm must not be mistaken for this round's result: %+v", res)
	}
}
