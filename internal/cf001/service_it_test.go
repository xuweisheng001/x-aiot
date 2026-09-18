package cf001

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

func itService(ctx context.Context, t *testing.T) *Service {
	t.Helper()
	db := config.MustPG(ctx)
	rdb := config.MustRedis()
	t.Cleanup(func() { db.Close(); rdb.Close() })
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return NewService(db, rdb, key)
}

// 配额事务：100 goroutine 抢 10 配额，恰好 10 成功 90 拒绝（-race）。
func TestIntegration_QuotaRace(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	svc := itService(ctx, t)
	orderNo := fmt.Sprintf("IT-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		c := context.Background()
		_, _ = svc.DB.Exec(c, `DELETE FROM cf001.oem_devices WHERE digest IN (SELECT digest FROM cf001.digest_maps WHERE order_no=$1)`, orderNo)
		_, _ = svc.DB.Exec(c, `DELETE FROM cf001.digest_maps WHERE order_no=$1`, orderNo)
		_, _ = svc.DB.Exec(c, `DELETE FROM cf001.oem_quotas WHERE order_no=$1`, orderNo)
	})
	if _, err := svc.CreateQuota(ctx, QuotaReq{OrderNo: orderNo, Supplier: "ACME", ProductKey: "LM_S1", Quota: 10}); err != nil {
		t.Fatal(err)
	}

	var okN, noQuotaN, otherN atomic.Int32
	var wg sync.WaitGroup
	results := make(chan *SignResult, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := Digest(fmt.Sprintf("uuid-%s-%d", orderNo, i), "MCU", "SOC", "MAC")
			r, err := svc.SignDigest(ctx, SignInput{OrderNo: orderNo, Line: "01", Digest: d, UUID: "u", MCUSN: "m", SOCSN: "s", MAC: "x"})
			switch {
			case err == nil:
				okN.Add(1)
				results <- r
			case errors.Is(err, ErrNoQuota):
				noQuotaN.Add(1)
			default:
				otherN.Add(1)
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	close(results)
	if okN.Load() != 10 || noQuotaN.Load() != 90 || otherN.Load() != 0 {
		t.Fatalf("ok=%d noquota=%d other=%d", okN.Load(), noQuotaN.Load(), otherN.Load())
	}
	seen := map[string]bool{}
	for r := range results {
		if !ValidSN(r.SN) || seen[r.SN] {
			t.Errorf("bad/duplicate sn %s", r.SN)
		}
		seen[r.SN] = true
		if err := VerifyDigestPSS(&svc.Key.PublicKey, "", r.SignatureB64); err == nil {
			t.Error("signature must be bound to digest")
		}
	}
	q, err := svc.GetQuota(ctx, orderNo)
	if err != nil || q.Registered != 10 || q.Status != 2 {
		t.Fatalf("quota after race: %+v err=%v", q, err)
	}
	// 幂等：已注册 digest 再签不消耗配额（此时配额已满仍成功）
	d0 := Digest(fmt.Sprintf("uuid-%s-%d", orderNo, 0), "MCU", "SOC", "MAC")
	var wantSN string
	_ = svc.DB.QueryRow(ctx, `SELECT sn FROM cf001.digest_maps WHERE digest=$1`, d0).Scan(&wantSN)
	if wantSN == "" {
		// goroutine 0 可能是被拒绝的 90 个之一；找任一已注册 digest
		_ = svc.DB.QueryRow(ctx, `SELECT digest, sn FROM cf001.digest_maps WHERE order_no=$1 LIMIT 1`, orderNo).Scan(&d0, &wantSN)
	}
	r, err := svc.SignDigest(ctx, SignInput{OrderNo: orderNo, Digest: d0})
	if err != nil || !r.Existing || r.SN != wantSN {
		t.Fatalf("idempotent re-sign: %+v err=%v", r, err)
	}
	// 追加配额 → status 回 1 → 又能签
	if _, err := svc.AppendQuota(ctx, orderNo, 1); err != nil {
		t.Fatal(err)
	}
	r2, err := svc.SignDigest(ctx, SignInput{OrderNo: orderNo, Digest: Digest("extra", orderNo, "s", "m")})
	if err != nil || r2.Existing {
		t.Fatalf("after append: %+v err=%v", r2, err)
	}
	// 三步自检
	v, err := svc.Verify(ctx, r2.SN, Digest("extra", orderNo, "s", "m"), r2.SignatureB64)
	if err != nil || !v.OK || v.Step != "signature" {
		t.Fatalf("verify: %+v err=%v", v, err)
	}
	if v, _ := svc.Verify(ctx, "NOPE", "x", "y"); v.OK || v.Step != "exists" {
		t.Fatalf("verify missing sn: %+v", v)
	}
	if v, _ := svc.Verify(ctx, r2.SN, "wrongdigest", r2.SignatureB64); v.OK || v.Step != "digest" {
		t.Fatalf("verify wrong digest: %+v", v)
	}
	if v, _ := svc.Verify(ctx, r2.SN, Digest("extra", orderNo, "s", "m"), r.SignatureB64); v.OK || v.Step != "signature" {
		t.Fatalf("verify wrong signature: %+v", v)
	}
}

func sqlStateOf(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// 约束证明（spec §5）：ck_full_stage_approved（23514）与 uk_binding_active（23505）真的生效。
func TestIntegration_DDLConstraints(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) // 全部回滚，不留数据

	// --- ota_batch: stage '100' + NULL approved_by → 23514
	var fwID int64
	if err := tx.QueryRow(ctx, `INSERT INTO iot_global.firmware(product_key,version,full_url,full_size,sha256,signature)
		VALUES('LM_S1',$1,'u',1,repeat('0',64),'s') RETURNING id`, fmt.Sprintf("it-%d", time.Now().UnixNano())).Scan(&fwID); err != nil {
		t.Fatal(err)
	}
	sp, _ := tx.Begin(ctx) // savepoint
	_, err = sp.Exec(ctx, `INSERT INTO iot_global.ota_batch(firmware_id,stage,created_by) VALUES($1,'100','a')`, fwID)
	if sqlStateOf(err) != "23514" {
		t.Fatalf("stage 100 without approved_by: want 23514, got %v", err)
	}
	_ = sp.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO iot_global.ota_batch(firmware_id,stage,created_by,approved_by) VALUES($1,'100','a','b')`, fwID); err != nil {
		t.Fatalf("stage 100 with approved_by should pass: %v", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO iot_global.ota_batch(firmware_id,stage,created_by) VALUES($1,'10','a')`, fwID); err != nil {
		t.Fatalf("stage 10 without approved_by should pass: %v", err)
	}

	// --- device_binding: 同 (sn,user_id) 第二条 active → 23505；unbound_at 非空可共存
	sn := fmt.Sprintf("ITBIND%d", time.Now().UnixNano()%1e9)
	if _, err = tx.Exec(ctx, `INSERT INTO iot_shard.device_binding(sn,user_id) VALUES($1,42)`, sn); err != nil {
		t.Fatal(err)
	}
	sp2, _ := tx.Begin(ctx)
	_, err = sp2.Exec(ctx, `INSERT INTO iot_shard.device_binding(sn,user_id) VALUES($1,42)`, sn)
	if sqlStateOf(err) != "23505" {
		t.Fatalf("second active binding: want 23505, got %v", err)
	}
	_ = sp2.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO iot_shard.device_binding(sn,user_id,unbound_at) VALUES($1,42,now())`, sn); err != nil {
		t.Fatalf("unbound row should coexist: %v", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO iot_shard.device_binding(sn,user_id) VALUES($1,43)`, sn); err != nil {
		t.Fatalf("different user should bind: %v", err)
	}
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM iot_shard.device_binding WHERE sn=$1`, sn).Scan(&n); err != nil || n != 3 {
		t.Fatalf("count=%d err=%v", n, err)
	}

}
