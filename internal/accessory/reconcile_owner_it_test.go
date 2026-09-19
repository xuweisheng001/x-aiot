package accessory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

// 集成（IOT_IT）：真实 PG。配对时 owner 一致 → 配件转让给别人 → 对账发现越权并停联动（不解绑）。
func TestIntegration_OwnerReconcile(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := config.MustPG(ctx)
	defer db.Close()

	tag := fmt.Sprintf("%d", time.Now().UnixNano()%1e9)
	host, acc := "ITOWNH"+tag, "ITOWNA"+tag
	var userA, userB int64 = 910000 + time.Now().UnixNano()%1000, 920000 + time.Now().UnixNano()%1000
	for _, d := range []struct{ sn, pk string }{{host, "LM_S1"}, {acc, "ACC_PURIFIER"}} {
		if _, err := db.Exec(ctx, `INSERT INTO iot_shard.device(sn,product_key,region,cell_id,status) VALUES($1,$2,'US',1,'activated')`, d.sn, d.pk); err != nil {
			t.Fatal(err)
		}
	}
	for _, sn := range []string{host, acc} {
		if _, err := db.Exec(ctx, `INSERT INTO iot_shard.device_binding(sn,user_id,role) VALUES($1,$2,'owner')`, sn, userA); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = db.Exec(c, `DELETE FROM iot_shard.accessory_pairing WHERE host_sn=$1`, host)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.device_binding WHERE sn IN ($1,$2)`, host, acc)
		_, _ = db.Exec(c, `DELETE FROM iot_shard.device WHERE sn IN ($1,$2)`, host, acc)
	})

	store := &PGStore{Pool: db}
	svc := &Service{Store: store, M: NewMetrics()}
	p, created, err := svc.Pair(ctx, PairReq{HostSN: host, AccSN: acc})
	if err != nil || !created {
		t.Fatalf("pair: %v created=%v", err, created)
	}

	m := NewMetrics()
	r := NewOwnerReconciler(store, nil, m)
	// 归属一致：本配对不应被判违规
	if _, err := r.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	cur, err := store.PairingByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !cur.LinkageEnabled {
		t.Fatal("owner 一致时不得停联动")
	}

	// 配件转让：解绑 A、绑给 B
	if _, err := db.Exec(ctx, `UPDATE iot_shard.device_binding SET unbound_at=now() WHERE sn=$1 AND user_id=$2`, acc, userA); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO iot_shard.device_binding(sn,user_id,role) VALUES($1,$2,'owner')`, acc, userB); err != nil {
		t.Fatal(err)
	}
	before := m.Get(MOwnerDrift)
	if _, err := r.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if m.Get(MOwnerDrift) <= before || m.Get(MOwnerDisabled) < 1 {
		t.Fatalf("drift=%d (before %d) disabled=%d", m.Get(MOwnerDrift), before, m.Get(MOwnerDisabled))
	}
	cur, err = store.PairingByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cur.LinkageEnabled {
		t.Fatal("转让之后必须停联动")
	}
	if cur.UnpairedAt != nil {
		t.Fatal("停联动不解绑：unpaired_at 必须仍为 NULL")
	}
}
