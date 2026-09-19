package accessory

import (
	"context"
	"testing"
)

func i64(v int64) *int64 { return &v }

func TestOwnerDrift(t *testing.T) {
	cases := []struct {
		name       string
		host, acc  *int64
		wantViol   bool
		wantReason string
	}{
		{name: "同一 owner：合规", host: i64(1), acc: i64(1)},
		{name: "owner 不同", host: i64(1), acc: i64(2), wantViol: true, wantReason: ReasonOwnerMismatch},
		{name: "主机无绑定", host: nil, acc: i64(2), wantViol: true, wantReason: ReasonHostUnbound},
		{name: "配件无绑定", host: i64(1), acc: nil, wantViol: true, wantReason: ReasonAccUnbound},
		{name: "两端都无绑定：先报主机", host: nil, acc: nil, wantViol: true, wantReason: ReasonHostUnbound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			viol, reason := OwnerDrift(c.host, c.acc)
			if viol != c.wantViol || reason != c.wantReason {
				t.Fatalf("OwnerDrift=(%v,%q) want (%v,%q)", viol, reason, c.wantViol, c.wantReason)
			}
		})
	}
}

func TestOwnerReconcilerRunOnce(t *testing.T) {
	st := newFakeStore()
	ctx := context.Background()
	mk := func(host, acc string, enabled bool) int64 {
		p, _, err := st.Pair(ctx, host, acc, "purifier", enabled, 180, "k")
		if err != nil {
			t.Fatal(err)
		}
		return p.ID
	}
	okID := mk("H_OK", "A_OK", true)            // owner 一致
	sharedID := mk("H_SH", "A_SH", true)        // 多 owner 有交集
	driftID := mk("H_DR", "A_DR", true)         // owner 不一致
	unboundID := mk("H_UN", "A_UN", true)       // 配件无绑定
	alreadyOffID := mk("H_OFF", "A_OFF", false) // 已停联动的越权配对：只报不重复写
	st.owners = map[string][]int64{
		"H_OK": {7}, "A_OK": {7},
		"H_SH": {7, 9}, "A_SH": {9},
		"H_DR": {7}, "A_DR": {8},
		"H_UN":  {7},
		"H_OFF": {7}, "A_OFF": {8},
	}

	m := NewMetrics()
	r := NewOwnerReconciler(st, nil, m)
	rep, err := r.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Checked != 5 || rep.Drift != 3 || rep.Disabled != 2 || rep.Errors != 0 {
		t.Fatalf("report=%+v", rep)
	}
	if rep.Reasons[ReasonOwnerMismatch] != 2 || rep.Reasons[ReasonAccUnbound] != 1 {
		t.Fatalf("reasons=%v", rep.Reasons)
	}
	enabled := func(id int64) bool {
		p, err := st.PairingByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return p.LinkageEnabled
	}
	if !enabled(okID) || !enabled(sharedID) {
		t.Fatal("合规配对不得被停联动")
	}
	if enabled(driftID) || enabled(unboundID) {
		t.Fatal("越权配对必须停联动")
	}
	if enabled(alreadyOffID) {
		t.Fatal("已停的仍应是停的")
	}
	// 停联动不解绑：配对行仍然有效
	for _, id := range []int64{driftID, unboundID} {
		p, _ := st.PairingByID(ctx, id)
		if p.UnpairedAt != nil {
			t.Fatalf("pairing %d 不应被解绑", id)
		}
	}
	if m.Get(MOwnerReconcileRuns) != 1 || m.Get(MOwnerChecked) != 5 || m.Get(MOwnerDrift) != 3 || m.Get(MOwnerDisabled) != 2 {
		t.Fatalf("metrics runs=%d checked=%d drift=%d disabled=%d",
			m.Get(MOwnerReconcileRuns), m.Get(MOwnerChecked), m.Get(MOwnerDrift), m.Get(MOwnerDisabled))
	}

	// 幂等：再跑一轮只报不再写
	rep2, err := r.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Drift != 3 || rep2.Disabled != 0 {
		t.Fatalf("second round=%+v", rep2)
	}
}
