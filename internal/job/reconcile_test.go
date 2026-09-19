package job

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestViolatingSNs(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]OptInState
		want []string
	}{
		{name: "空输入"},
		{name: "全部同意", in: map[string]OptInState{"A": OptInAllowed, "B": OptInAllowed}},
		{name: "明确拒绝", in: map[string]OptInState{"A": OptInFalse}, want: []string{"A"}},
		{name: "字段缺失也算违规", in: map[string]OptInState{"A": OptInMissing}, want: []string{"A"}},
		{name: "Redis 出错不算违规", in: map[string]OptInState{"A": OptInErr}},
		{
			name: "混合：只挑 false/missing，且升序",
			in:   map[string]OptInState{"C": OptInFalse, "A": OptInMissing, "B": OptInAllowed, "D": OptInErr},
			want: []string{"A", "C"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ViolatingSNs(c.in)
			if len(got) == 0 && len(c.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

type fakeSNs struct {
	sns []string
	err error
}

func (f *fakeSNs) DistinctJobSNs(context.Context) ([]string, error) { return f.sns, f.err }

func TestOptInReconcilerRunOnce(t *testing.T) {
	st := newFakeStore()
	st.records["j1"], st.records["j2"], st.records["j3"] = "SN_BAD", "SN_OK", "SN_ERR"
	shadowErr := errors.New("redis down")
	sh := ShadowReaderFunc(func(_ context.Context, sn string) (map[string]string, error) {
		switch sn {
		case "SN_OK":
			return map[string]string{OptInField: "true"}, nil
		case "SN_BAD":
			return map[string]string{OptInField: "false"}, nil
		case "SN_MISSING":
			return map[string]string{}, nil
		}
		return nil, shadowErr // SN_ERR
	})
	svc := NewService(st, sh, NewMetrics())
	r := NewOptInReconciler(svc, &fakeSNs{sns: []string{"SN_BAD", "SN_OK", "SN_ERR", "SN_MISSING"}})

	rep, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Scanned != 4 || rep.Violations != 2 || rep.Purged != 2 || rep.ShadowErrors != 1 {
		t.Fatalf("report=%+v", rep)
	}
	// SN_BAD 的记录被删；SN_OK（同意）与 SN_ERR（读不到）的都还在
	if _, ok := st.records["j1"]; ok {
		t.Fatal("SN_BAD job record must be purged")
	}
	if _, ok := st.records["j2"]; !ok {
		t.Fatal("SN_OK job record must survive")
	}
	if _, ok := st.records["j3"]; !ok {
		t.Fatal("SN_ERR 的数据不能因为 Redis 抖动被删")
	}
	if svc.M.Get(MOptInReconcileRuns) != 1 || svc.M.Get(MOptInChecked) != 4 ||
		svc.M.Get(MOptInViolations) != 2 || svc.M.Get(MOptInPurged) != 2 || svc.M.Get(MOptInShadowErrors) != 1 {
		t.Fatalf("metrics runs=%d checked=%d violations=%d purged=%d shadow_errors=%d",
			svc.M.Get(MOptInReconcileRuns), svc.M.Get(MOptInChecked), svc.M.Get(MOptInViolations),
			svc.M.Get(MOptInPurged), svc.M.Get(MOptInShadowErrors))
	}
	// 对账不得污染消费链路的丢弃口径
	if svc.M.Get("dropped_optin_false") != 0 || svc.M.Get("dropped_optin_err") != 0 {
		t.Fatal("reconcile must not touch dropped_optin_* counters")
	}

	// 列 SN 失败：整轮放弃，不删任何东西
	r2 := NewOptInReconciler(svc, &fakeSNs{err: errors.New("pg down")})
	if _, err := r2.RunOnce(context.Background()); err == nil {
		t.Fatal("list error must surface")
	}
	if svc.M.Get(MOptInReconcileErrors) != 1 {
		t.Fatalf("reconcile_errors=%d", svc.M.Get(MOptInReconcileErrors))
	}
}
