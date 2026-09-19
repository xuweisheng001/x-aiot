package param

import (
	"encoding/json"
	"reflect"
	"testing"
)

func i64(v int64) *int64 { return &v }

func prof(id int64, mod, mat string, power, speed float64, added int64, removed *int64) Profile {
	params, _ := json.Marshal(map[string]any{"power": power, "speed": speed})
	return Profile{ID: id, ProductKey: "PK", ModuleModel: mod, MaterialID: mat, Params: params, Source: SourceOfficial, VersionAdded: added, VersionRemoved: removed}
}

func ids(ps []Profile) []int64 {
	out := []int64{}
	for _, p := range ps {
		out = append(out, p.ID)
	}
	return out
}

func TestEffectiveAt(t *testing.T) {
	ps := []Profile{
		prof(1, "M", "A", 50, 10, 1, nil),    // v1 起一直有效
		prof(2, "M", "B", 50, 10, 1, i64(3)), // v1..v2 有效，v3 移除
		prof(3, "M", "C", 50, 10, 3, nil),    // v3 起有效
		prof(4, "M", "D", 50, 10, 2, i64(2)), // 非法生命周期不会出现（CHECK），但函数应视为从不有效
	}
	cases := []struct {
		v    int64
		want []int64
	}{
		{0, []int64{}},
		{1, []int64{1, 2}},
		{2, []int64{1, 2}},
		{3, []int64{1, 3}},
		{99, []int64{1, 3}},
	}
	for _, c := range cases {
		if got := ids(EffectiveAt(ps, c.v)); !reflect.DeepEqual(got, c.want) {
			t.Errorf("EffectiveAt(v=%d)=%v want %v", c.v, got, c.want)
		}
	}
}

func TestDelta(t *testing.T) {
	ps := []Profile{
		prof(1, "M", "A", 50, 10, 1, nil),
		prof(2, "M", "B", 50, 10, 1, i64(3)), // since=2 有效, to=4 无效 → removed
		prof(3, "M", "C", 50, 10, 3, nil),    // since=2 无效, to=4 有效 → added
		prof(4, "M", "D", 50, 10, 3, i64(4)), // 区间内先加后删 → 两边都不出现
		prof(5, "M", "E", 50, 10, 1, i64(2)), // since 之前已删 → 不出现
	}
	cases := []struct {
		name        string
		since, to   int64
		wantRemoved []int64
		wantAdded   []int64
	}{
		{"2→4", 2, 4, []int64{2}, []int64{3}},
		{"0→4 全量等价", 0, 4, []int64{}, []int64{1, 3}},
		{"4→4 无变化", 4, 4, []int64{}, []int64{}},
		{"1→2 只删 5", 1, 2, []int64{5}, []int64{}},
	}
	for _, c := range cases {
		r, a := Delta(ps, c.since, c.to)
		if !reflect.DeepEqual(r, c.wantRemoved) || !reflect.DeepEqual(ids(a), c.wantAdded) {
			t.Errorf("%s: removed=%v added=%v want %v/%v", c.name, r, ids(a), c.wantRemoved, c.wantAdded)
		}
	}
}

func TestDiffValidate(t *testing.T) {
	old := []Profile{prof(1, "M", "A", 50, 10, 1, nil), prof(2, "M", "B", 80, 20, 1, nil), prof(3, "M", "C", 40, 5, 1, nil)}
	cases := []struct {
		name string
		new  []Profile
		want int
	}{
		{"无变化", []Profile{prof(1, "M", "A", 50, 10, 2, nil)}, 0},
		{"power 变 30% 恰好不超", []Profile{prof(1, "M", "A", 65, 10, 2, nil)}, 0},
		{"power 变 31%", []Profile{prof(1, "M", "A", 65.5, 10, 2, nil)}, 1},
		{"power 与 speed 都超", []Profile{prof(2, "M", "B", 10, 100, 2, nil)}, 2},
		{"新 key 不比较", []Profile{prof(9, "M", "Z", 100, 100, 2, nil)}, 0},
		{"不同 source 不比较", func() []Profile {
			p := prof(3, "M", "C", 100, 100, 2, nil)
			p.Source = SourceRecommended
			return []Profile{p}
		}(), 0},
	}
	for _, c := range cases {
		if got := DiffValidate(old, c.new, DiffPctThreshold, DiffMaxCount); len(got) != c.want {
			t.Errorf("%s: got %d violations %+v want %d", c.name, len(got), got, c.want)
		}
	}
	// 阈值判定：恰好 10 条不拒绝，11 条拒绝
	vs := make([]Violation, 10)
	if DiffRejected(vs, DiffMaxCount) {
		t.Error("10 violations should not reject")
	}
	if !DiffRejected(append(vs, Violation{}), DiffMaxCount) {
		t.Error("11 violations should reject")
	}
}

func TestRollbackPlan(t *testing.T) {
	cur := []Profile{prof(1, "M", "A", 1, 1, 1, nil), prof(3, "M", "C", 1, 1, 3, nil)}
	tgt := []Profile{prof(1, "M", "A", 1, 1, 1, nil), prof(2, "M", "B", 1, 1, 1, i64(3))}
	rm, re := RollbackPlan(cur, tgt)
	if !reflect.DeepEqual(rm, []int64{3}) {
		t.Errorf("toRemove=%v want [3]", rm)
	}
	if len(re) != 1 || re[0].MaterialID != "B" || re[0].ID != 0 || re[0].VersionRemoved != nil {
		t.Errorf("toReadd=%+v want copy of B with ID=0 and no removed", re)
	}
	// 目标与当前相同 → 空计划
	rm, re = RollbackPlan(cur, cur)
	if len(rm) != 0 || len(re) != 0 {
		t.Errorf("identical sets should yield empty plan: %v %v", rm, re)
	}
}

func TestBucketAndRollout(t *testing.T) {
	b := Bucket("user-42")
	if b < 0 || b >= 1 || b != Bucket("user-42") {
		t.Fatalf("bucket out of range or unstable: %v", b)
	}
	cases := []struct {
		bucket float64
		has    bool
		pct    int
		want   bool
	}{
		{0.5, true, 100, true}, {0.5, false, 100, true},
		{0.05, true, 10, true}, {0.10, true, 10, false}, {0.5, true, 10, false},
		{0.5, false, 10, false}, {0.0, true, 0, false},
	}
	for _, c := range cases {
		if got := InRollout(c.bucket, c.has, c.pct); got != c.want {
			t.Errorf("InRollout(%v,%v,%d)=%v want %v", c.bucket, c.has, c.pct, got, c.want)
		}
	}
	rels := []Release{{Version: 1, RolloutPct: 100}, {Version: 3, RolloutPct: 10}, {Version: 2, RolloutPct: 100}}
	if r, ok := VisibleVersion(rels, 0.05, true); !ok || r.Version != 3 {
		t.Errorf("bucket 0.05 should see v3, got %+v", r)
	}
	if r, ok := VisibleVersion(rels, 0.5, true); !ok || r.Version != 2 {
		t.Errorf("bucket 0.5 should see v2, got %+v", r)
	}
	if r, ok := VisibleVersion(rels, 0, false); !ok || r.Version != 2 {
		t.Errorf("no bucket should see v2, got %+v", r)
	}
	if _, ok := VisibleVersion([]Release{{Version: 1, RolloutPct: 10}}, 0, false); ok {
		t.Error("no bucket and only 10% release → not visible")
	}
}

func TestCanonicalHash(t *testing.T) {
	a := CanonicalHash(json.RawMessage(`{"speed":10,"power":50}`))
	b := CanonicalHash(json.RawMessage(`{ "power": 50, "speed": 10 }`))
	if a == "" || a != b || len(a) != 64 {
		t.Errorf("canonical hash should ignore key order/whitespace: %q vs %q", a, b)
	}
	if CanonicalHash(json.RawMessage(`{"power":51,"speed":10}`)) == a {
		t.Error("different params must differ")
	}
	if CanonicalHash(json.RawMessage(`not json`)) != "" {
		t.Error("invalid json → empty")
	}
}

func TestCorrection(t *testing.T) {
	cfg := DefaultCfg
	cases := []struct {
		name                    string
		hours, health           float64
		hoursKnown, healthKnown bool
		wantKP, wantKS          float64
		wantReason              string
	}{
		{"全新模块", 0, 100, true, true, 1, 1, ""},
		{"health 缺失", 100, 0, true, false, 1, 1, "input_missing"},
		{"hours 缺失", 0, 50, false, true, 1, 1, "input_missing"},
		{"中度磨损", 2000, 60, true, true, 1.07, 0.98, "health"},
		{"极端磨损触上限", 100000, 0, true, true, KMax, 0.95, "limit"},
		{"health 越界被 clamp", 0, 150, true, true, 1, 1, ""},
	}
	for _, c := range cases {
		r := Correction(cfg, c.hours, c.health, c.healthKnown, c.hoursKnown)
		if r.KPower != c.wantKP || r.KSpeed != c.wantKS {
			t.Errorf("%s: k=(%v,%v) want (%v,%v)", c.name, r.KPower, r.KSpeed, c.wantKP, c.wantKS)
		}
		if r.KPower < KMin || r.KPower > KMax || r.KSpeed < KMin || r.KSpeed > KMax {
			t.Errorf("%s: out of hard bounds", c.name)
		}
		if c.wantReason != "" {
			found := false
			for _, rs := range r.Reasons {
				if rs.Type == c.wantReason {
					found = true
				}
			}
			if !found {
				t.Errorf("%s: reasons %+v lack %q", c.name, r.Reasons, c.wantReason)
			}
		}
	}
	// 配置 rated_hours<=0 回退默认，不 panic
	r := Correction(Cfg{A: 0.15, B: 0.05, C: 0.05}, 1000, 100, true, true)
	if r.KPower != 1.005 {
		t.Errorf("rated_hours fallback: got %v", r.KPower)
	}
}

func TestParamNumAndIdent(t *testing.T) {
	if v, ok := ParamNum(json.RawMessage(`{"power":"55.5"}`), "power"); !ok || v != 55.5 {
		t.Errorf("string number: %v %v", v, ok)
	}
	if _, ok := ParamNum(json.RawMessage(`{"power":true}`), "power"); ok {
		t.Error("bool should not parse")
	}
	for s, want := range map[string]bool{"LM_S1": true, "BASSWOOD_3MM": true, "": false, "a b": false, "x/y": false} {
		if ValidIdent(s) != want {
			t.Errorf("ValidIdent(%q)=%v", s, !want)
		}
	}
}
