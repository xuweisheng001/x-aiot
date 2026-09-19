package param

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeStore 是内存版 Store，Publish 直接按 plan 修改内存并调用 snap。
type fakeStore struct {
	profiles []Profile
	releases []Release
	recs     []Recommendation
	cfg      map[string]Cfg
	ups      map[string]UserParam // key user|pk|mod|mat
	nextID   int64
}

func newFake() *fakeStore {
	return &fakeStore{cfg: map[string]Cfg{}, ups: map[string]UserParam{}, nextID: 1}
}

func (f *fakeStore) Profiles(_ context.Context, pk string) ([]Profile, error) {
	var out []Profile
	for _, p := range f.profiles {
		if p.ProductKey == pk {
			out = append(out, p)
		}
	}
	return out, nil
}
func (f *fakeStore) Releases(_ context.Context, pk string) ([]Release, error) {
	var out []Release
	for i := len(f.releases) - 1; i >= 0; i-- {
		if f.releases[i].ProductKey == pk {
			out = append(out, f.releases[i])
		}
	}
	return out, nil
}
func (f *fakeStore) Publish(_ context.Context, plan PublishPlan, snap SnapshotFn) (*Release, error) {
	if plan.ApprovedBy == "" || plan.ApprovedBy == plan.CreatedBy {
		return nil, ErrDenied
	}
	var maxV int64
	for _, r := range f.releases {
		if r.ProductKey == plan.ProductKey && r.Version > maxV {
			maxV = r.Version
		}
	}
	v := maxV + 1
	for _, id := range plan.RemoveIDs {
		for i := range f.profiles {
			if f.profiles[i].ID == id {
				f.profiles[i].VersionRemoved = &v
			}
		}
	}
	for _, p := range plan.Added {
		if pw, ok := ParamNum(p.Params, "power"); !ok || pw < 0 || pw > 100 {
			return nil, ErrBadParam
		}
		p.ID = f.nextID
		f.nextID++
		p.VersionAdded = v
		f.profiles = append(f.profiles, p)
	}
	eff, _ := f.Profiles(context.Background(), plan.ProductKey)
	url, err := snap(v, time.Unix(0, 0), EffectiveAt(eff, v))
	if err != nil {
		return nil, err
	}
	pct := plan.RolloutPct
	if pct <= 0 {
		pct = 100
	}
	r := Release{ProductKey: plan.ProductKey, Version: v, SnapshotURL: url, RolloutPct: pct, CreatedBy: plan.CreatedBy, ApprovedBy: plan.ApprovedBy, RolledBackFrom: plan.RolledBackFrom}
	f.releases = append(f.releases, r)
	return &r, nil
}
func (f *fakeStore) PublishedRecommendations(_ context.Context, pk string) ([]Recommendation, error) {
	return f.recs, nil
}
func (f *fakeStore) CorrectionCfg(_ context.Context, pk, mod string) (Cfg, bool, error) {
	c, ok := f.cfg[pk+"|"+mod]
	return c, ok, nil
}
func (f *fakeStore) UserParams(_ context.Context, uid int64, pk string) ([]UserParam, error) {
	var out []UserParam
	for _, u := range f.ups {
		if u.UserID == uid && u.ProductKey == pk {
			out = append(out, u)
		}
	}
	return out, nil
}
func (f *fakeStore) PutUserParam(_ context.Context, up UserParam, ifMatch int64) (int64, int64, bool, error) {
	k := strings.Join([]string{string(rune(up.UserID)), up.ProductKey, up.ModuleModel, up.MaterialID}, "|")
	cur, exists := f.ups[k]
	if ifMatch == 0 {
		if exists {
			return 0, cur.Version, true, nil
		}
		up.Version = 1
		f.ups[k] = up
		return 1, 1, false, nil
	}
	if !exists || cur.Version != ifMatch {
		return 0, cur.Version, true, nil
	}
	up.Version = cur.Version + 1
	f.ups[k] = up
	return up.Version, up.Version, false, nil
}

func newTestServer(t *testing.T) (*httptest.Server, *fakeStore, *Service) {
	t.Helper()
	fs := newFake()
	svc := NewService(fs, NewMetrics(), t.TempDir())
	srv := httptest.NewServer(Routes(svc, "test"))
	t.Cleanup(srv.Close)
	return srv, fs, svc
}

func do(t *testing.T, method, url, body string, hdr map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	resp.Body.Close()
	return resp, m
}

func publishBody(pk string, added ...string) string {
	return `{"product_key":"` + pk + `","created_by":"ops","approved_by":"lead","added":[` + strings.Join(added, ",") + `]}`
}

func TestHandler_PublishLatestDeltaSnapshot(t *testing.T) {
	srv, _, svc := newTestServer(t)
	// v1：两条档
	resp, m := do(t, "POST", srv.URL+"/internal/params/releases", publishBody("PK",
		`{"module_model":"M40","material_id":"BASSWOOD_3MM","params":{"power":60,"speed":12}}`,
		`{"module_model":"M40","material_id":"ACRYLIC_3MM","params":{"power":80,"speed":8}}`), nil)
	if resp.StatusCode != 201 {
		t.Fatalf("publish v1: %d %v", resp.StatusCode, m)
	}
	// v2：删 id=1 加一条
	resp, m = do(t, "POST", srv.URL+"/internal/params/releases",
		`{"product_key":"PK","created_by":"ops","approved_by":"lead","removed":[1],"added":[{"module_model":"M40","material_id":"LEATHER_1MM","params":{"power":30,"speed":20}}]}`, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("publish v2: %d %v", resp.StatusCode, m)
	}
	// latest
	resp, m = do(t, "GET", srv.URL+"/api/v1/params/releases/latest?product_key=PK", "", nil)
	data := m["data"].(map[string]any)
	if resp.StatusCode != 200 || data["version"].(float64) != 2 || data["snapshot_url"] != "/snapshots/PK/v2.json" {
		t.Fatalf("latest: %d %v", resp.StatusCode, m)
	}
	if resp.Header.Get("ETag") != `"2"` {
		t.Errorf("etag %q", resp.Header.Get("ETag"))
	}
	// delta 1→2：removed 在 added 之前（JSON 字段顺序）
	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/params?product_key=PK&since_version=1", nil)
	raw, _ := http.DefaultClient.Do(req)
	var buf strings.Builder
	b := make([]byte, 4096)
	n, _ := raw.Body.Read(b)
	buf.Write(b[:n])
	raw.Body.Close()
	body := buf.String()
	if !strings.Contains(body, `"removed":[1]`) || strings.Index(body, `"removed"`) > strings.Index(body, `"added"`) {
		t.Fatalf("delta must list removed before added: %s", body)
	}
	if !strings.Contains(body, `"material_id":"LEATHER_1MM"`) {
		t.Fatalf("delta added missing: %s", body)
	}
	// 304
	resp, _ = do(t, "GET", srv.URL+"/api/v1/params?product_key=PK&since_version=2", "", map[string]string{"If-None-Match": `"2"`})
	if resp.StatusCode != 304 {
		t.Fatalf("expected 304, got %d", resp.StatusCode)
	}
	// 快照可读且 immutable
	resp, m = do(t, "GET", srv.URL+"/snapshots/PK/v2.json", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Cache-Control"), "immutable") || m["version"].(float64) != 2 {
		t.Fatalf("snapshot: %d %v %v", resp.StatusCode, resp.Header.Get("Cache-Control"), m)
	}
	if profiles := m["profiles"].([]any); len(profiles) != 2 {
		t.Fatalf("snapshot v2 should have 2 effective profiles, got %d", len(profiles))
	}
	// 路径穿越
	resp, _ = do(t, "GET", srv.URL+"/snapshots/PK/..%2F..%2Fetc", "", nil)
	if resp.StatusCode != 404 {
		t.Fatalf("traversal should 404, got %d", resp.StatusCode)
	}
	if svc.M.Get(MPublished) != 2 || svc.M.Get(MNotModified) != 1 || svc.M.Get(MDeltaServed) != 1 {
		t.Errorf("metrics: published=%d 304=%d delta=%d", svc.M.Get(MPublished), svc.M.Get(MNotModified), svc.M.Get(MDeltaServed))
	}
}

func TestHandler_PublishRejections(t *testing.T) {
	srv, _, _ := newTestServer(t)
	// 审批缺失 → 403
	resp, m := do(t, "POST", srv.URL+"/internal/params/releases", `{"product_key":"PK","created_by":"ops","added":[]}`, nil)
	if resp.StatusCode != 403 || m["code"].(float64) != 10003 {
		t.Fatalf("no approver: %d %v", resp.StatusCode, m)
	}
	// 参数越界 → 400
	resp, m = do(t, "POST", srv.URL+"/internal/params/releases", publishBody("PK", `{"module_model":"M","material_id":"X","params":{"power":150,"speed":1}}`), nil)
	if resp.StatusCode != 400 {
		t.Fatalf("power 150: %d %v", resp.StatusCode, m)
	}
	// 差异校验：先发 12 条，再把 12 条都改 50% → 409 附 violations；force 通过
	var items []string
	for i := 0; i < 12; i++ {
		items = append(items, `{"module_model":"M","material_id":"MAT`+string(rune('A'+i))+`","params":{"power":50,"speed":10}}`)
	}
	resp, m = do(t, "POST", srv.URL+"/internal/params/releases", publishBody("PK", items...), nil)
	if resp.StatusCode != 201 {
		t.Fatalf("seed: %d %v", resp.StatusCode, m)
	}
	var changed []string
	for i := 0; i < 12; i++ {
		changed = append(changed, `{"module_model":"M","material_id":"MAT`+string(rune('A'+i))+`","params":{"power":75,"speed":10}}`)
	}
	resp, m = do(t, "POST", srv.URL+"/internal/params/releases", publishBody("PK", changed...), nil)
	if resp.StatusCode != 409 {
		t.Fatalf("diff should reject: %d %v", resp.StatusCode, m)
	}
	if vs := m["data"].(map[string]any)["violations"].([]any); len(vs) != 12 {
		t.Fatalf("want 12 violations, got %d", len(vs))
	}
	forced := strings.Replace(publishBody("PK", changed...), `"created_by"`, `"force":true,"created_by"`, 1)
	resp, m = do(t, "POST", srv.URL+"/internal/params/releases", forced, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("force should pass: %d %v", resp.StatusCode, m)
	}
}

func TestHandler_RollbackEquivalence(t *testing.T) {
	srv, fs, _ := newTestServer(t)
	do(t, "POST", srv.URL+"/internal/params/releases", publishBody("PK", `{"module_model":"M","material_id":"A","params":{"power":50,"speed":10}}`), nil)
	do(t, "POST", srv.URL+"/internal/params/releases", `{"product_key":"PK","created_by":"ops","approved_by":"lead","removed":[1],"added":[{"module_model":"M","material_id":"B","params":{"power":50,"speed":10}}]}`, nil)
	resp, m := do(t, "POST", srv.URL+"/internal/params/releases/PK/rollback", `{"to_version":1,"created_by":"ops","approved_by":"lead"}`, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("rollback: %d %v", resp.StatusCode, m)
	}
	all, _ := fs.Profiles(context.Background(), "PK")
	v1, v3 := EffectiveAt(all, 1), EffectiveAt(all, 3)
	if len(v3) != 1 || len(v1) != 1 || v3[0].MaterialID != v1[0].MaterialID || v3[0].ID == v1[0].ID {
		t.Fatalf("v3 should equal v1 by content via a new copy: v1=%+v v3=%+v", ids(v1), ids(v3))
	}
	if rb := m["data"].(map[string]any)["rolled_back_from"].(float64); rb != 1 {
		t.Errorf("rolled_back_from=%v", rb)
	}
	// 回滚到不存在的版本 → 404
	resp, _ = do(t, "POST", srv.URL+"/internal/params/releases/PK/rollback", `{"to_version":99,"created_by":"ops","approved_by":"lead"}`, nil)
	if resp.StatusCode != 404 {
		t.Fatalf("missing version: %d", resp.StatusCode)
	}
}

func TestHandler_RolloutVisibility(t *testing.T) {
	srv, _, _ := newTestServer(t)
	do(t, "POST", srv.URL+"/internal/params/releases", publishBody("PK"), nil)
	body := strings.Replace(publishBody("PK"), `"created_by"`, `"rollout_pct":10,"created_by"`, 1)
	do(t, "POST", srv.URL+"/internal/params/releases", body, nil)
	for _, c := range []struct {
		q    string
		want float64
	}{{"&bucket=0.05", 2}, {"&bucket=0.5", 1}, {"", 1}} {
		_, m := do(t, "GET", srv.URL+"/api/v1/params/releases/latest?product_key=PK"+c.q, "", nil)
		if got := m["data"].(map[string]any)["version"].(float64); got != c.want {
			t.Errorf("latest%s = v%v want v%v", c.q, got, c.want)
		}
	}
	resp, _ := do(t, "GET", srv.URL+"/api/v1/params/releases/latest?product_key=PK&bucket=1.5", "", nil)
	if resp.StatusCode != 400 {
		t.Errorf("bad bucket should 400, got %d", resp.StatusCode)
	}
}

func TestHandler_UserParamOptimisticLock(t *testing.T) {
	srv, _, _ := newTestServer(t)
	body := `{"product_key":"PK","module_model":"M","material_id":"A","params":{"power":42}}`
	resp, _ := do(t, "PUT", srv.URL+"/api/v1/user-params", body, map[string]string{"If-Match": `"0"`})
	if resp.StatusCode != 401 {
		t.Fatalf("no user → 401, got %d", resp.StatusCode)
	}
	hdr := map[string]string{"X-User-Id": "7", "If-Match": `"0"`}
	resp, m := do(t, "PUT", srv.URL+"/api/v1/user-params", body, hdr)
	if resp.StatusCode != 200 || m["data"].(map[string]any)["version"].(float64) != 1 {
		t.Fatalf("create: %d %v", resp.StatusCode, m)
	}
	resp, m = do(t, "PUT", srv.URL+"/api/v1/user-params", body, hdr) // 再次以 0 创建 → 409
	if resp.StatusCode != 409 || m["data"].(map[string]any)["current_version"].(float64) != 1 {
		t.Fatalf("dup create: %d %v", resp.StatusCode, m)
	}
	hdr["If-Match"] = `"1"`
	resp, m = do(t, "PUT", srv.URL+"/api/v1/user-params", body, hdr)
	if resp.StatusCode != 200 || m["data"].(map[string]any)["version"].(float64) != 2 {
		t.Fatalf("update: %d %v", resp.StatusCode, m)
	}
	resp, _ = do(t, "PUT", srv.URL+"/api/v1/user-params", body, hdr) // 旧版本 → 409
	if resp.StatusCode != 409 {
		t.Fatalf("stale: %d", resp.StatusCode)
	}
	resp, _ = do(t, "PUT", srv.URL+"/api/v1/user-params", body, map[string]string{"X-User-Id": "7"})
	if resp.StatusCode != 400 {
		t.Fatalf("missing If-Match: %d", resp.StatusCode)
	}
	resp, m = do(t, "GET", srv.URL+"/api/v1/user-params?product_key=PK", "", map[string]string{"X-User-Id": "7"})
	if resp.StatusCode != 200 || len(m["data"].([]any)) != 1 {
		t.Fatalf("list: %d %v", resp.StatusCode, m)
	}
}

func TestHandler_Correction(t *testing.T) {
	srv, fs, _ := newTestServer(t)
	fs.cfg["PK|M40"] = Cfg{A: 0.2, B: 0, C: 0, RatedHours: 1000}
	_, m := do(t, "GET", srv.URL+"/api/v1/devices/SN1/correction?product_key=PK&module_model=M40&laser_hours=100&health=50", "", nil)
	d := m["data"].(map[string]any)
	if d["k_power"].(float64) != 1.1 || d["k_speed"].(float64) != 1 {
		t.Fatalf("cfg override: %v", d)
	}
	_, m = do(t, "GET", srv.URL+"/api/v1/devices/SN1/correction?product_key=PK&module_model=M40&laser_hours=100", "", nil)
	d = m["data"].(map[string]any)
	if d["k_power"].(float64) != 1 || d["reasons"].([]any)[0].(map[string]any)["type"] != "input_missing" {
		t.Fatalf("missing health: %v", d)
	}
}
