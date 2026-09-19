package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ===== 材料码：假码与可疑码绝不带出参数（FR-341 / INC-3-14）=====

func TestMaterialCodeSignature(t *testing.T) {
	master := []byte("dev-master-key")
	keyFor := func(b uint32) []byte { return DeriveBatchKey(master, b) }
	code := EncodeCode(7, 123, 4567, keyFor(123))
	if len(code) != CodeLen {
		t.Fatalf("code length %d want %d", len(code), CodeLen)
	}
	got, err := DecodeVerify(code, keyFor)
	if err != nil || got.MaterialIdx != 7 || got.Batch != 123 || got.Seq != 4567 {
		t.Fatalf("roundtrip: %+v %v", got, err)
	}
	// 每个批次派生独立密钥：拿别的批次的密钥验不过（批次级吊销的前提）
	otherKey := func(uint32) []byte { return DeriveBatchKey(master, 999) }
	if _, err := DecodeVerify(code, otherKey); !errors.Is(err, ErrBadCode) {
		t.Fatal("a code must not verify under another batch key")
	}
	// 篡改任意一位
	bad := []byte(code)
	if bad[0] == 'A' {
		bad[0] = 'B'
	} else {
		bad[0] = 'A'
	}
	if _, err := DecodeVerify(string(bad), keyFor); !errors.Is(err, ErrBadCode) {
		t.Fatal("tampered code must fail")
	}
	for _, c := range []string{"", "TOOSHORT", strings.Repeat("A", CodeLen+1), strings.Repeat("1", CodeLen)} {
		if _, err := DecodeVerify(c, keyFor); !errors.Is(err, ErrBadCode) {
			t.Fatalf("malformed %q must fail", c)
		}
	}
	// batch 只有 24 位，高位被截断后仍自洽
	hi := EncodeCode(1, 0x1FFFFFF, 1, keyFor(0xFFFFFF))
	if c, err := DecodeVerify(hi, keyFor); err != nil || c.Batch != 0xFFFFFF {
		t.Fatalf("24-bit batch: %+v %v", c, err)
	}
}

// ===== fake store =====

type fakeStore struct {
	mu      sync.Mutex
	health  map[string]*HealthRow
	rem     map[int64]*Reminder
	byS     map[string][]Reminder
	skus    []SKU
	codes   map[string]*MaterialCode
	mats    map[uint32]*MaterialInfo
	consume func(code string) (bool, string)
	clicks  int
	attrib  []string
	attrErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		health: map[string]*HealthRow{},
		rem:    map[int64]*Reminder{},
		byS:    map[string][]Reminder{},
		codes:  map[string]*MaterialCode{},
		mats:   map[uint32]*MaterialInfo{},
	}
}

func (f *fakeStore) HealthRow(_ context.Context, sn, _ string) (*HealthRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.health[sn], nil
}
func (f *fakeStore) Explain(context.Context, string, string) (*Explain, error) { return nil, nil }
func (f *fakeStore) Cfg(context.Context, string, string) (Cfg, bool, error) {
	return DefaultCfg(), true, nil
}
func (f *fakeStore) WriteHealth(context.Context, HealthUpdate) error                { return nil }
func (f *fakeStore) LastSentAt(context.Context, string, string) (*time.Time, error) { return nil, nil }
func (f *fakeStore) InsertReminder(_ context.Context, r *Reminder) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r.ID = int64(len(f.rem) + 1)
	cp := *r
	f.rem[r.ID] = &cp
	f.byS[r.SN] = append(f.byS[r.SN], cp)
	return nil
}
func (f *fakeStore) Reminders(_ context.Context, sn string) ([]Reminder, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.byS[sn], nil
}
func (f *fakeStore) Reminder(_ context.Context, id int64) (*Reminder, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rem[id]
	if !ok {
		return nil, ErrNotFound
	}
	return r, nil
}
func (f *fakeStore) SKUs(_ context.Context, pk, part string) ([]SKU, error) {
	var out []SKU
	for _, s := range f.skus {
		if s.ProductKey == pk && (part == "" || s.Part == part) {
			out = append(out, s)
		}
	}
	return out, nil
}
func (f *fakeStore) Click(_ context.Context, _ int64, _ int64, _ string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clicks++
	return nil
}
func (f *fakeStore) Attribute(_ context.Context, _ int64, orderNo, _ string, _ time.Time) error {
	if f.attrErr != nil {
		return f.attrErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attrib = append(f.attrib, orderNo)
	return nil
}
func (f *fakeStore) MaterialCode(_ context.Context, id string) (*MaterialCode, error) {
	if c, ok := f.codes[id]; ok {
		return c, nil
	}
	return nil, ErrNotFound
}
func (f *fakeStore) MaterialByIdx(_ context.Context, idx uint32) (*MaterialInfo, error) {
	if m, ok := f.mats[idx]; ok {
		return m, nil
	}
	return nil, ErrNotFound
}
func (f *fakeStore) InsertMaterialCode(_ context.Context, mc MaterialCode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := mc
	f.codes[mc.CodeID] = &cp
	return nil
}
func (f *fakeStore) ConsumeScan(_ context.Context, code string, _ *int64, _ string, _ int, _ time.Time) (bool, string, error) {
	if f.consume != nil {
		ok, reason := f.consume(code)
		return ok, reason, nil
	}
	if _, ok := f.codes[code]; !ok {
		return false, "unknown", nil
	}
	return true, "", nil
}

type fakeShadow struct{ rep Reported }

func (f *fakeShadow) Reported(context.Context, string) (Reported, error) { return f.rep, nil }

func newTestSvc(st Store) *Service {
	svc := NewService(st, nil, &fakeShadow{}, nil, NewMetrics(), Options{MaterialKey: []byte("test-master")})
	svc.Now = func() time.Time { return now }
	return svc
}

func do(h http.Handler, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "10.0.0.9:1111"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func body(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var r struct {
		Code int            `json:"code"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("bad json: %s", rec.Body.String())
	}
	return r.Data
}

func TestHandlerHealthAndStale(t *testing.T) {
	st := newFakeStore()
	st.health["SN1"] = &HealthRow{SN: "SN1", Part: PartModule, Health: 73, UpdatedAt: now.Add(-30 * time.Minute)}
	st.health["SN2"] = &HealthRow{SN: "SN2", Part: PartModule, Health: 41, UpdatedAt: now.Add(-5 * time.Hour)}
	h := Routes(newTestSvc(st), "test")

	rec := do(h, http.MethodGet, "/api/v1/health/SN1", "", nil)
	if rec.Code != http.StatusOK || body(t, rec)["stale"] != false {
		t.Fatalf("fresh: %d %s", rec.Code, rec.Body.String())
	}
	rec = do(h, http.MethodGet, "/api/v1/health/SN2", "", nil)
	if rec.Code != http.StatusOK || body(t, rec)["stale"] != true {
		t.Fatalf("stale: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, http.MethodGet, "/api/v1/health/NOPE", "", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("missing device: %d", rec.Code)
	}
}

func TestHandlerRemindersSKUClickAttribute(t *testing.T) {
	st := newFakeStore()
	st.skus = []SKU{{ID: 1, Part: PartModule, ProductKey: "LM_S1", ModuleModel: "LM40", SkuID: "SKU-LM40", Title: "40W 模块", Sellable: true}}
	sent := now.Add(-time.Hour)
	_ = st.InsertReminder(context.Background(), &Reminder{SN: "SN1", Part: PartModule, Level: "80", HealthAt: 79, SkuID: "SKU-LM40", SentAt: &sent, CreatedAt: sent})
	h := Routes(newTestSvc(st), "test")

	if rec := do(h, http.MethodGet, "/api/v1/health/SN1/reminders", "", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "SKU-LM40") {
		t.Fatalf("reminders: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, http.MethodGet, "/api/v1/sku?product_key=LM_S1&part=module", "", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "SKU-LM40") {
		t.Fatalf("sku: %s", rec.Body.String())
	}
	if rec := do(h, http.MethodGet, "/api/v1/sku", "", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("product_key required: %d", rec.Code)
	}
	// 点击归因必须带用户身份
	if rec := do(h, http.MethodPost, "/api/v1/reminders/1/click", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("click without user: %d", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/api/v1/reminders/1/click", "", map[string]string{HeaderUserID: "42"}); rec.Code != http.StatusOK {
		t.Fatalf("click: %d %s", rec.Code, rec.Body.String())
	}
	if st.clicks != 1 {
		t.Fatalf("clicks=%d", st.clicks)
	}
	if rec := do(h, http.MethodPost, "/api/v1/reminders/abc/click", "", map[string]string{HeaderUserID: "42"}); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad id: %d", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/internal/orders/attribute", `{"reminder_id":1,"order_no":"PO-9"}`, nil); rec.Code != http.StatusOK {
		t.Fatalf("attribute: %d %s", rec.Code, rec.Body.String())
	}
	if len(st.attrib) != 1 || st.attrib[0] != "PO-9" {
		t.Fatalf("attributed=%v", st.attrib)
	}
	if rec := do(h, http.MethodPost, "/internal/orders/attribute", `{"reminder_id":0,"order_no":""}`, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty attribute: %d", rec.Code)
	}
}

func TestHandlerVerifyNeverLeaksParamsOnBadCode(t *testing.T) {
	st := newFakeStore()
	svc := newTestSvc(st)
	th := 3.0
	st.mats[7] = &MaterialInfo{MaterialID: "BASSWOOD_3MM", ThicknessMM: &th}
	good, err := svc.MintCode(context.Background(), 7, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	h := Routes(svc, "test")

	rec := do(h, http.MethodPost, "/api/v1/materials/verify", `{"code":"`+good+`","sn":"SN1"}`, nil)
	d := body(t, rec)
	if rec.Code != http.StatusOK || d["valid"] != true || d["material_id"] != "BASSWOOD_3MM" {
		t.Fatalf("good code: %d %s", rec.Code, rec.Body.String())
	}

	// 假码：valid=false 且响应里不得出现任何参数字段
	bad := strings.Repeat("A", CodeLen)
	rec = do(h, http.MethodPost, "/api/v1/materials/verify", `{"code":"`+bad+`"}`, nil)
	d = body(t, rec)
	if d["valid"] != false || d["reason"] != "bad_signature" {
		t.Fatalf("forged code: %s", rec.Body.String())
	}
	for _, k := range []string{"material_id", "batch", "thickness_mm"} {
		if _, ok := d[k]; ok {
			t.Fatalf("forged code leaked %q: %s", k, rec.Body.String())
		}
	}

	// 可疑码（重放 / 吊销）同样不带参数
	for _, reason := range []string{"replay", "revoked", "suspicious"} {
		st.consume = func(string) (bool, string) { return false, reason }
		rec = do(h, http.MethodPost, "/api/v1/materials/verify", `{"code":"`+good+`","sn":"SN1"}`, nil)
		d = body(t, rec)
		if d["valid"] != false || d["reason"] != reason {
			t.Fatalf("%s: %s", reason, rec.Body.String())
		}
		if _, ok := d["material_id"]; ok {
			t.Fatalf("%s leaked material_id: %s", reason, rec.Body.String())
		}
	}
	st.consume = nil

	if rec := do(h, http.MethodPost, "/api/v1/materials/verify", `{"code":""}`, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty code: %d", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/api/v1/materials/verify", `not json`, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json: %d", rec.Code)
	}
}

func TestHandlerHealthzMetrics(t *testing.T) {
	h := Routes(newTestSvc(newFakeStore()), "test")
	if rec := do(h, http.MethodGet, "/healthz", "", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "health-svc") {
		t.Fatalf("healthz: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, http.MethodGet, "/metrics", "", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), MBatchRuns) {
		t.Fatalf("metrics: %d", rec.Code)
	}
}
