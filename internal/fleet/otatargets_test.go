package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

// ---- fake：在 fleet_test.go 的 fakeStore 之上补 BL5 §08/§09 需要的几张表 ----

type otaFakeStore struct {
	*fakeStore
	mu         sync.Mutex
	platform   map[int64]*PlatformBatch // firmware_id → 最新平台批次
	orgBatches []OrgOTABatch
	subs       map[int64][]AlarmTarget // site_id → 站点订阅用户
	personal   map[string][]int64      // sn → 个人绑定用户
}

func newOTAFakeStore() *otaFakeStore {
	return &otaFakeStore{fakeStore: newFakeStore(), platform: map[int64]*PlatformBatch{},
		subs: map[int64][]AlarmTarget{}, personal: map[string][]int64{}}
}

func (f *otaFakeStore) OrgSNSet(ctx context.Context, orgID int64, sns []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, sn := range sns {
		if d, err := f.GetDevice(ctx, orgID, sn); err == nil && d != nil {
			out[sn] = true
		}
	}
	return out, nil
}

func (f *otaFakeStore) LatestPlatformBatch(_ context.Context, firmwareID int64) (*PlatformBatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b, ok := f.platform[firmwareID]; ok {
		return b, nil
	}
	return nil, ErrNotFound
}

func (f *otaFakeStore) InsertOrgOTABatch(_ context.Context, b OrgOTABatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.orgBatches = append(f.orgBatches, b)
	return nil
}

func (f *otaFakeStore) ListOrgOTABatches(_ context.Context, orgID int64) ([]OrgOTABatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []OrgOTABatch{}
	for _, b := range f.orgBatches {
		if b.OrgID == orgID {
			out = append(out, b)
		}
	}
	return out, nil
}

func (f *otaFakeStore) SiteSubscribers(_ context.Context, _ int64, siteID int64) ([]AlarmTarget, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subs[siteID], nil
}

func (f *otaFakeStore) PersonalUsers(_ context.Context, sn string) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.personal[sn], nil
}

type fakeOTA struct {
	mu   sync.Mutex
	reqs []OTABatchReq
	resp *OTABatchResp
	err  error
}

func (f *fakeOTA) CreateBatch(_ context.Context, r OTABatchReq) (*OTABatchResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, r)
	if f.err != nil {
		return nil, f.err
	}
	if f.resp != nil {
		return f.resp, nil
	}
	return &OTABatchResp{ID: 777, Stage: "10", Status: "running", TargetTotal: len(r.ExplicitSNs)}, nil
}

func (f *fakeOTA) last() (OTABatchReq, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reqs) == 0 {
		return OTABatchReq{}, false
	}
	return f.reqs[len(f.reqs)-1], true
}

// seedOTA：org A（1 admin / 2 teacher / 3 student）有站点 3 与两台设备，org B（admin 9）有一台设备。
func seedOTA(t *testing.T) (*otaFakeStore, *fakeOTA, *Service, http.Handler) {
	t.Helper()
	st := newOTAFakeStore()
	ctx := context.Background()
	a, _ := st.CreateOrg(ctx, "A School", "school", "US", 1)
	b, _ := st.CreateOrg(ctx, "B Studio", "studio", "US", 9)
	_ = st.AddMember(ctx, a.OrgID, 2, RoleTeacher, 1)
	_ = st.AddMember(ctx, a.OrgID, 3, RoleStudent, 1)
	siteA, _ := st.CreateSite(ctx, a.OrgID, "Room 1", "UTC")
	siteB, _ := st.CreateSite(ctx, b.OrgID, "Studio", "UTC")
	for _, sn := range []string{"SNA1", "SNA2"} {
		if err := st.AttachDevice(ctx, DeviceOrg{SN: sn, OrgID: a.OrgID, SiteID: siteA.SiteID, AssignedBy: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.AttachDevice(ctx, DeviceOrg{SN: "SNB1", OrgID: b.OrgID, SiteID: siteB.SiteID, AssignedBy: 9}); err != nil {
		t.Fatal(err)
	}
	st.platform[42] = &PlatformBatch{ID: 100, Stage: "10", Status: "running"}
	ota := &fakeOTA{}
	svc := newTestSvc(st.fakeStore, nil)
	svc.Store = st // 用带 BL5 扩展的 fake
	svc.OTA = ota
	return st, ota, svc, Routes(svc, "test")
}

// ---- 纯函数 ----

func TestValidateWindowTable(t *testing.T) {
	cases := []struct {
		name string
		w    OTAWindow
		ok   bool
	}{
		{"零值", OTAWindow{}, true},
		{"正常", OTAWindow{Start: "22:00", End: "06:00", TZ: "Asia/Shanghai"}, true},
		{"带 weekdays", OTAWindow{Start: "00:00", End: "05:00", Weekdays: []int{0, 6}}, true},
		{"只有 weekdays", OTAWindow{Weekdays: []int{3}}, true},
		{"只给 start", OTAWindow{Start: "22:00"}, false},
		{"只给 end", OTAWindow{End: "06:00"}, false},
		{"start 非 HH:MM", OTAWindow{Start: "9:00", End: "17:00"}, false},
		{"end 越界", OTAWindow{Start: "09:00", End: "24:00"}, false},
		{"分钟越界", OTAWindow{Start: "09:60", End: "17:00"}, false},
		{"非数字", OTAWindow{Start: "ab:cd", End: "17:00"}, false},
		{"weekday 7", OTAWindow{Start: "09:00", End: "17:00", Weekdays: []int{7}}, false},
		{"weekday -1", OTAWindow{Start: "09:00", End: "17:00", Weekdays: []int{-1}}, false},
		{"坏 tz", OTAWindow{Start: "09:00", End: "17:00", TZ: "Mars/Olympus"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := ValidateWindow(c.w); (err == nil) != c.ok {
				t.Errorf("ValidateWindow(%+v)=%v want ok=%v", c.w, err, c.ok)
			}
		})
	}
}

func TestParentEligibleTable(t *testing.T) {
	cases := []struct {
		stage, status string
		want          bool
	}{
		{"10", "running", true},
		{"50", "running", true},
		{"100", "running", true},
		{"10", "paused", true}, // 暂停的父批次仍可建子批次：暂停是平台节奏，不是质量否决
		{"10", "fused", false},
		{"100", "fused", false},
		{"1", "running", false},
		{"0.1", "running", false},
		{"", "running", false},
		{"abc", "running", false},
	}
	for _, c := range cases {
		t.Run(c.stage+"/"+c.status, func(t *testing.T) {
			if got := ParentEligible(c.stage, c.status); got != c.want {
				t.Errorf("ParentEligible(%q,%q)=%v want %v", c.stage, c.status, got, c.want)
			}
		})
	}
}

func TestMergeTargets(t *testing.T) {
	org := []AlarmTarget{{UserID: 5, Role: "teacher"}, {UserID: 2, Role: "org_admin"}}
	got := MergeTargets(org, []int64{9, 2, 0, 9})
	want := []AlarmTarget{
		{UserID: 2, Role: "org_admin", Source: SourceOrg},
		{UserID: 5, Role: "teacher", Source: SourceOrg},
		{UserID: 9, Source: SourcePersonal},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("MergeTargets=%v want %v", got, want)
	}
	if n := len(MergeTargets(nil, nil)); n != 0 {
		t.Fatalf("empty merge = %d", n)
	}
	if got := MergeTargets(nil, []int64{7}); len(got) != 1 || got[0].Source != SourcePersonal {
		t.Fatalf("personal only = %v", got)
	}
}

// ---- 批量 OTA ----

// 越权：请求里混入不属于本组织的 SN → 403，并把违规 SN 列出来；不得调用 ota-svc。
func TestOrgOTARejectsForeignSNs(t *testing.T) {
	_, ota, svc, h := seedOTA(t)
	body := `{"firmware_id":42,"sns":["SNA1","SNB1","SNZZ"]}`
	rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", body, asUser(1))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, sn := range []string{"SNB1", "SNZZ"} {
		if !strings.Contains(rec.Body.String(), sn) {
			t.Fatalf("response must list %s: %s", sn, rec.Body.String())
		}
	}
	if len(ota.reqs) != 0 {
		t.Fatalf("ota-svc must not be called: %+v", ota.reqs)
	}
	if svc.M.Get(MOTABatchesCreated) != 0 {
		t.Fatal("batch counted despite rejection")
	}
}

// 正常路径：窗口原样透传给 ota-svc（explicit_sns + parent_batch_id + policy.window），并写 org_ota_batch。
func TestOrgOTAWindowPassthrough(t *testing.T) {
	st, ota, svc, h := seedOTA(t)
	body := `{"firmware_id":42,"sns":["SNA2","SNA1"],"window":{"start":"22:00","end":"06:00","tz":"Asia/Shanghai","weekdays":[1,2]}}`
	rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", body, asUser(2)) // teacher 有 manage_queue
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	req, ok := ota.last()
	if !ok {
		t.Fatal("ota-svc not called")
	}
	if fmt.Sprint(req.ExplicitSNs) != "[SNA1 SNA2]" {
		t.Fatalf("explicit_sns=%v (want sorted org SNs)", req.ExplicitSNs)
	}
	if req.ParentBatchID == nil || *req.ParentBatchID != 100 {
		t.Fatalf("parent_batch_id=%v want 100", req.ParentBatchID)
	}
	raw, _ := json.Marshal(req.Policy)
	var got struct {
		IdleOnly bool      `json:"idle_only"`
		Window   OTAWindow `json:"window"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !got.IdleOnly || got.Window.Start != "22:00" || got.Window.End != "06:00" ||
		got.Window.TZ != "Asia/Shanghai" || fmt.Sprint(got.Window.Weekdays) != "[1 2]" {
		t.Fatalf("policy=%s", raw)
	}
	// 本地台账
	rows, _ := st.ListOrgOTABatches(context.Background(), 1)
	if len(rows) != 1 || rows[0].BatchID != 777 || rows[0].Window == nil || rows[0].Window.Start != "22:00" {
		t.Fatalf("org_ota_batch=%+v", rows)
	}
	if svc.M.Get(MOTABatchesCreated) != 1 {
		t.Fatalf("ota_batches_created=%d", svc.M.Get(MOTABatchesCreated))
	}
	// 列表接口
	rec = doReq(h, "GET", "/api/v1/orgs/1/ota/batches", "", asUser(3)) // student 只读可见
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "777") {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
}

// site_ids 展开为该站点的 SN。
func TestOrgOTASiteExpansion(t *testing.T) {
	_, ota, _, h := seedOTA(t)
	rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", `{"firmware_id":42,"site_ids":[3]}`, asUser(1))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	req, _ := ota.last()
	if fmt.Sprint(req.ExplicitSNs) != "[SNA1 SNA2]" {
		t.Fatalf("explicit_sns=%v", req.ExplicitSNs)
	}
	// 别的组织的站点 → 404（不泄露存在性）
	if rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", `{"firmware_id":42,"site_ids":[4]}`, asUser(1)); rec.Code != http.StatusNotFound {
		t.Fatalf("foreign site: %d %s", rec.Code, rec.Body.String())
	}
}

// 坏窗口 400 + ota_window_rejected；父批次不够格 409；无平台批次 409；学生 403。
func TestOrgOTAGuards(t *testing.T) {
	st, ota, svc, h := seedOTA(t)

	rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", `{"firmware_id":42,"sns":["SNA1"],"window":{"start":"22:00"}}`, asUser(1))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad window: %d %s", rec.Code, rec.Body.String())
	}
	if svc.M.Get(MOTAWindowRejected) != 1 {
		t.Fatalf("ota_window_rejected=%d", svc.M.Get(MOTAWindowRejected))
	}

	if rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", `{"firmware_id":99,"sns":["SNA1"]}`, asUser(1)); rec.Code != http.StatusConflict {
		t.Fatalf("unknown firmware: %d %s", rec.Code, rec.Body.String())
	}

	st.platform[42] = &PlatformBatch{ID: 100, Stage: "1", Status: "running"} // 只铺到 1%
	if rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", `{"firmware_id":42,"sns":["SNA1"]}`, asUser(1)); rec.Code != http.StatusConflict {
		t.Fatalf("stage 1%%: %d %s", rec.Code, rec.Body.String())
	}
	st.platform[42] = &PlatformBatch{ID: 100, Stage: "50", Status: "fused"} // 已熔断
	if rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", `{"firmware_id":42,"sns":["SNA1"]}`, asUser(1)); rec.Code != http.StatusConflict {
		t.Fatalf("fused parent: %d %s", rec.Code, rec.Body.String())
	}

	st.platform[42] = &PlatformBatch{ID: 100, Stage: "10", Status: "running"}
	if rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", `{"firmware_id":42,"sns":["SNA1"]}`, asUser(3)); rec.Code != http.StatusForbidden {
		t.Fatalf("student must be 403: %d %s", rec.Code, rec.Body.String())
	}
	// 既没 sns 也没 site_ids → 400
	if rec := doReq(h, "POST", "/api/v1/orgs/1/ota/batches", `{"firmware_id":42}`, asUser(1)); rec.Code != http.StatusBadRequest {
		t.Fatalf("no targets: %d %s", rec.Code, rec.Body.String())
	}
	if len(ota.reqs) != 0 {
		t.Fatalf("ota-svc called on rejected requests: %+v", ota.reqs)
	}
}

// ---- 告警目标 ----

// 三种归属：组织设备（站点订阅 + 个人绑定合并）、纯个人设备、未知设备。
func TestAlarmTargetsEndpoint(t *testing.T) {
	st, _, svc, h := seedOTA(t)
	st.subs[3] = []AlarmTarget{{UserID: 2, Role: "teacher"}, {UserID: 1, Role: "org_admin"}}
	st.personal["SNA1"] = []int64{2, 77} // 2 同时是订阅者与绑定者：只算一次，保留组织身份
	st.personal["SNPERSONAL"] = []int64{55}

	type target struct {
		UserID int64  `json:"user_id"`
		Role   string `json:"role"`
		Source string `json:"source"`
	}
	decode := func(t *testing.T, body string) (int64, []target) {
		t.Helper()
		var resp struct {
			Data struct {
				SN      string   `json:"sn"`
				OrgID   int64    `json:"org_id"`
				Targets []target `json:"targets"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatalf("bad json: %s", body)
		}
		return resp.Data.OrgID, resp.Data.Targets
	}

	// 组织设备：不带任何身份头也能查（内部接口，不过租户中间件）
	rec := doReq(h, "GET", "/internal/alarm-targets?sn=SNA1", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("org device: %d %s", rec.Code, rec.Body.String())
	}
	orgID, targets := decode(t, rec.Body.String())
	if orgID != 1 || len(targets) != 3 {
		t.Fatalf("org=%d targets=%+v", orgID, targets)
	}
	if targets[0].UserID != 1 || targets[0].Source != SourceOrg ||
		targets[1].UserID != 2 || targets[1].Role != "teacher" || targets[1].Source != SourceOrg ||
		targets[2].UserID != 77 || targets[2].Source != SourcePersonal {
		t.Fatalf("targets=%+v", targets)
	}

	// 纯个人设备：org_id=0，只有绑定用户
	rec = doReq(h, "GET", "/internal/alarm-targets?sn=SNPERSONAL", "", nil)
	orgID, targets = decode(t, rec.Body.String())
	if rec.Code != http.StatusOK || orgID != 0 || len(targets) != 1 || targets[0].UserID != 55 || targets[0].Source != SourcePersonal {
		t.Fatalf("personal device: %d org=%d targets=%+v", rec.Code, orgID, targets)
	}

	// 未知设备：200 + 空目标（alarm-svc 据此回退，不该收到 404 当故障处理）
	rec = doReq(h, "GET", "/internal/alarm-targets?sn=SNUNKNOWN", "", nil)
	orgID, targets = decode(t, rec.Body.String())
	if rec.Code != http.StatusOK || orgID != 0 || len(targets) != 0 {
		t.Fatalf("unknown device: %d org=%d targets=%+v", rec.Code, orgID, targets)
	}

	if rec := doReq(h, "GET", "/internal/alarm-targets", "", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing sn: %d", rec.Code)
	}
	if got := svc.M.Get(MAlarmTargetsServed); got != 3 {
		t.Fatalf("alarm_targets_served=%d want 3", got)
	}
}

// ---- 集成（需 PG）：新加的几条 SQL 只有真库能验——站点订阅 JOIN 在册成员、个人绑定、
// org_ota_batch 台账、最新平台批次查询、SN 归属集合。ota-svc 用 fake，不依赖它起进程。
func TestFleetOTATargetsIT(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := config.MustPG(ctx)
	defer pool.Close()
	st := NewPGStore(pool)

	stamp := time.Now().UnixNano() % 1_000_000
	admin, teacher, personalUser := stamp*10+1, stamp*10+2, stamp*10+3
	snOrg := fmt.Sprintf("ITOTAORG%d", stamp)
	snPersonal := fmt.Sprintf("ITOTAPER%d", stamp)

	org, err := st.CreateOrg(ctx, fmt.Sprintf("IT OTA Org %d", stamp), "school", "US", admin)
	if err != nil {
		t.Fatalf("create org: %v", err)
	}
	var fwID, parentID int64
	defer func() {
		bg := context.Background()
		for _, q := range []struct {
			sql  string
			args []any
		}{
			{`DELETE FROM iot_shard.org_ota_batch WHERE org_id=$1`, []any{org.OrgID}},
			{`DELETE FROM iot_shard.org_alarm_subscription WHERE org_id=$1`, []any{org.OrgID}},
			{`DELETE FROM iot_shard.device_binding WHERE sn = ANY($1)`, []any{[]string{snOrg, snPersonal}}},
			{`DELETE FROM iot_shard.device_org WHERE org_id=$1`, []any{org.OrgID}},
			{`DELETE FROM iot_shard.org_member WHERE org_id=$1`, []any{org.OrgID}},
			{`DELETE FROM iot_global.site WHERE org_id=$1`, []any{org.OrgID}},
			{`DELETE FROM iot_global.org WHERE org_id=$1`, []any{org.OrgID}},
			{`DELETE FROM iot_global.ota_batch WHERE firmware_id=$1`, []any{fwID}},
			{`DELETE FROM iot_global.firmware WHERE id=$1`, []any{fwID}},
		} {
			if _, err := pool.Exec(bg, q.sql, q.args...); err != nil {
				t.Logf("cleanup %q: %v", q.sql, err)
			}
		}
	}()
	if err := st.AddMember(ctx, org.OrgID, teacher, RoleTeacher, admin); err != nil {
		t.Fatal(err)
	}
	site, err := st.CreateSite(ctx, org.OrgID, "IT OTA Room", "UTC")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AttachDevice(ctx, DeviceOrg{SN: snOrg, OrgID: org.OrgID, SiteID: site.SiteID, AssignedBy: admin}); err != nil {
		t.Fatal(err)
	}
	// 站点订阅：teacher 订阅 + 一个已退出成员（必须被 JOIN 过滤掉）
	for _, uid := range []int64{teacher, 999_000_000 + stamp} {
		if _, err := pool.Exec(ctx, `INSERT INTO iot_shard.org_alarm_subscription(org_id,site_id,user_id) VALUES($1,$2,$3)`,
			org.OrgID, site.SiteID, uid); err != nil {
			t.Fatal(err)
		}
	}
	// 个人绑定：组织设备上也有一个绑定用户，另一台纯个人设备
	for _, b := range []struct {
		sn  string
		uid int64
	}{{snOrg, personalUser}, {snPersonal, personalUser}} {
		if _, err := pool.Exec(ctx, `INSERT INTO iot_shard.device_binding(sn,user_id) VALUES($1,$2)`, b.sn, b.uid); err != nil {
			t.Fatal(err)
		}
	}

	ota := &fakeOTA{}
	svc := NewService(st, newFakeShadows(), NewMetrics(), DefaultOptions())
	svc.OTA = ota
	h := Routes(svc, "it")

	// 告警目标：组织设备 = 在册订阅者 + 个人绑定；纯个人设备只有绑定用户
	got, err := svc.AlarmTargetsOf(ctx, snOrg)
	if err != nil {
		t.Fatalf("alarm targets: %v", err)
	}
	if got.OrgID != org.OrgID || len(got.Targets) != 2 {
		t.Fatalf("org device targets=%+v", got)
	}
	if got.Targets[0].UserID != teacher || got.Targets[0].Source != SourceOrg || got.Targets[0].Role != string(RoleTeacher) {
		t.Fatalf("first target=%+v (退出的成员必须被过滤)", got.Targets[0])
	}
	if got.Targets[1].UserID != personalUser || got.Targets[1].Source != SourcePersonal {
		t.Fatalf("second target=%+v", got.Targets[1])
	}
	if got, err := svc.AlarmTargetsOf(ctx, snPersonal); err != nil || got.OrgID != 0 || len(got.Targets) != 1 {
		t.Fatalf("personal device targets=%+v err=%v", got, err)
	}

	// 批量 OTA：父批次未到 10% 档 → 409；到档后建子批次并写台账
	if err := pool.QueryRow(ctx, `INSERT INTO iot_global.firmware(product_key,version,full_url,full_size,sha256,signature,status)
	 VALUES('LM_S1',$1,'https://cdn/x.bin',1024,$2,'sig','released') RETURNING id`,
		fmt.Sprintf("9.8.%d", stamp), strings.Repeat("ab", 32)).Scan(&fwID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO iot_global.ota_batch(firmware_id,stage,status,created_by) VALUES($1,'1','running','it') RETURNING id`,
		fwID).Scan(&parentID); err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("/api/v1/orgs/%d/ota/batches", org.OrgID)
	if rec := doReq(h, "POST", path, fmt.Sprintf(`{"firmware_id":%d,"sns":[%q]}`, fwID, snOrg), asUser(admin)); rec.Code != http.StatusConflict {
		t.Fatalf("stage 1%% parent must be 409: %d %s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE iot_global.ota_batch SET stage='10' WHERE id=$1`, parentID); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"firmware_id":%d,"site_ids":[%d],"window":{"start":"22:00","end":"06:00","tz":"UTC","weekdays":[1]}}`, fwID, site.SiteID)
	rec := doReq(h, "POST", path, body, asUser(admin))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org batch: %d %s", rec.Code, rec.Body.String())
	}
	req, _ := ota.last()
	if req.ParentBatchID == nil || *req.ParentBatchID != parentID || fmt.Sprint(req.ExplicitSNs) != fmt.Sprintf("[%s]", snOrg) {
		t.Fatalf("ota-svc req=%+v want parent=%d sns=[%s]", req, parentID, snOrg)
	}
	rows, err := st.ListOrgOTABatches(ctx, org.OrgID)
	if err != nil || len(rows) != 1 || rows[0].BatchID != 777 || rows[0].Window == nil || rows[0].Window.TZ != "UTC" {
		t.Fatalf("org_ota_batch ledger=%+v err=%v", rows, err)
	}
	// 越权 SN（不属于本组织）→ 403
	if rec := doReq(h, "POST", path, fmt.Sprintf(`{"firmware_id":%d,"sns":[%q]}`, fwID, snPersonal), asUser(admin)); rec.Code != http.StatusForbidden {
		t.Fatalf("foreign sn: %d %s", rec.Code, rec.Body.String())
	}
}
