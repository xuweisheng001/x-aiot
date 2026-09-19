package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/schedule"
)

func init() { slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) }

var fixedNow = time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC) // 周二 10:00 UTC

// ---- fake Store ----

type fakeStore struct {
	mu        sync.Mutex
	orgs      map[int64]*Org
	sites     map[int64]*Site
	roles     map[string]Role
	personal  map[string]bool
	devs      map[string]*DeviceOrg
	policies  map[int64]*PolicyRow
	desired   map[string]map[string]any
	audits    []LockAudit
	queues    map[int64]*Queue
	items     map[string]*Item
	alarmSNs  map[string]bool
	otaSNs    map[string]bool
	health    []HealthRow
	claimFail bool // 模拟被别的副本抢先
	seq       int64
}

func newFakeStore() *fakeStore {
	return &fakeStore{orgs: map[int64]*Org{}, sites: map[int64]*Site{}, roles: map[string]Role{},
		personal: map[string]bool{}, devs: map[string]*DeviceOrg{}, policies: map[int64]*PolicyRow{},
		desired: map[string]map[string]any{}, queues: map[int64]*Queue{}, items: map[string]*Item{},
		alarmSNs: map[string]bool{}, otaSNs: map[string]bool{}}
}

func rkey(org, user int64) string { return fmt.Sprintf("%d:%d", org, user) }

func (f *fakeStore) next() int64 { f.seq++; return f.seq }

func (f *fakeStore) CreateOrg(_ context.Context, name, typ, region string, creator int64) (*Org, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o := &Org{OrgID: f.next(), Name: name, Type: typ, Region: region, UnlockMaxMinutes: 240, CreatedAt: fixedNow}
	f.orgs[o.OrgID] = o
	if creator > 0 {
		f.roles[rkey(o.OrgID, creator)] = RoleAdmin
	}
	return o, nil
}

func (f *fakeStore) GetOrg(_ context.Context, orgID int64) (*Org, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if o, ok := f.orgs[orgID]; ok {
		return o, nil
	}
	return nil, ErrNotFound
}

func (f *fakeStore) GetRole(_ context.Context, orgID, userID int64) (Role, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.roles[rkey(orgID, userID)]; ok {
		return r, nil
	}
	return "", ErrNotMember
}

func (f *fakeStore) AddMember(_ context.Context, orgID, userID int64, role Role, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.roles[rkey(orgID, userID)] = role
	return nil
}

func (f *fakeStore) RemoveMember(_ context.Context, orgID, userID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.roles[rkey(orgID, userID)]; !ok {
		return ErrNotFound
	}
	delete(f.roles, rkey(orgID, userID))
	return nil
}

func (f *fakeStore) ListMembers(_ context.Context, orgID int64) ([]Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []Member{}
	for k, r := range f.roles {
		var o, u int64
		fmt.Sscanf(k, "%d:%d", &o, &u)
		if o == orgID {
			out = append(out, Member{OrgID: o, UserID: u, Role: r})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out, nil
}

func (f *fakeStore) CreateSite(_ context.Context, orgID int64, name, tz string) (*Site, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := &Site{SiteID: f.next(), OrgID: orgID, Name: name, TZ: tz, CreatedAt: fixedNow}
	f.sites[s.SiteID] = s
	return s, nil
}

func (f *fakeStore) GetSite(_ context.Context, orgID, siteID int64) (*Site, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.sites[siteID]; ok && s.OrgID == orgID {
		return s, nil
	}
	return nil, ErrNotFound
}

func (f *fakeStore) ListSites(_ context.Context, orgID int64) ([]Site, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []Site{}
	for _, s := range f.sites {
		if s.OrgID == orgID {
			out = append(out, *s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SiteID < out[j].SiteID })
	return out, nil
}

func (f *fakeStore) PersonalBound(_ context.Context, sn string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.personal[sn], nil
}

func (f *fakeStore) DeviceOrgOf(_ context.Context, sn string) (*DeviceOrg, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.devs[sn]; ok {
		return d, nil
	}
	return nil, ErrNotFound
}

func (f *fakeStore) AttachDevice(_ context.Context, d DeviceOrg) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.devs[d.SN]; ok && cur.OrgID != d.OrgID {
		return ErrConflict
	}
	cp := d
	cp.AssignedAt = fixedNow
	f.devs[d.SN] = &cp
	return nil
}

func (f *fakeStore) DetachDevice(_ context.Context, orgID int64, sn string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.devs[sn]; ok && d.OrgID == orgID {
		delete(f.devs, sn)
		return nil
	}
	return ErrNotFound
}

func (f *fakeStore) GetDevice(_ context.Context, orgID int64, sn string) (*DeviceOrg, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.devs[sn]; ok && d.OrgID == orgID {
		return d, nil
	}
	return nil, ErrNotFound
}

func (f *fakeStore) ListDevices(_ context.Context, orgID int64, offset, limit int) ([]DeviceOrg, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	all := []DeviceOrg{}
	for _, d := range f.devs {
		if d.OrgID == orgID {
			all = append(all, *d)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].SN < all[j].SN })
	total := len(all)
	if offset > total {
		offset = total
	}
	end := min(offset+limit, total)
	return all[offset:end], total, nil
}

func (f *fakeStore) SiteSNs(_ context.Context, orgID, siteID int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []string{}
	for _, d := range f.devs {
		if d.OrgID == orgID && d.SiteID == siteID {
			out = append(out, d.SN)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (f *fakeStore) UpsertPolicy(_ context.Context, orgID, siteID int64, p schedule.Policy, _ int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pr, ok := f.policies[siteID]
	if !ok {
		pr = &PolicyRow{OrgID: orgID, SiteID: siteID}
		f.policies[siteID] = pr
	}
	if pr.OrgID != orgID {
		return 0, ErrNotFound
	}
	pr.Version++
	pr.Policy = p
	return pr.Version, nil
}

func (f *fakeStore) GetPolicy(_ context.Context, orgID, siteID int64) (*PolicyRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if pr, ok := f.policies[siteID]; ok && pr.OrgID == orgID {
		return pr, nil
	}
	return nil, ErrNotFound
}

func (f *fakeStore) AllPolicies(_ context.Context) ([]PolicyRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []PolicyRow{}
	for _, pr := range f.policies {
		out = append(out, *pr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SiteID < out[j].SiteID })
	return out, nil
}

func (f *fakeStore) GetDesiredLock(_ context.Context, sn string) (DesiredLock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, _ := json.Marshal(f.desired[sn])
	return ParseDesiredLock(raw), nil
}

func (f *fakeStore) MergeDesired(_ context.Context, sn string, patch json.RawMessage) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var p map[string]any
	if err := json.Unmarshal(patch, &p); err != nil {
		return 0, err
	}
	cur := f.desired[sn]
	if cur == nil {
		cur = map[string]any{}
		f.desired[sn] = cur
	}
	for k, v := range p {
		cur[k] = v
	}
	return int64(len(cur)), nil
}

func (f *fakeStore) InsertLockAudit(_ context.Context, a LockAudit) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audits = append(f.audits, a)
	return nil
}

func (f *fakeStore) ListLockAudit(_ context.Context, orgID int64, sn string, _ time.Time, _ int) ([]map[string]any, error) {
	return []map[string]any{}, nil
}

func (f *fakeStore) CreateQueue(_ context.Context, orgID, siteID int64, name string, sns []string, _ int64) (*Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if sns == nil {
		sns = []string{}
	}
	q := &Queue{QueueID: f.next(), OrgID: orgID, SiteID: siteID, Name: name, DeviceSNs: sns}
	f.queues[q.QueueID] = q
	return q, nil
}

func (f *fakeStore) GetQueue(_ context.Context, orgID, queueID int64) (*Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if q, ok := f.queues[queueID]; ok && q.OrgID == orgID {
		return q, nil
	}
	return nil, ErrNotFound
}

func (f *fakeStore) ListQueues(_ context.Context, orgID int64) ([]Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []Queue{}
	for _, q := range f.queues {
		if q.OrgID == orgID {
			out = append(out, *q)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].QueueID < out[j].QueueID })
	return out, nil
}

func (f *fakeStore) AllQueues(_ context.Context) ([]Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []Queue{}
	for _, q := range f.queues {
		out = append(out, *q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].QueueID < out[j].QueueID })
	return out, nil
}

func (f *fakeStore) InsertItem(_ context.Context, it Item) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := it
	cp.JobID = it.ItemID
	f.items[it.ItemID] = &cp
	return nil
}

func (f *fakeStore) GetItem(_ context.Context, orgID int64, itemID string) (*Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if it, ok := f.items[itemID]; ok && it.OrgID == orgID {
		cp := *it
		return &cp, nil
	}
	return nil, ErrNotFound
}

func (f *fakeStore) ListItems(_ context.Context, orgID, queueID int64, status string) ([]Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []Item{}
	for _, it := range f.items {
		if it.OrgID == orgID && it.QueueID == queueID && (status == "" || it.Status == status) {
			out = append(out, *it)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SubmittedAt.Before(out[j].SubmittedAt) })
	return out, nil
}

func (f *fakeStore) SetStatus(_ context.Context, orgID int64, itemID, from, to, by, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.items[itemID]
	if !ok || it.OrgID != orgID || it.Status != from {
		return ErrConflict
	}
	it.Status = to
	if to == StApproved {
		it.ApprovedBy = by
		at := fixedNow
		it.ApprovedAt = &at
	}
	if to == StRejected {
		it.RejectedReason = reason
	}
	return nil
}

func (f *fakeStore) HeadApproved(_ context.Context, queueID int64) (*Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var head *Item
	for _, it := range f.items {
		if it.QueueID != queueID || it.Status != StApproved {
			continue
		}
		if head == nil || it.SubmittedAt.Before(head.SubmittedAt) {
			head = it
		}
	}
	if head == nil {
		return nil, ErrNotFound
	}
	cp := *head
	return &cp, nil
}

func (f *fakeStore) ActiveSNs(_ context.Context, _ int64) (map[string]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]bool{}
	for _, it := range f.items {
		if it.AssignedSN != "" && (it.Status == StDispatched || it.Status == StRunning) {
			out[it.AssignedSN] = true
		}
	}
	return out, nil
}

func (f *fakeStore) ClaimDispatch(_ context.Context, itemID, sn, token string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimFail {
		return false, nil
	}
	it, ok := f.items[itemID]
	if !ok || it.Status != StApproved {
		return false, nil
	}
	it.Status = StDispatched
	it.AssignedSN = sn
	it.DispatchedCmdID = token
	at := fixedNow
	it.DispatchedAt = &at
	return true, nil
}

func (f *fakeStore) ConfirmDispatch(_ context.Context, itemID, token, cmdID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if it, ok := f.items[itemID]; ok && it.DispatchedCmdID == token && it.Status == StDispatched {
		it.DispatchedCmdID = cmdID
	}
	return nil
}

func (f *fakeStore) RevertDispatch(_ context.Context, itemID, token string, maxRetry int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.items[itemID]
	if !ok || it.DispatchedCmdID != token || it.Status != StDispatched {
		return nil
	}
	it.Retry++
	it.AssignedSN, it.DispatchedCmdID, it.DispatchedAt = "", "", nil
	it.Status = StApproved
	if it.Retry >= maxRetry {
		it.Status = StNeedsTeacher
	}
	return nil
}

func (f *fakeStore) ExpireItems(_ context.Context, approvedTTL, dispatchedTTL time.Duration, now time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, it := range f.items {
		switch {
		case it.Status == StApproved && it.SubmittedAt.Before(now.Add(-approvedTTL)),
			it.Status == StDispatched && it.DispatchedAt != nil && it.DispatchedAt.Before(now.Add(-dispatchedTTL)):
			it.Status = StSkipped
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) OpenAlarmSNs(_ context.Context, sns []string) (map[string]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]bool{}
	for _, sn := range sns {
		if f.alarmSNs[sn] {
			out[sn] = true
		}
	}
	return out, nil
}

func (f *fakeStore) PendingOTASNs(_ context.Context, sns []string) (map[string]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]bool{}
	for _, sn := range sns {
		if f.otaSNs[sn] {
			out[sn] = true
		}
	}
	return out, nil
}

func (f *fakeStore) ConsumableHealth(_ context.Context, _ int64) ([]HealthRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.health, nil
}

// ---- fake 影子与下发器 ----

type fakeShadows struct {
	mu   sync.Mutex
	m    map[string]map[string]string
	errs map[string]error
}

func newFakeShadows() *fakeShadows {
	return &fakeShadows{m: map[string]map[string]string{}, errs: map[string]error{}}
}

func (f *fakeShadows) set(sn string, workState int, locked bool, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	lock := "0"
	if locked {
		lock = "1"
	}
	f.m[sn] = map[string]string{"work_state": fmt.Sprint(workState), "lock_state": lock,
		"updated_at": fmt.Sprint(at.UnixMilli())}
}

func (f *fakeShadows) Read(_ context.Context, sn string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs[sn]; err != nil {
		return nil, err
	}
	if m, ok := f.m[sn]; ok {
		return m, nil
	}
	return map[string]string{}, nil
}

type fakeDispatcher struct {
	mu    sync.Mutex
	calls []string // sn
	err   error
	n     int
}

func (d *fakeDispatcher) JobStart(_ context.Context, sn string, _ map[string]any) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return "", d.err
	}
	d.n++
	d.calls = append(d.calls, sn)
	return fmt.Sprintf("cmd-%d", d.n), nil
}

func (d *fakeDispatcher) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

// ---- 测试脚手架 ----

func newTestSvc(st *fakeStore, sh Shadows) *Service {
	if sh == nil {
		sh = newFakeShadows()
	}
	svc := NewService(st, sh, NewMetrics(), DefaultOptions())
	svc.Now = func() time.Time { return fixedNow }
	return svc
}

func doReq(h http.Handler, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "10.0.0.1:1234"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func asUser(id int64) map[string]string { return map[string]string{HeaderUserID: fmt.Sprint(id)} }

func dataOf(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var resp struct {
		Code int            `json:"code"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad json: %s", rec.Body.String())
	}
	return resp.Data
}

// seed 建两个组织：org A（1: admin, 2: teacher, 3: student）与 org B（9: admin）。
func seed(t *testing.T) (*fakeStore, *Service, http.Handler) {
	t.Helper()
	st := newFakeStore()
	ctx := context.Background()
	a, _ := st.CreateOrg(ctx, "A School", "school", "US", 1)
	b, _ := st.CreateOrg(ctx, "B Studio", "studio", "US", 9)
	if a.OrgID != 1 || b.OrgID != 2 {
		t.Fatalf("unexpected org ids %d %d", a.OrgID, b.OrgID)
	}
	_ = st.AddMember(ctx, a.OrgID, 2, RoleTeacher, 1)
	_ = st.AddMember(ctx, a.OrgID, 3, RoleStudent, 1)
	svc := newTestSvc(st, nil)
	return st, svc, Routes(svc, "test")
}

// ---- 租户隔离 ----

// A 组织成员访问 B 组织的所有路由：一律 404（不泄露存在性），并计数 authz_notfound。
func TestTenantIsolation404(t *testing.T) {
	st, svc, h := seed(t)
	ctx := context.Background()
	siteB, _ := st.CreateSite(ctx, 2, "B Room", "UTC")
	qB, _ := st.CreateQueue(ctx, 2, siteB.SiteID, "qb", []string{"SNB1"}, 9)
	_ = st.AttachDevice(ctx, DeviceOrg{SN: "SNB1", OrgID: 2, SiteID: siteB.SiteID})

	routes := []struct{ method, path, body string }{
		{"GET", "/api/v1/orgs/2", ""},
		{"GET", "/api/v1/orgs/2/members", ""},
		{"POST", "/api/v1/orgs/2/members", `{"user_id":5,"role":"teacher"}`},
		{"DELETE", "/api/v1/orgs/2/members/9", ""},
		{"GET", "/api/v1/orgs/2/sites", ""},
		{"POST", "/api/v1/orgs/2/sites", `{"name":"x","tz":"UTC"}`},
		{"GET", "/api/v1/orgs/2/devices", ""},
		{"POST", "/api/v1/orgs/2/devices", `{"sn":"SNB1","site_id":3}`},
		{"DELETE", "/api/v1/orgs/2/devices/SNB1", ""},
		{"GET", "/api/v1/orgs/2/fleet/summary", ""},
		{"PUT", "/api/v1/orgs/2/sites/3/policy", `{"tz":"UTC"}`},
		{"GET", "/api/v1/orgs/2/sites/3/policy", ""},
		{"POST", "/api/v1/orgs/2/devices/SNB1/unlock", ""},
		{"GET", "/api/v1/orgs/2/queues", ""},
		{"POST", "/api/v1/orgs/2/queues", `{"site_id":3,"name":"q"}`},
		{"POST", fmt.Sprintf("/api/v1/orgs/2/queues/%d/items", qB.QueueID), `{"file_sha256":"` + strings.Repeat("a", 64) + `"}`},
		{"GET", fmt.Sprintf("/api/v1/orgs/2/queues/%d/items", qB.QueueID), ""},
		{"POST", "/api/v1/orgs/2/items/x/approve", ""},
		{"GET", "/api/v1/orgs/2/consumables", ""},
	}
	for _, r := range routes {
		rec := doReq(h, r.method, r.path, r.body, asUser(1)) // user 1 只是 org A 的 admin
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s: status=%d want 404 (body=%s)", r.method, r.path, rec.Code, rec.Body.String())
		}
	}
	if got := svc.M.Get(MAuthzNotFound); got != int64(len(routes)) {
		t.Errorf("authz_notfound=%d want %d", got, len(routes))
	}
	// 组织不存在同样 404；缺 X-User-Id → 401；X-Org-Id 与路径不一致 → 404
	if rec := doReq(h, "GET", "/api/v1/orgs/777", "", asUser(1)); rec.Code != http.StatusNotFound {
		t.Errorf("unknown org: %d", rec.Code)
	}
	if rec := doReq(h, "GET", "/api/v1/orgs/1", "", nil); rec.Code != http.StatusUnauthorized {
		t.Errorf("missing user id: %d", rec.Code)
	}
	hdr := asUser(1)
	hdr[HeaderOrgID] = "2"
	if rec := doReq(h, "GET", "/api/v1/orgs/1", "", hdr); rec.Code != http.StatusNotFound {
		t.Errorf("org header mismatch: %d", rec.Code)
	}
	// 本组织成员正常访问
	if rec := doReq(h, "GET", "/api/v1/orgs/1", "", asUser(3)); rec.Code != http.StatusOK {
		t.Errorf("own org: %d %s", rec.Code, rec.Body.String())
	}
}

// 角色不足 → 403（组织存在性已确认），并计数 authz_denied。
func TestRoleMatrix403(t *testing.T) {
	_, svc, h := seed(t)
	cases := []struct {
		user           int64
		method, path   string
		body           string
		wantStatusCode int
	}{
		{3, "POST", "/api/v1/orgs/1/members", `{"user_id":8,"role":"student"}`, http.StatusForbidden}, // 学生不能管成员
		{3, "POST", "/api/v1/orgs/1/sites", `{"name":"r","tz":"UTC"}`, http.StatusForbidden},
		{3, "POST", "/api/v1/orgs/1/devices/SN1/unlock", "", http.StatusForbidden}, // 学生不能解锁
		{3, "GET", "/api/v1/orgs/1/consumables", "", http.StatusForbidden},         // 学生看不到耗材
		{2, "POST", "/api/v1/orgs/1/members", `{"user_id":8,"role":"student"}`, http.StatusForbidden},
		{2, "GET", "/api/v1/orgs/1/consumables", "", http.StatusOK}, // 教师可以
	}
	for _, c := range cases {
		rec := doReq(h, c.method, c.path, c.body, asUser(c.user))
		if rec.Code != c.wantStatusCode {
			t.Errorf("user %d %s %s: status=%d want %d", c.user, c.method, c.path, rec.Code, c.wantStatusCode)
		}
	}
	if svc.M.Get(MAuthzDenied) != 5 {
		t.Errorf("authz_denied=%d want 5", svc.M.Get(MAuthzDenied))
	}
}

// ---- 归属冲突 ----

func TestAttachOwnership(t *testing.T) {
	st, svc, h := seed(t)
	ctx := context.Background()
	siteA, _ := st.CreateSite(ctx, 1, "A Room", "UTC")
	body := func(sn string) string { return fmt.Sprintf(`{"sn":%q,"site_id":%d}`, sn, siteA.SiteID) }

	// 干净设备：放行，无留痕
	if rec := doReq(h, "POST", "/api/v1/orgs/1/devices", body("SNA1"), asUser(1)); rec.Code != http.StatusOK {
		t.Fatalf("clean attach: %d %s", rec.Code, rec.Body.String())
	}
	if len(st.audits) != 0 {
		t.Fatalf("clean attach must not audit: %+v", st.audits)
	}
	// 个人绑定：组织为准，放行 + 留痕
	st.personal["SNA2"] = true
	if rec := doReq(h, "POST", "/api/v1/orgs/1/devices", body("SNA2"), asUser(1)); rec.Code != http.StatusOK {
		t.Fatalf("personal attach: %d %s", rec.Code, rec.Body.String())
	}
	if len(st.audits) != 1 || st.audits[0].Action != "denied_personal" || st.audits[0].SN != "SNA2" {
		t.Fatalf("personal override audit: %+v", st.audits)
	}
	// 已属别的组织：拒绝 409 + 留痕
	_ = st.AttachDevice(ctx, DeviceOrg{SN: "SNB9", OrgID: 2, SiteID: 99})
	rec := doReq(h, "POST", "/api/v1/orgs/1/devices", body("SNB9"), asUser(1))
	if rec.Code != http.StatusConflict {
		t.Fatalf("cross-org attach: %d %s", rec.Code, rec.Body.String())
	}
	if len(st.audits) != 2 || st.audits[1].SN != "SNB9" {
		t.Fatalf("conflict audit: %+v", st.audits)
	}
	if d, _ := st.DeviceOrgOf(ctx, "SNB9"); d.OrgID != 2 {
		t.Fatalf("device must stay in org B: %+v", d)
	}
	// 站点属别的组织 → 404（不泄露存在性）
	siteB, _ := st.CreateSite(ctx, 2, "B Room", "UTC")
	bad := fmt.Sprintf(`{"sn":"SNA3","site_id":%d}`, siteB.SiteID)
	if rec := doReq(h, "POST", "/api/v1/orgs/1/devices", bad, asUser(1)); rec.Code != http.StatusNotFound {
		t.Fatalf("foreign site: %d", rec.Code)
	}
	_ = svc
}

// ---- 队列状态机 ----

func TestQueueStateMachine(t *testing.T) {
	st, _, h := seed(t)
	ctx := context.Background()
	site, _ := st.CreateSite(ctx, 1, "A Room", "UTC")
	q, _ := st.CreateQueue(ctx, 1, site.SiteID, "class-1", []string{"SNA1"}, 1)
	itemsPath := fmt.Sprintf("/api/v1/orgs/1/queues/%d/items", q.QueueID)
	sha := strings.Repeat("ab", 32)

	// 学生可提交
	rec := doReq(h, "POST", itemsPath, fmt.Sprintf(`{"file_sha256":%q,"est_minutes":5}`, sha), asUser(3))
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit: %d %s", rec.Code, rec.Body.String())
	}
	itemID, _ := dataOf(t, rec)["item_id"].(string)
	if len(itemID) != 36 {
		t.Fatalf("item_id=%q", itemID)
	}
	// 非法 sha256 → 400
	if rec := doReq(h, "POST", itemsPath, `{"file_sha256":"zz"}`, asUser(3)); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad sha: %d", rec.Code)
	}
	// 学生不能审批 → 403
	if rec := doReq(h, "POST", "/api/v1/orgs/1/items/"+itemID+"/approve", "", asUser(3)); rec.Code != http.StatusForbidden {
		t.Fatalf("student approve: %d", rec.Code)
	}
	// 教师审批 → approved
	rec = doReq(h, "POST", "/api/v1/orgs/1/items/"+itemID+"/approve", "", asUser(2))
	if rec.Code != http.StatusOK || dataOf(t, rec)["status"] != StApproved {
		t.Fatalf("approve: %d %s", rec.Code, rec.Body.String())
	}
	// 重复审批 → 非法迁移 403（approved → approved 不在状态表里）
	if rec := doReq(h, "POST", "/api/v1/orgs/1/items/"+itemID+"/approve", "", asUser(2)); rec.Code != http.StatusForbidden {
		t.Fatalf("double approve: %d %s", rec.Code, rec.Body.String())
	}
	// 列表按状态过滤
	rec = doReq(h, "GET", itemsPath+"?status=approved", "", asUser(2))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), itemID) {
		t.Fatalf("list approved: %s", rec.Body.String())
	}
	// 未知任务 → 404
	if rec := doReq(h, "POST", "/api/v1/orgs/1/items/nope/approve", "", asUser(2)); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown item: %d", rec.Code)
	}
	// 其它学生不能取消别人的任务
	_ = st.AddMember(ctx, 1, 4, RoleStudent, 1)
	if rec := doReq(h, "POST", "/api/v1/orgs/1/items/"+itemID+"/cancel", "", asUser(4)); rec.Code != http.StatusForbidden {
		t.Fatalf("other student cancel: %d", rec.Code)
	}
}

// ---- 调度器 ----

func schedSetup(t *testing.T) (*fakeStore, *Service, *fakeShadows, string) {
	t.Helper()
	st := newFakeStore()
	ctx := context.Background()
	_, _ = st.CreateOrg(ctx, "A", "school", "US", 1)
	site, _ := st.CreateSite(ctx, 1, "room", "UTC")
	q, _ := st.CreateQueue(ctx, 1, site.SiteID, "class", []string{"SN1", "SN2"}, 1)
	sh := newFakeShadows()
	sh.set("SN1", 0, false, fixedNow) // 空闲
	sh.set("SN2", 0, false, fixedNow)
	svc := newTestSvc(st, sh)
	it := Item{ItemID: NewID(), OrgID: 1, QueueID: q.QueueID, Submitter: 3, FileSHA256: strings.Repeat("a", 64),
		Status: StApproved, SubmittedAt: fixedNow}
	_ = st.InsertItem(ctx, it)
	return st, svc, sh, it.ItemID
}

func TestSchedulerDispatchOnce(t *testing.T) {
	st, svc, _, itemID := schedSetup(t)
	d := &fakeDispatcher{}
	sc := NewScheduler(svc, d)
	ctx := context.Background()

	n, err := sc.DispatchOnce(ctx)
	if err != nil || n != 1 || d.count() != 1 || d.calls[0] != "SN1" {
		t.Fatalf("first round n=%d err=%v calls=%v", n, err, d.calls)
	}
	it := st.items[itemID]
	if it.Status != StDispatched || it.AssignedSN != "SN1" || it.DispatchedCmdID != "cmd-1" {
		t.Fatalf("item after dispatch: %+v", it)
	}
	// 第二轮：队首已无 approved 项 → 不重复下发
	if n, err := sc.DispatchOnce(ctx); err != nil || n != 0 || d.count() != 1 {
		t.Fatalf("second round n=%d err=%v calls=%v", n, err, d.calls)
	}
	if svc.M.Get(MDispatchOK) != 1 || svc.M.Get(MDispatchClaimed) != 1 || svc.M.Get(MDispatchAttempts) != 1 {
		t.Fatalf("metrics ok=%d claimed=%d attempts=%d", svc.M.Get(MDispatchOK), svc.M.Get(MDispatchClaimed), svc.M.Get(MDispatchAttempts))
	}
}

// 抢占失败（别的副本先到）：绝不下发。
func TestSchedulerClaimLostNoDispatch(t *testing.T) {
	st, svc, _, itemID := schedSetup(t)
	st.claimFail = true
	d := &fakeDispatcher{}
	if n, err := NewScheduler(svc, d).DispatchOnce(context.Background()); err != nil || n != 0 || d.count() != 0 {
		t.Fatalf("n=%d err=%v calls=%v", n, err, d.calls)
	}
	if st.items[itemID].Status != StApproved {
		t.Fatalf("item must stay approved: %+v", st.items[itemID])
	}
	if svc.M.Get(MDispatchAttempts) != 1 || svc.M.Get(MDispatchClaimed) != 0 {
		t.Fatalf("attempts=%d claimed=%d", svc.M.Get(MDispatchAttempts), svc.M.Get(MDispatchClaimed))
	}
}

// 下发失败：回退 approved 并 retry+1；重试到上限 → needs_teacher。
func TestSchedulerDispatchFailureReverts(t *testing.T) {
	st, svc, _, itemID := schedSetup(t)
	d := &fakeDispatcher{err: errors.New("deviceapi down")}
	sc := NewScheduler(svc, d)
	ctx := context.Background()
	if n, err := sc.DispatchOnce(ctx); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	it := st.items[itemID]
	if it.Status != StApproved || it.Retry != 1 || it.AssignedSN != "" {
		t.Fatalf("after revert: %+v", it)
	}
	if svc.M.Get(MDispatchFailed) != 1 {
		t.Fatalf("dispatch_failed=%d", svc.M.Get(MDispatchFailed))
	}
	for i := 0; i < 2; i++ {
		if _, err := sc.DispatchOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if st.items[itemID].Status != StNeedsTeacher {
		t.Fatalf("retry exhausted: %+v", st.items[itemID])
	}
}

// 设备不空闲 / 锁定 / 已被占用：不抢占也不下发。
func TestSchedulerSkipsBusyDevices(t *testing.T) {
	st, svc, sh, itemID := schedSetup(t)
	sh.set("SN1", 2, false, fixedNow) // 作业中
	sh.set("SN2", 0, true, fixedNow)  // 锁定
	d := &fakeDispatcher{}
	if n, _ := NewScheduler(svc, d).DispatchOnce(context.Background()); n != 0 || d.count() != 0 {
		t.Fatalf("n=%d calls=%v", n, d.calls)
	}
	if st.items[itemID].Status != StApproved || svc.M.Get(MDispatchAttempts) != 0 {
		t.Fatalf("status=%s attempts=%d", st.items[itemID].Status, svc.M.Get(MDispatchAttempts))
	}
	// SN1 空闲但被别的进行中任务占用 → 跳过
	sh.set("SN1", 0, false, fixedNow)
	other := Item{ItemID: NewID(), OrgID: 1, QueueID: 3, Status: StRunning, AssignedSN: "SN1", SubmittedAt: fixedNow}
	_ = st.InsertItem(context.Background(), other)
	st.items[other.ItemID].AssignedSN = "SN1"
	if n, _ := NewScheduler(svc, d).DispatchOnce(context.Background()); n != 0 || d.count() != 0 {
		t.Fatalf("occupied: n=%d calls=%v", n, d.calls)
	}
}

func TestExpireOnce(t *testing.T) {
	st, svc, _, itemID := schedSetup(t)
	st.items[itemID].SubmittedAt = fixedNow.Add(-24 * time.Hour)
	n, err := NewScheduler(svc, &fakeDispatcher{}).ExpireOnce(context.Background())
	if err != nil || n != 1 || st.items[itemID].Status != StSkipped {
		t.Fatalf("n=%d err=%v status=%s", n, err, st.items[itemID].Status)
	}
	if svc.M.Get(MItemsExpired) != 1 {
		t.Fatalf("items_expired=%d", svc.M.Get(MItemsExpired))
	}
}

// ---- 看板聚合 ----

func TestSummaryAggregation(t *testing.T) {
	st, svc, h := seed(t)
	ctx := context.Background()
	site, _ := st.CreateSite(ctx, 1, "room", "UTC")
	sh := newFakeShadows()
	svc.Sh = sh
	for i, spec := range []struct {
		ws     int
		locked bool
		age    time.Duration
	}{{0, false, 0}, {2, false, 10 * time.Second}, {0, true, 0}, {3, false, 10 * time.Minute}} {
		sn := fmt.Sprintf("SN%d", i)
		_ = st.AttachDevice(ctx, DeviceOrg{SN: sn, OrgID: 1, SiteID: site.SiteID})
		sh.set(sn, spec.ws, spec.locked, fixedNow.Add(-spec.age))
	}
	_ = st.AttachDevice(ctx, DeviceOrg{SN: "SN4", OrgID: 1, SiteID: site.SiteID}) // 无影子
	st.alarmSNs["SN1"] = true
	st.otaSNs["SN2"] = true
	st.otaSNs["SN3"] = true

	resp, err := svc.Summary(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := Counts{Total: 5, Online: 3, Working: 1, Locked: 1, Alarm: 1, PendingOTA: 2}
	if resp.Counts != want {
		t.Fatalf("counts=%+v want %+v", resp.Counts, want)
	}
	// 分批读影子（≤ 200）路径与单批一致
	svc.Opt.ShadowBatch = 2
	resp2, err := svc.Summary(ctx, 1)
	if err != nil || resp2.Counts != want {
		t.Fatalf("batched counts=%+v err=%v", resp2.Counts, err)
	}
	rec := doReq(h, "GET", "/api/v1/orgs/1/fleet/summary", "", asUser(3))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"total":5`) {
		t.Fatalf("summary handler: %d %s", rec.Code, rec.Body.String())
	}
	// 分页
	rec = doReq(h, "GET", "/api/v1/orgs/1/devices?offset=1&limit=2", "", asUser(3))
	d := dataOf(t, rec)
	if d["total"] != float64(5) || len(d["devices"].([]any)) != 2 {
		t.Fatalf("paging: %s", rec.Body.String())
	}
}

// ---- 耗材 CSV ----

func TestConsumablesCSVHandler(t *testing.T) {
	st, _, h := seed(t)
	eol := fixedNow.Add(72 * time.Hour)
	st.health = []HealthRow{
		{SN: "SN1", Part: "laser", Health: 95},
		{SN: "SN2", Part: "laser", Health: 12, PredictedEOLAt: &eol},
		{SN: "SN3", Part: "laser", Health: 45},
		{SN: "SN4", Part: "filter", Health: 5},
	}
	rec := doReq(h, "GET", "/api/v1/orgs/1/consumables", "", asUser(1))
	if rec.Code != http.StatusOK {
		t.Fatalf("json: %d %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"part":"laser","total":3,"good":1,"warn":1,"critical":1`) {
		t.Fatalf("buckets: %s", body)
	}
	rec = doReq(h, "GET", "/api/v1/orgs/1/consumables?format=csv", "", asUser(1))
	if ct := rec.Header().Get("Content-Type"); rec.Code != http.StatusOK || !strings.HasPrefix(ct, "text/csv") {
		t.Fatalf("csv: %d %s", rec.Code, ct)
	}
	lines := strings.Split(strings.TrimSpace(rec.Body.String()), "\n")
	if len(lines) != 3 || lines[0] != "sn,part,health,predicted_eol_at" {
		t.Fatalf("csv body=%q", rec.Body.String())
	}
	if !strings.HasPrefix(lines[1], "SN4,filter,5.00") || !strings.HasPrefix(lines[2], "SN2,laser,12.00") {
		t.Fatalf("csv order: %v", lines)
	}
}

// ---- 课表对账与解锁 ----

func TestReconcileAndUnlock(t *testing.T) {
	st, svc, h := seed(t)
	ctx := context.Background()
	site, _ := st.CreateSite(ctx, 1, "room", "UTC")
	_ = st.AttachDevice(ctx, DeviceOrg{SN: "SN1", OrgID: 1, SiteID: site.SiteID})

	// 课表：周二 08:00-09:00 允许；此刻 10:00 → 应锁
	policy := fmt.Sprintf(`{"tz":"UTC","weekly":{"tue":[{"start":"08:00","end":"09:00"}]}}`)
	rec := doReq(h, "PUT", fmt.Sprintf("/api/v1/orgs/1/sites/%d/policy", site.SiteID), policy, asUser(2))
	if rec.Code != http.StatusOK {
		t.Fatalf("put policy: %d %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, "PUT", fmt.Sprintf("/api/v1/orgs/1/sites/%d/policy", site.SiteID), `{"tz":"Mars/Base"}`, asUser(2)); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad tz: %d", rec.Code)
	}

	n, err := svc.ReconcileOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("reconcile n=%d err=%v", n, err)
	}
	if st.desired["SN1"]["lock"] != true {
		t.Fatalf("desired=%v", st.desired["SN1"])
	}
	if svc.M.Get(MLocksApplied) != 1 || svc.M.Get(MReconcileRuns) != 1 {
		t.Fatalf("metrics locks=%d runs=%d", svc.M.Get(MLocksApplied), svc.M.Get(MReconcileRuns))
	}
	// 幂等：desired 已等于应有值 → 不再下发（避免下发风暴）
	if n, _ := svc.ReconcileOnce(ctx); n != 0 {
		t.Fatalf("second reconcile n=%d", n)
	}

	// 教师临时解锁：patch lock=false + lock_expires_at，写 temp_unlock 留痕
	rec = doReq(h, "POST", "/api/v1/orgs/1/devices/SN1/unlock", `{"minutes":30}`, asUser(2))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlock: %d %s", rec.Code, rec.Body.String())
	}
	if st.desired["SN1"]["lock"] != false || st.desired["SN1"]["lock_expires_at"] == nil {
		t.Fatalf("desired after unlock=%v", st.desired["SN1"])
	}
	if svc.M.Get(MUnlocks) != 1 {
		t.Fatalf("unlocks=%d", svc.M.Get(MUnlocks))
	}
	var tempUnlock int
	for _, a := range st.audits {
		if a.Action == "temp_unlock" {
			tempUnlock++
		}
	}
	if tempUnlock != 1 {
		t.Fatalf("temp_unlock audits=%d (%+v)", tempUnlock, st.audits)
	}
	// 解锁未到期：对账不动它（教师解锁优先于课表）
	if n, _ := svc.ReconcileOnce(ctx); n != 0 || st.desired["SN1"]["lock"] != false {
		t.Fatalf("reconcile during temp unlock: n=%d desired=%v", n, st.desired["SN1"])
	}
	// 到期后补回锁
	svc.Now = func() time.Time { return fixedNow.Add(31 * time.Minute) }
	if n, _ := svc.ReconcileOnce(ctx); n != 1 || st.desired["SN1"]["lock"] != true {
		t.Fatalf("after expiry: n=%d desired=%v", n, st.desired["SN1"])
	}
	// 解锁时长受 org.unlock_max_minutes 上限约束
	svc.Now = func() time.Time { return fixedNow }
	rec = doReq(h, "POST", "/api/v1/orgs/1/devices/SN1/unlock", `{"minutes":10000}`, asUser(1))
	exp, _ := dataOf(t, rec)["lock_expires_at"].(string)
	got, _ := time.Parse(time.RFC3339, exp)
	if want := fixedNow.Add(240 * time.Minute); !got.Equal(want) {
		t.Fatalf("unlock cap: %v want %v", got, want)
	}
	// 设备不属本组织 → 404
	if rec := doReq(h, "POST", "/api/v1/orgs/1/devices/SNX/unlock", "", asUser(2)); rec.Code != http.StatusNotFound {
		t.Fatalf("foreign device unlock: %d", rec.Code)
	}
}

func TestHealthzAndMetrics(t *testing.T) {
	_, _, h := seed(t)
	if rec := doReq(h, "GET", "/healthz", "", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "fleet-svc") {
		t.Fatalf("healthz: %d %s", rec.Code, rec.Body.String())
	}
	rec := doReq(h, "GET", "/metrics", "", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "fleet_dispatch_ok 0") {
		t.Fatalf("metrics: %d %s", rec.Code, rec.Body.String())
	}
}
