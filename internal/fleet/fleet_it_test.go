package fleet

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
	"github.com/xtool/xtool-aiot/internal/pkg/schedule"
)

// 打真实 PG（sql/bl5.sql 已应用）：租户隔离、队列提交审批、调度器零重复下发、课表对账写 shadow_desired。
// 用独立 org / SN，结束时清理干净，不碰别人的数据。
func TestFleetIT(t *testing.T) {
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := config.MustPG(ctx)
	defer pool.Close()
	st := NewPGStore(pool)

	stamp := time.Now().UnixNano() % 1_000_000
	admin, teacher, student, outsider := stamp*10+1, stamp*10+2, stamp*10+3, stamp*10+4
	sn := fmt.Sprintf("ITFLEET%d", stamp)

	orgA, err := st.CreateOrg(ctx, fmt.Sprintf("IT Fleet A %d", stamp), "school", "US", admin)
	if err != nil {
		t.Fatalf("create org A: %v", err)
	}
	orgB, err := st.CreateOrg(ctx, fmt.Sprintf("IT Fleet B %d", stamp), "studio", "US", outsider)
	if err != nil {
		t.Fatalf("create org B: %v", err)
	}
	defer func() {
		bg := context.Background()
		for _, q := range []string{
			`DELETE FROM iot_shard.queue_item WHERE org_id = ANY($1)`,
			`DELETE FROM iot_shard.job_queue WHERE org_id = ANY($1)`,
			`DELETE FROM iot_shard.schedule_policy WHERE org_id = ANY($1)`,
			`DELETE FROM iot_shard.device_org WHERE org_id = ANY($1)`,
			`DELETE FROM iot_shard.lock_audit WHERE org_id = ANY($1)`,
			`DELETE FROM iot_shard.org_member WHERE org_id = ANY($1)`,
			`DELETE FROM iot_global.site WHERE org_id = ANY($1)`,
			`DELETE FROM iot_global.org WHERE org_id = ANY($1)`,
		} {
			if _, err := pool.Exec(bg, q, []int64{orgA.OrgID, orgB.OrgID}); err != nil {
				t.Logf("cleanup %q: %v", q, err)
			}
		}
		if _, err := pool.Exec(bg, `DELETE FROM iot_shard.shadow_desired WHERE sn=$1`, sn); err != nil {
			t.Logf("cleanup shadow_desired: %v", err)
		}
	}()

	if err := st.AddMember(ctx, orgA.OrgID, teacher, RoleTeacher, admin); err != nil {
		t.Fatal(err)
	}
	if err := st.AddMember(ctx, orgA.OrgID, student, RoleStudent, admin); err != nil {
		t.Fatal(err)
	}
	site, err := st.CreateSite(ctx, orgA.OrgID, "IT Room", "UTC")
	if err != nil {
		t.Fatal(err)
	}

	sh := newFakeShadows()
	sh.set(sn, 0, false, time.Now()) // 空闲未锁
	svc := NewService(st, sh, NewMetrics(), DefaultOptions())
	h := Routes(svc, "it")
	orgPath := fmt.Sprintf("/api/v1/orgs/%d", orgA.OrgID)

	// 归属设备（干净设备，走 ResolveOwnership 的 allow 分支）
	if err := svc.Attach(ctx, orgA.OrgID, admin, sn, site.SiteID); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if d, err := st.GetDevice(ctx, orgA.OrgID, sn); err != nil || d.SiteID != site.SiteID {
		t.Fatalf("device %+v err=%v", d, err)
	}

	// 越权：org B 的 admin 访问 org A 的资源 → 404
	for _, p := range []string{orgPath, orgPath + "/devices", orgPath + "/fleet/summary", orgPath + "/queues"} {
		if rec := doReq(h, "GET", p, "", asUser(outsider)); rec.Code != http.StatusNotFound {
			t.Fatalf("cross-tenant %s: %d %s", p, rec.Code, rec.Body.String())
		}
	}
	// 本组织学生可看看板
	if rec := doReq(h, "GET", orgPath+"/fleet/summary", "", asUser(student)); rec.Code != http.StatusOK {
		t.Fatalf("summary: %d %s", rec.Code, rec.Body.String())
	}

	// 建队列 → 学生提交 → 教师审批
	rec := doReq(h, "POST", orgPath+"/queues", fmt.Sprintf(`{"site_id":%d,"name":"IT class","device_sns":[%q]}`, site.SiteID, sn), asUser(admin))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create queue: %d %s", rec.Code, rec.Body.String())
	}
	queueID := int64(dataOf(t, rec)["queue_id"].(float64))
	itemsPath := fmt.Sprintf("%s/queues/%d/items", orgPath, queueID)
	rec = doReq(h, "POST", itemsPath, fmt.Sprintf(`{"file_sha256":%q,"est_minutes":3}`, strings.Repeat("c", 64)), asUser(student))
	if rec.Code != http.StatusCreated {
		t.Fatalf("submit: %d %s", rec.Code, rec.Body.String())
	}
	itemID := dataOf(t, rec)["item_id"].(string)
	if rec := doReq(h, "POST", fmt.Sprintf("%s/items/%s/approve", orgPath, itemID), "", asUser(student)); rec.Code != http.StatusForbidden {
		t.Fatalf("student approve must be 403: %d", rec.Code)
	}
	rec = doReq(h, "POST", fmt.Sprintf("%s/items/%s/approve", orgPath, itemID), "", asUser(teacher))
	if rec.Code != http.StatusOK {
		t.Fatalf("approve: %d %s", rec.Code, rec.Body.String())
	}

	// 调度器：下发一次，第二轮不重复
	d := &fakeDispatcher{}
	sc := NewScheduler(svc, d)
	if n, err := sc.DispatchOnce(ctx); err != nil || n != 1 || d.count() != 1 {
		t.Fatalf("dispatch round 1: n=%d err=%v calls=%v", n, err, d.calls)
	}
	if n, err := sc.DispatchOnce(ctx); err != nil || n != 0 || d.count() != 1 {
		t.Fatalf("dispatch round 2 must not repeat: n=%d err=%v calls=%v", n, err, d.calls)
	}
	it, err := st.GetItem(ctx, orgA.OrgID, itemID)
	if err != nil || it.Status != StDispatched || it.AssignedSN != sn || it.DispatchedCmdID != "cmd-1" {
		t.Fatalf("item after dispatch: %+v err=%v", it, err)
	}

	// 课表对账：今天整天禁用 → desired.lock = true
	today := time.Now().UTC().Format("2006-01-02")
	p := schedule.Policy{TZ: "UTC", Weekly: map[string][]schedule.Window{}, Overrides: []schedule.Override{{Date: today}}}
	if _, err := svc.PutPolicy(ctx, orgA.OrgID, site.SiteID, p, admin); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	if _, err := svc.ReconcileOnce(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	dl, err := st.GetDesiredLock(ctx, sn)
	if err != nil || dl.Lock == nil || !*dl.Lock {
		t.Fatalf("desired lock after reconcile: %+v err=%v", dl, err)
	}
	// 幂等：第二轮不再对该 SN 下发
	before := svc.M.Get(MLocksApplied)
	if _, err := svc.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := svc.M.Get(MLocksApplied); got != before {
		t.Fatalf("reconcile not idempotent: %d → %d", before, got)
	}
	// 留痕落到分区表
	audits, err := st.ListLockAudit(ctx, orgA.OrgID, sn, time.Now().Add(-time.Hour), 10)
	if err != nil || len(audits) == 0 {
		t.Fatalf("lock audit: %d rows err=%v", len(audits), err)
	}
}
