package support

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// 集成（IOT_IT）：真实 PG。窗口内的 support/agent 指令中，
// 只有被 grant 覆盖的那条不报；绕过 grant 的那条必须被对账抓出来；result=denied 的不重复报。
func TestIntegration_AuditReconcile(t *testing.T) {
	ctx, db, _, sn, cleanup := itSetup(t)
	defer cleanup()
	defer func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM iot_shard.cmd_audit WHERE sn=$1`, sn)
	}()

	store := &PGStore{DB: db}
	now := time.Now().UTC()
	// grant：now-10m 批准，5 分钟有效（now-5m 过期）
	gid := NewID()
	if _, err := db.Exec(ctx, `INSERT INTO iot_shard.support_grant(grant_id, sn, ticket_id, operator, status, requested_at, granted_at, expires_at)
		VALUES($1,$2,'T-REC','cs-rec','granted',$3,$3,$4)`, gid, sn, now.Add(-10*time.Minute), now.Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	ins := func(suffix, source, result string, at time.Time) string {
		id := fmt.Sprintf("%s-%s", sn, suffix)
		if _, err := db.Exec(ctx, `INSERT INTO iot_shard.cmd_audit(cmd_id, sn, action, params, operator, source, result, created_at)
			VALUES($1,$2,'self_check','{}'::jsonb,'cs-rec',$3,$4,$5)`, id, sn, source, result, at); err != nil {
			t.Fatal(err)
		}
		return id
	}
	okID := ins("ok", "support", "dispatched", now.Add(-8*time.Minute))     // grant 窗口内
	badID := ins("bad", "agent", "dispatched", now.Add(-2*time.Minute))     // 过期之后：越权
	deniedID := ins("denied", "support", "denied", now.Add(-2*time.Minute)) // 护栏已拦下：不重复报

	since := now.Add(-DefaultAuditWindow)
	cmds, err := store.AuditCmdsSince(ctx, since)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, c := range cmds {
		seen[c.CmdID] = true
	}
	if !seen[okID] || !seen[badID] {
		t.Fatalf("audit rows missing: ok=%v bad=%v", seen[okID], seen[badID])
	}
	if seen[deniedID] {
		t.Fatal("result=denied 行不应参与对账（护栏已经拦下并留痕）")
	}
	grants, err := store.GrantWindowsSince(ctx, since)
	if err != nil {
		t.Fatal(err)
	}
	mine := map[string]bool{}
	for _, c := range UnauthorizedCmds(cmds, grants) {
		if c.SN == sn {
			mine[c.CmdID] = true
		}
	}
	if len(mine) != 1 || !mine[badID] {
		t.Fatalf("unauthorized for %s = %v, want only %s", sn, mine, badID)
	}

	// 整轮跑通并计数（全库口径：只断言单调与本轮至少抓到我们这条）
	m := NewMetrics()
	r := NewAuditReconciler(store, m, DefaultAuditWindow)
	rep, err := r.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Checked < 2 || rep.Unauthorized < 1 {
		t.Fatalf("report=%+v", rep)
	}
	if m.Get(MAuditReconcileRuns) != 1 || m.Get(MAuditUnauthorized) < 1 || m.Get(MAuditErrors) != 0 {
		t.Fatalf("metrics runs=%d unauthorized=%d errors=%d", m.Get(MAuditReconcileRuns), m.Get(MAuditUnauthorized), m.Get(MAuditErrors))
	}
}
