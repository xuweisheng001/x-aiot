package support

import (
	"context"
	"errors"
	"testing"
	"time"
)

var recBase = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func grantWin(sn string, fromMin, toMin int, revokedMin *int) GrantWindow {
	g := GrantWindow{GrantID: "g-" + sn, SN: sn,
		GrantedAt: recBase.Add(time.Duration(fromMin) * time.Minute),
		ExpiresAt: recBase.Add(time.Duration(toMin) * time.Minute)}
	if revokedMin != nil {
		t := recBase.Add(time.Duration(*revokedMin) * time.Minute)
		g.RevokedAt = &t
	}
	return g
}

func cmdAt(id, sn string, min int) AuditCmd {
	return AuditCmd{CmdID: id, SN: sn, Action: "self_check", Source: "support", Operator: "cs-1",
		Result: "dispatched", CreatedAt: recBase.Add(time.Duration(min) * time.Minute)}
}

func TestUnauthorizedCmds(t *testing.T) {
	revoked5 := 5
	cases := []struct {
		name   string
		cmds   []AuditCmd
		grants []GrantWindow
		want   []string // 期望越权的 cmd_id
	}{
		{
			name:   "正好在窗口内",
			cmds:   []AuditCmd{cmdAt("c1", "SN1", 5)},
			grants: []GrantWindow{grantWin("SN1", 0, 15, nil)},
		},
		{
			name:   "正好等于 granted_at（闭区间左端）",
			cmds:   []AuditCmd{cmdAt("c1", "SN1", 0)},
			grants: []GrantWindow{grantWin("SN1", 0, 15, nil)},
		},
		{
			name:   "早于 granted_at",
			cmds:   []AuditCmd{cmdAt("c1", "SN1", -1)},
			grants: []GrantWindow{grantWin("SN1", 0, 15, nil)},
			want:   []string{"c1"},
		},
		{
			name:   "晚于 expires_at",
			cmds:   []AuditCmd{cmdAt("c1", "SN1", 16)},
			grants: []GrantWindow{grantWin("SN1", 0, 15, nil)},
			want:   []string{"c1"},
		},
		{
			name:   "正好等于 expires_at（右开区间）",
			cmds:   []AuditCmd{cmdAt("c1", "SN1", 15)},
			grants: []GrantWindow{grantWin("SN1", 0, 15, nil)},
			want:   []string{"c1"},
		},
		{
			name:   "撤销之前合法、撤销之后越权",
			cmds:   []AuditCmd{cmdAt("c1", "SN1", 3), cmdAt("c2", "SN1", 5), cmdAt("c3", "SN1", 7)},
			grants: []GrantWindow{grantWin("SN1", 0, 15, &revoked5)},
			want:   []string{"c2", "c3"}, // revoked_at 当刻即失效
		},
		{
			name:   "grant 属于别的 SN",
			cmds:   []AuditCmd{cmdAt("c1", "SN2", 5)},
			grants: []GrantWindow{grantWin("SN1", 0, 15, nil)},
			want:   []string{"c1"},
		},
		{
			name: "多 grant 取任一命中",
			cmds: []AuditCmd{cmdAt("c1", "SN1", 25)},
			grants: []GrantWindow{
				grantWin("SN1", 0, 15, nil),
				{GrantID: "g2", SN: "SN1", GrantedAt: recBase.Add(20 * time.Minute), ExpiresAt: recBase.Add(35 * time.Minute)},
			},
		},
		{
			name: "无 grant：全部越权，且输出按 created_at 升序（输入乱序）",
			cmds: []AuditCmd{cmdAt("c2", "SN1", 9), cmdAt("c1", "SN1", 1)},
			want: []string{"c1", "c2"},
		},
		{
			name:   "grant 时间列缺失（异常行）不覆盖任何指令",
			cmds:   []AuditCmd{cmdAt("c1", "SN1", 5)},
			grants: []GrantWindow{{GrantID: "g0", SN: "SN1"}},
			want:   []string{"c1"},
		},
		{
			name: "空输入",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := UnauthorizedCmds(c.cmds, c.grants)
			if len(got) != len(c.want) {
				t.Fatalf("got %d unauthorized %v, want %v", len(got), ids(got), c.want)
			}
			for i, id := range c.want {
				if got[i].CmdID != id {
					t.Fatalf("unauthorized[%d]=%s want %s (all: %v)", i, got[i].CmdID, id, ids(got))
				}
			}
		})
	}
}

func ids(cs []AuditCmd) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.CmdID)
	}
	return out
}

type fakeAuditStore struct {
	cmds   []AuditCmd
	grants []GrantWindow
	err    error
}

func (f *fakeAuditStore) AuditCmdsSince(context.Context, time.Time) ([]AuditCmd, error) {
	return f.cmds, f.err
}
func (f *fakeAuditStore) GrantWindowsSince(context.Context, time.Time) ([]GrantWindow, error) {
	return f.grants, f.err
}

func TestAuditReconcilerRunOnce(t *testing.T) {
	st := &fakeAuditStore{
		cmds:   []AuditCmd{cmdAt("c1", "SN1", 5), cmdAt("c2", "SN2", 5)},
		grants: []GrantWindow{grantWin("SN1", 0, 15, nil)},
	}
	m := NewMetrics()
	r := NewAuditReconciler(st, m, 0)
	r.Now = func() time.Time { return recBase.Add(10 * time.Minute) }
	rep, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Checked != 2 || rep.Grants != 1 || rep.Unauthorized != 1 {
		t.Fatalf("report=%+v", rep)
	}
	if m.Get(MAuditReconcileRuns) != 1 || m.Get(MAuditChecked) != 2 || m.Get(MAuditUnauthorized) != 1 {
		t.Fatalf("metrics runs=%d checked=%d unauthorized=%d", m.Get(MAuditReconcileRuns), m.Get(MAuditChecked), m.Get(MAuditUnauthorized))
	}
	if r.Window != DefaultAuditWindow {
		t.Fatalf("window=%v", r.Window)
	}

	st.err = errors.New("pg down")
	if _, err := r.RunOnce(context.Background()); err == nil {
		t.Fatal("store error must surface")
	}
	if m.Get(MAuditErrors) != 1 {
		t.Fatalf("errors=%d", m.Get(MAuditErrors))
	}
}
