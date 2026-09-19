package support

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

// SelfCheckPollTimeout 自检回执轮询上限（§6：轮询至 10 s，超时回填「未知」）。
const SelfCheckPollTimeout = 10 * time.Second

// CmdTicket 是 cmd_ticket_map 的一行。
type CmdTicket struct {
	CmdID     string    `json:"cmd_id"`
	TicketID  string    `json:"ticket_id"`
	GrantID   string    `json:"grant_id"`
	SN        string    `json:"sn"`
	Source    string    `json:"source"`
	CreatedAt time.Time `json:"created_at"`
}

// SelfCheckResult 是一次自检的结果。
type SelfCheckResult struct {
	CmdID    string `json:"cmd_id"`
	GrantID  string `json:"grant_id"`
	TicketID string `json:"ticket_id"`
	SN       string `json:"sn"`
	Status   string `json:"status"` // acked | unknown
	Ack      any    `json:"ack,omitempty"`
}

// SelfCheck：按 grant 发起自检。source ∈ support | agent；operator 是工号或 agent_id。
// 服务先自查 grant 有效性（快速失败），deviceapi 再直查一次（授权以 deviceapi 为准，双保险）。
func (s *Service) SelfCheck(ctx context.Context, grantID, source, operator string) (SelfCheckResult, error) {
	if source != grantcheck.SourceSupport && source != grantcheck.SourceAgent {
		return SelfCheckResult{}, fmt.Errorf("%w: source must be support or agent", ErrBadParam)
	}
	g, err := s.Store.GetGrant(ctx, grantID)
	if err != nil {
		return SelfCheckResult{}, err
	}
	if !g.Effective(s.now()) {
		s.M.Inc(MSelfCheckRefused)
		return SelfCheckResult{}, fmt.Errorf("%w: grant %s not effective (status=%s)", ErrDenied, grantID, g.Status)
	}
	if s.Devices == nil {
		return SelfCheckResult{}, ErrUnavailable
	}
	cmdID, status, err := s.Devices.PostCmd(ctx, g.SN, grantcheck.ActionSelfCheck, map[string]any{"grant_id": g.GrantID}, source, operator, g.GrantID)
	if err != nil {
		return SelfCheckResult{}, fmt.Errorf("%w: deviceapi: %v", ErrUnavailable, err)
	}
	if status == http.StatusForbidden {
		s.M.Inc(MSelfCheckRefused)
		return SelfCheckResult{}, fmt.Errorf("%w: deviceapi refused (no valid grant)", ErrDenied)
	}
	if status != http.StatusOK || cmdID == "" {
		return SelfCheckResult{}, fmt.Errorf("%w: deviceapi status %d", ErrUnavailable, status)
	}
	// cmd → 工单映射落 PG（INC-6-18：回填校验 SN）
	if err := s.Store.InsertCmdTicket(ctx, CmdTicket{CmdID: cmdID, TicketID: g.TicketID, GrantID: g.GrantID, SN: g.SN, Source: source, CreatedAt: s.now()}); err != nil {
		return SelfCheckResult{}, err
	}
	s.M.Inc(MSelfCheckOK)
	res := SelfCheckResult{CmdID: cmdID, GrantID: g.GrantID, TicketID: g.TicketID, SN: g.SN, Status: "unknown"}
	// 轮询回执
	deadline := s.now().Add(s.pollTimeout())
	for {
		r, err := s.Devices.GetCmd(ctx, cmdID)
		if err == nil && r.Status == "acked" {
			res.Status, res.Ack = "acked", r.Ack
			return res, nil
		}
		if !s.now().Before(deadline) || ctx.Err() != nil {
			s.M.Inc(MSelfCheckUnknown)
			return res, nil
		}
		select {
		case <-ctx.Done():
			s.M.Inc(MSelfCheckUnknown)
			return res, nil
		case <-time.After(s.pollInterval()):
		}
	}
}

func (s *Service) pollTimeout() time.Duration {
	if s.SelfCheckPoll > 0 {
		return s.SelfCheckPoll
	}
	return SelfCheckPollTimeout
}

func (s *Service) pollInterval() time.Duration {
	if s.SelfCheckPollInterval > 0 {
		return s.SelfCheckPollInterval
	}
	return 500 * time.Millisecond
}

// TicketCmds 汇总工单下的指令与结果（结果优先 deviceapi/Redis，回退 cmd_audit）。
func (s *Service) TicketCmds(ctx context.Context, ticketID string) ([]map[string]any, error) {
	list, err := s.Store.ListCmdTickets(ctx, ticketID)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(list))
	for _, m := range list {
		item := map[string]any{"cmd_id": m.CmdID, "grant_id": m.GrantID, "sn": m.SN, "source": m.Source, "created_at": m.CreatedAt, "status": "unknown"}
		if s.Devices != nil {
			if r, err := s.Devices.GetCmd(ctx, m.CmdID); err == nil && r.Status != "" {
				item["status"] = r.Status
				if r.Ack != nil {
					item["ack"] = r.Ack
				}
			}
		}
		if item["status"] == "unknown" || item["status"] == "dispatched" {
			if res, err := s.Store.AuditResult(ctx, m.CmdID); err == nil && res != "" {
				item["audit_result"] = res
			}
		}
		out = append(out, item)
	}
	return out, nil
}
