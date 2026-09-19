package support

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

// 授权状态机（§4.2）：requested → granted → （过期靠 expires_at）；requested → denied；granted → revoked。
// 全部迁移用「带前置状态的 UPDATE」，affected=0 → ErrConflict；Allowed 与 SQL 的 IN 列表来自同一张表。
const (
	GrantRequested = "requested"
	GrantGranted   = "granted"
	GrantDenied    = "denied"
	GrantRevoked   = "revoked"

	// GrantTTL 授权有效期。
	GrantTTL = 15 * time.Minute
)

var grantTransitions = map[string]map[string]bool{
	GrantRequested: {GrantGranted: true, GrantDenied: true},
	GrantGranted:   {GrantRevoked: true},
	GrantDenied:    {},
	GrantRevoked:   {},
}

// GrantAllowed 报告 from→to 是否合法迁移。
func GrantAllowed(from, to string) bool {
	tos, ok := grantTransitions[from]
	return ok && tos[to]
}

// GrantSources 返回可迁往 to 的前置状态（排序稳定），用于 `WHERE status = ANY($n)`。
func GrantSources(to string) []string {
	var out []string
	for from, tos := range grantTransitions {
		if tos[to] {
			out = append(out, from)
		}
	}
	sort.Strings(out)
	return out
}

// Grant 是 support_grant 的一行。
type Grant struct {
	GrantID     string     `json:"grant_id"`
	SN          string     `json:"sn"`
	TicketID    string     `json:"ticket_id"`
	Operator    string     `json:"operator"`
	Actions     []string   `json:"actions"`
	Status      string     `json:"status"`
	RequestedAt time.Time  `json:"requested_at"`
	GrantedAt   *time.Time `json:"granted_at,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
	DeniedAt    *time.Time `json:"denied_at,omitempty"`
}

// Effective 报告 grant 在 now 是否有效（granted、未过期、未撤回）。纯函数。
func (g Grant) Effective(now time.Time) bool {
	return g.Status == GrantGranted && g.RevokedAt == nil && g.ExpiresAt != nil && g.ExpiresAt.After(now)
}

// ValidGrantRequest 校验请求：sn / ticket / operator 非空；actions 只允许 self_check（缺省即 self_check）。
func ValidGrantRequest(sn, ticket, operator string, actions []string) ([]string, error) {
	if !identRe.MatchString(sn) {
		return nil, fmt.Errorf("%w: sn", ErrBadParam)
	}
	if strings.TrimSpace(ticket) == "" || len(ticket) > 64 {
		return nil, fmt.Errorf("%w: ticket_id", ErrBadParam)
	}
	if strings.TrimSpace(operator) == "" || len(operator) > 64 {
		return nil, fmt.Errorf("%w: operator", ErrBadParam)
	}
	if len(actions) == 0 {
		return []string{grantcheck.ActionSelfCheck}, nil
	}
	for _, a := range actions {
		if a != grantcheck.ActionSelfCheck {
			return nil, fmt.Errorf("%w: action %q not grantable (P0 only self_check)", ErrBadParam, a)
		}
	}
	return []string{grantcheck.ActionSelfCheck}, nil
}

// NewID 生成 RFC 4122 v4 UUID。
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// RequestGrant：客服发起（xpilot 侧只能 request）。
func (s *Service) RequestGrant(ctx context.Context, sn, ticket, operator string, actions []string) (Grant, error) {
	acts, err := ValidGrantRequest(sn, ticket, operator, actions)
	if err != nil {
		return Grant{}, err
	}
	g := Grant{GrantID: NewID(), SN: sn, TicketID: ticket, Operator: operator, Actions: acts, Status: GrantRequested, RequestedAt: s.now()}
	if err := s.Store.InsertGrant(ctx, g); err != nil {
		return Grant{}, err
	}
	s.M.Inc(MGrantRequested)
	return g, nil
}

// ApproveGrant：用户确认。userID 必须是该 SN 的有效 owner（BFF 已校验，服务再查一次不信任上游）。
func (s *Service) ApproveGrant(ctx context.Context, id string, userID int64) (Grant, error) {
	return s.userTransition(ctx, id, userID, GrantGranted, MGrantApproved)
}

// DenyGrant：用户拒绝。
func (s *Service) DenyGrant(ctx context.Context, id string, userID int64) (Grant, error) {
	return s.userTransition(ctx, id, userID, GrantDenied, MGrantDenied)
}

// RevokeGrant：用户撤回（5 s 内 deviceapi 拒绝：grantcheck 每次直查 PG）。
func (s *Service) RevokeGrant(ctx context.Context, id string, userID int64) (Grant, error) {
	return s.userTransition(ctx, id, userID, GrantRevoked, MGrantRevoked)
}

func (s *Service) userTransition(ctx context.Context, id string, userID int64, to, metric string) (Grant, error) {
	g, err := s.Store.GetGrant(ctx, id)
	if err != nil {
		return Grant{}, err
	}
	owner, err := s.Store.IsOwner(ctx, g.SN, userID)
	if err != nil {
		return Grant{}, err
	}
	if !owner {
		return Grant{}, fmt.Errorf("%w: user %d is not owner of %s", ErrDenied, userID, g.SN)
	}
	ok, err := s.Store.TransitionGrant(ctx, id, to, s.now())
	if err != nil {
		return Grant{}, err
	}
	if !ok {
		s.M.Inc(MGrantConflict)
		return Grant{}, fmt.Errorf("%w: grant %s cannot move %s → %s", ErrConflict, id, g.Status, to)
	}
	s.M.Inc(metric)
	return s.Store.GetGrant(ctx, id)
}
