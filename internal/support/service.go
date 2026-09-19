package support

import (
	"context"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// TDQuerier 是批次聚合依赖的 TDengine 子集（*tdengine.Client 满足）。
type TDQuerier interface {
	Query(ctx context.Context, sql string) (*tdengine.Result, error)
}

// DeviceAPI 是 support-svc 对 deviceapi 的调用抽象（HTTP 实现见 clients.go；测试用 fake）。
type DeviceAPI interface {
	Shadow(ctx context.Context, sn string) (ShadowResp, error)
	// PostCmd 以指定来源下发指令；返回 cmd_id 与 HTTP 状态（403 = 无授权）。
	PostCmd(ctx context.Context, sn, action string, params map[string]any, source, operator, grantID string) (cmdID string, status int, err error)
	// GetCmd 查指令结果：status = dispatched | acked。
	GetCmd(ctx context.Context, cmdID string) (CmdResult, error)
}

// CmdResult 是 deviceapi GET /cmds/{id} 的 data。
type CmdResult struct {
	CmdID  string `json:"cmd_id"`
	Status string `json:"status"`
	Ack    any    `json:"ack,omitempty"`
}

// Service 编排全部能力。
type Service struct {
	Store   Store
	Devices DeviceAPI
	TD      TelemetrySource
	TDQ     TDQuerier
	M       *Metrics
	Now     func() time.Time

	// Audit 是指令授权对账器（INC-6-02）；nil 时 /internal/support/audit/run 返回 503。
	Audit *AuditReconciler

	// SourceTimeoutOverride 诊断包单源超时（0 = SourceTimeout）。
	SourceTimeoutOverride time.Duration
	// SelfCheckPoll 自检结果轮询上限（0 = SelfCheckPollTimeout）。
	SelfCheckPoll time.Duration
	// SelfCheckPollInterval 轮询间隔（0 = 500ms）。
	SelfCheckPollInterval time.Duration
}

func NewService(st Store, dev DeviceAPI, td TelemetrySource, tdq TDQuerier, m *Metrics) *Service {
	if m == nil {
		m = NewMetrics()
	}
	return &Service{Store: st, Devices: dev, TD: td, TDQ: tdq, M: m}
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Service) sourceTimeout() time.Duration {
	if s.SourceTimeoutOverride > 0 {
		return s.SourceTimeoutOverride
	}
	return SourceTimeout
}
