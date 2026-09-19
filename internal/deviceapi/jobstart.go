package deviceapi

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
)

const (
	// ActionJobStart 是 BL5 队列调度器下发的作业指令。
	ActionJobStart = "job_start"
	// SourceFleet 是 job_start 唯一合法来源头（X-Source）。fleet-svc 的调度器带这个头。
	SourceFleet = "fleet"
	// MaxShadowBatch 是批量影子接口一次最多查询的 SN 数（BL5 技术方案 §17.3）。
	MaxShadowBatch = 200
)

// DecideJobStart 纯函数（BL5 §17.3）：只有 fleet 来源、设备空闲（work_state==0）且未锁定才允许下发作业。
// 其余一律 403 / 10003 并给出原因——「禁用只作用于开始新任务」，所以这里拒绝得越明确越好。
// workState < 0 表示影子缺失或非法，按非空闲处理（fail-closed）。
func DecideJobStart(source string, workState int, locked bool) Decision {
	deny := func(msg string) Decision {
		return Decision{HTTPStatus: http.StatusForbidden, BizCode: httpx.CodeDenied, Msg: msg}
	}
	switch {
	case strings.ToLower(strings.TrimSpace(source)) != SourceFleet:
		return deny("job_start requires X-Source: fleet")
	case locked:
		return deny("device is locked")
	case workState != 0:
		return deny("device is not idle")
	default:
		return Decision{Allowed: true, HTTPStatus: http.StatusOK, BizCode: httpx.CodeOK}
	}
}

// ShadowWorkState 解析 reported.work_state；缺失或非法 → -1（未知）。
func ShadowWorkState(reported map[string]string) int {
	v, ok := reported["work_state"]
	if !ok {
		return -1
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return -1
	}
	return int(f)
}

// ShadowLocked 解析 reported 的锁态：lock_state 非 0 或 lock 为真都算锁定。
func ShadowLocked(reported map[string]string) bool {
	if v, ok := reported["lock_state"]; ok {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil && f != 0 {
			return true
		}
	}
	b, ok := ParseBoolLoose(reported["lock"])
	return ok && b
}

// decideJobStart 读影子后调纯函数；Redis 不可用 → 影子未知 → 拒绝（fail-closed）。
func (s *Server) decideJobStart(ctx context.Context, sn, source string) Decision {
	reported := map[string]string{}
	if s.RDB != nil {
		if m, err := shadow.Read(ctx, s.RDB, sn); err == nil {
			reported = m
		}
	}
	return DecideJobStart(source, ShadowWorkState(reported), ShadowLocked(reported))
}

// POST /api/v1/devices/shadows —— 批量读影子（看板用），body {"sns":[...]}，上限 MaxShadowBatch。
func (s *Server) postShadows(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SNs []string `json:"sns"`
	}
	if err := httpx.Decode(r, &req); err != nil {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
		return
	}
	if len(req.SNs) == 0 {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "sns required")
		return
	}
	if len(req.SNs) > MaxShadowBatch {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "too many sns: max "+strconv.Itoa(MaxShadowBatch))
		return
	}
	out := make(map[string]map[string]string, len(req.SNs))
	for _, sn := range req.SNs {
		if _, done := out[sn]; done {
			continue
		}
		reported := map[string]string{}
		if s.RDB != nil {
			m, err := shadow.Read(r.Context(), s.RDB, sn)
			if err != nil {
				httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
				return
			}
			reported = m
		}
		out[sn] = reported
	}
	httpx.OK(w, map[string]any{"shadows": out})
}
