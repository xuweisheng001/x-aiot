package support

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

// 头约定（BFF / xpilot 服务身份注入；本服务只做归属与状态校验，不信任任何头的授权含义）。
const (
	HeaderUserID  = "X-User-Id"
	HeaderAgentID = "X-Agent-Id"
)

// Routes 组装 support-svc 的 HTTP 路由（:8095）。
// 三组路由：客服/工单（/api/v1/support/*）、用户可见（/api/v1/devices/{sn}/bundles）、Agent 只读代理（/api/v1/agent/*）。
func Routes(svc *Service, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz("support-svc", version))
	mux.Handle("GET /metrics", svc.M.Handler())

	// ---- 诊断包 ----
	mux.HandleFunc("POST /api/v1/support/bundles", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SN       string `json:"sn"`
			Trigger  string `json:"trigger"`
			TicketID string `json:"ticket_id"`
			UserNote string `json:"user_note"`
		}
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		if req.Trigger == "" {
			req.Trigger = "support"
		}
		b, err := svc.Generate(r.Context(), req.SN, req.Trigger, req.TicketID, req.UserNote)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: b})
	})
	mux.HandleFunc("GET /api/v1/support/bundles/{id}", func(w http.ResponseWriter, r *http.Request) {
		b, err := svc.GetBundle(r.Context(), r.PathValue("id"))
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, b)
	})
	// 用户可查看自己设备被生成过哪些诊断包（归属由 BFF 校验）
	mux.HandleFunc("GET /api/v1/devices/{sn}/bundles", func(w http.ResponseWriter, r *http.Request) {
		sn := r.PathValue("sn")
		if !identRe.MatchString(sn) {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "sn")
			return
		}
		list, err := svc.Store.ListBundles(r.Context(), sn, 20)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, list)
	})

	// ---- 授权 ----
	mux.HandleFunc("POST /api/v1/support/grants", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SN          string   `json:"sn"`
			TicketID    string   `json:"ticket_id"`
			RequestedBy string   `json:"requested_by"`
			Actions     []string `json:"actions"`
		}
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		if op := strings.TrimSpace(r.Header.Get(grantcheck.HeaderOperator)); op != "" {
			req.RequestedBy = op
		}
		g, err := svc.RequestGrant(r.Context(), req.SN, req.TicketID, req.RequestedBy, req.Actions)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: g})
	})
	mux.HandleFunc("GET /api/v1/support/grants/{id}", func(w http.ResponseWriter, r *http.Request) {
		g, err := svc.Store.GetGrant(r.Context(), r.PathValue("id"))
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, g)
	})
	// 用户侧三态迁移：confirm / deny / revoke，X-User-Id 由 BFF 注入，服务再查 owner 归属（不信任上游）。
	userTransition := func(fn func(ctx context.Context, id string, uid int64) (Grant, error)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			uid, ok := userID(r)
			if !ok {
				httpx.Error(w, http.StatusUnauthorized, httpx.CodeDenied, "X-User-Id required")
				return
			}
			g, err := fn(r.Context(), r.PathValue("id"), uid)
			if err != nil {
				writeErr(w, err)
				return
			}
			httpx.OK(w, g)
		}
	}
	mux.HandleFunc("POST /api/v1/support/grants/{id}/confirm", userTransition(svc.ApproveGrant))
	mux.HandleFunc("POST /api/v1/support/grants/{id}/deny", userTransition(svc.DenyGrant))
	mux.HandleFunc("POST /api/v1/support/grants/{id}/revoke", userTransition(svc.RevokeGrant))

	// ---- 客服自检（source=support）----
	mux.HandleFunc("POST /api/v1/support/grants/{id}/self_check", func(w http.ResponseWriter, r *http.Request) {
		op := strings.TrimSpace(r.Header.Get(grantcheck.HeaderOperator))
		if op == "" {
			httpx.Error(w, http.StatusUnauthorized, httpx.CodeDenied, "X-Operator required")
			return
		}
		res, err := svc.SelfCheck(r.Context(), r.PathValue("id"), grantcheck.SourceSupport, op)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, res)
	})
	mux.HandleFunc("GET /api/v1/support/tickets/{ticket_id}/cmds", func(w http.ResponseWriter, r *http.Request) {
		out, err := svc.TicketCmds(r.Context(), r.PathValue("ticket_id"))
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, out)
	})

	// ---- 保修数据汇总（只给数据不给判定）----
	mux.HandleFunc("GET /api/v1/support/warranty/{sn}", func(w http.ResponseWriter, r *http.Request) {
		c, err := svc.GenerateWarranty(r.Context(), r.PathValue("sn"), r.URL.Query().Get("ticket_id"))
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, c)
	})

	// ---- 错误码字典 ----
	mux.HandleFunc("POST /internal/support/dict", func(w http.ResponseWriter, r *http.Request) {
		var e DictEntry
		if err := httpx.Decode(r, &e); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		out, err := svc.PublishDict(r.Context(), e)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: out})
	})
	mux.HandleFunc("GET /api/v1/support/dict/{code}", func(w http.ResponseWriter, r *http.Request) {
		e, err := svc.GetDict(r.Context(), r.PathValue("code"))
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, e)
	})

	// ---- 批次缺陷 ----
	mux.HandleFunc("POST /internal/support/defects/run", func(w http.ResponseWriter, r *http.Request) {
		rep, err := svc.RunDefectOnce(r.Context())
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, rep)
	})
	// ---- 指令授权对账（INC-6-02）----
	mux.HandleFunc("POST /internal/support/audit/run", func(w http.ResponseWriter, r *http.Request) {
		if svc.Audit == nil {
			httpx.Error(w, http.StatusServiceUnavailable, httpx.CodeInternal, "audit reconciler not configured")
			return
		}
		rep, err := svc.Audit.RunOnce(r.Context())
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, rep)
	})
	mux.HandleFunc("GET /api/v1/support/defects", func(w http.ResponseWriter, r *http.Request) {
		hours, _ := strconv.Atoi(r.URL.Query().Get("hours"))
		if hours <= 0 {
			hours = 24 * 7
		}
		list, err := svc.Store.ListDefectAlerts(r.Context(), svc.now().Add(-time.Duration(hours)*time.Hour), 100)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, list)
	})

	// ---- Agent 只读代理（独立路由组：三读一写）----
	mux.HandleFunc("GET /api/v1/agent/devices/{sn}/context", func(w http.ResponseWriter, r *http.Request) {
		out, err := svc.AgentContextFor(r.Context(), r.PathValue("sn"), strings.TrimSpace(r.Header.Get(HeaderAgentID)), r.URL.Query().Get("ticket_id"))
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, out)
	})
	mux.HandleFunc("POST /api/v1/agent/devices/{sn}/self_check", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			GrantID string `json:"grant_id"`
		}
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		res, err := svc.AgentSelfCheck(r.Context(), r.PathValue("sn"), req.GrantID, strings.TrimSpace(r.Header.Get(HeaderAgentID)))
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, res)
	})
	// Agent 对其它任何指令一律 403（路由层即拒绝，不进 deviceapi）
	mux.HandleFunc("POST /api/v1/agent/devices/{sn}/cmd", func(w http.ResponseWriter, _ *http.Request) {
		svc.M.Inc(MAgentRefused)
		httpx.Error(w, http.StatusForbidden, httpx.CodeDenied, "agent may only self_check")
	})

	ipKey := func(r *http.Request) *string {
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			return nil
		}
		ip := httpx.ClientIP(r)
		return &ip
	}
	return httpx.Chain(mux, httpx.Recover, httpx.Logging, httpx.RateLimitByKey(ipKey, 50, 100))
}

func userID(r *http.Request) (int64, bool) {
	v, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get(HeaderUserID)), 10, 64)
	return v, err == nil && v > 0
}

// writeErr 把包内错误映射到 HTTP 状态与业务码。
func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, http.StatusNotFound, httpx.CodeNotFound, err.Error())
	case errors.Is(err, ErrGone):
		httpx.Error(w, http.StatusGone, httpx.CodeNotFound, err.Error())
	case errors.Is(err, ErrConflict):
		httpx.Error(w, http.StatusConflict, httpx.CodeConflict, err.Error())
	case errors.Is(err, ErrDenied):
		httpx.Error(w, http.StatusForbidden, httpx.CodeDenied, err.Error())
	case errors.Is(err, ErrBadParam):
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, err.Error())
	case errors.Is(err, ErrUnavailable):
		httpx.Error(w, http.StatusServiceUnavailable, httpx.CodeInternal, err.Error())
	default:
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
	}
}
