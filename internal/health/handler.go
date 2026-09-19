package health

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// HeaderUserID 是 BFF 透传的用户标识；归因类接口必填（user_id 只进 reminder_attribution，不与 SN 并存）。
const HeaderUserID = "X-User-Id"

// status 把业务错误翻译为 HTTP 状态与业务码。
func status(err error) (int, int) {
	switch {
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound, httpx.CodeNotFound
	case errors.Is(err, ErrBadParam):
		return http.StatusBadRequest, httpx.CodeBadParam
	case errors.Is(err, ErrDenied):
		return http.StatusForbidden, httpx.CodeDenied
	case errors.Is(err, ErrConflict):
		return http.StatusConflict, httpx.CodeBadParam
	default:
		return http.StatusInternalServerError, httpx.CodeInternal
	}
}

func fail(w http.ResponseWriter, err error) {
	st, code := status(err)
	httpx.Error(w, st, code, err.Error())
}

// userID 从请求头解析；缺失或非法 → ok=false。
func userID(r *http.Request) (int64, bool) {
	v := strings.TrimSpace(r.Header.Get(HeaderUserID))
	if v == "" {
		return 0, false
	}
	id, err := strconv.ParseInt(v, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// Routes 装配 health-svc 的路由（spec §6）。
func Routes(svc *Service, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz("health-svc", version))
	mux.Handle("GET /metrics", svc.M.Handler())

	// 健康度只读
	mux.HandleFunc("GET /api/v1/health/{sn}", func(w http.ResponseWriter, r *http.Request) {
		resp, err := svc.GetHealth(r.Context(), r.PathValue("sn"))
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, resp)
	})
	mux.HandleFunc("GET /api/v1/health/{sn}/reminders", func(w http.ResponseWriter, r *http.Request) {
		list, err := svc.Store.Reminders(r.Context(), r.PathValue("sn"))
		if err != nil {
			fail(w, err)
			return
		}
		if list == nil {
			list = []Reminder{}
		}
		httpx.OK(w, map[string]any{"reminders": list})
	})

	// SKU 与下单归因
	mux.HandleFunc("GET /api/v1/sku", func(w http.ResponseWriter, r *http.Request) {
		list, err := svc.SKUs(r.Context(), r.URL.Query().Get("product_key"), r.URL.Query().Get("part"))
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"skus": list})
	})
	mux.HandleFunc("POST /api/v1/reminders/{id}/click", func(w http.ResponseWriter, r *http.Request) {
		uid, ok := userID(r)
		if !ok {
			httpx.Error(w, http.StatusUnauthorized, httpx.CodeDenied, "X-User-Id required")
			return
		}
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil || id <= 0 {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad reminder id")
			return
		}
		if err := svc.Click(r.Context(), id, uid); err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"status": "recorded"})
	})
	mux.HandleFunc("POST /internal/orders/attribute", func(w http.ResponseWriter, r *http.Request) {
		var req AttributeReq
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		if err := svc.Attribute(r.Context(), req); err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"status": "attributed"})
	})

	// 材料码：假码或可疑码 valid=false 且不带任何参数字段（FR-341）
	mux.HandleFunc("POST /api/v1/materials/verify", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Code string `json:"code"`
			SN   string `json:"sn"`
		}
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		if strings.TrimSpace(req.Code) == "" {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "code required")
			return
		}
		var uid *int64
		if id, ok := userID(r); ok {
			uid = &id
		}
		res, err := svc.Verify(r.Context(), req.Code, req.SN, uid)
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, res)
	})

	// 内部：立即跑一轮批计算（运维与集成测试用）
	mux.HandleFunc("POST /internal/health/run", func(w http.ResponseWriter, r *http.Request) {
		var only []string
		if sn := strings.TrimSpace(r.URL.Query().Get("sn")); sn != "" {
			only = strings.Split(sn, ",")
		}
		rep, err := svc.RunOnce(r.Context(), only...)
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, rep)
	})

	// 内部：提醒冷却对账（INC-3-09），只报不改数据
	mux.HandleFunc("POST /internal/health/reminders/reconcile", func(w http.ResponseWriter, r *http.Request) {
		if svc.ReminderRec == nil {
			httpx.Error(w, http.StatusServiceUnavailable, httpx.CodeInternal, "reminder reconciler not configured")
			return
		}
		rep, err := svc.ReminderRec.RunOnce(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, rep)
	})

	ipKey := func(r *http.Request) *string {
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			return nil
		}
		ip := httpx.ClientIP(r)
		return &ip
	}
	return httpx.Chain(mux, httpx.Recover, httpx.Logging, httpx.RateLimitByKey(ipKey, 50, 50))
}
