package fleet

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/pkg/schedule"
)

// 请求头：BFF 透传的调用者身份与租户。授权只信这两个头 + PG 里的成员关系，不信任请求体。
const (
	HeaderUserID = "X-User-Id"
	HeaderOrgID  = "X-Org-Id"
)

type ctxKey int

const (
	ctxOrg ctxKey = iota
	ctxRole
	ctxUser
)

// OrgOf / RoleOf / UserOf 从请求上下文取租户中间件解析好的身份。
func OrgOf(ctx context.Context) int64 {
	v, _ := ctx.Value(ctxOrg).(int64)
	return v
}

func RoleOf(ctx context.Context) Role {
	v, _ := ctx.Value(ctxRole).(Role)
	return v
}

func UserOf(ctx context.Context) int64 {
	v, _ := ctx.Value(ctxUser).(int64)
	return v
}

// status 把业务错误翻译为 HTTP 状态与业务码。
func status(err error) (int, int) {
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrNotMember):
		return http.StatusNotFound, httpx.CodeNotFound
	case errors.Is(err, ErrBadParam):
		return http.StatusBadRequest, httpx.CodeBadParam
	case errors.Is(err, ErrDenied):
		return http.StatusForbidden, httpx.CodeDenied
	case errors.Is(err, ErrConflict):
		return http.StatusConflict, httpx.CodeConflict
	default:
		return http.StatusInternalServerError, httpx.CodeInternal
	}
}

func fail(w http.ResponseWriter, err error) {
	st, code := status(err)
	httpx.Error(w, st, code, err.Error())
}

func parseID(s string) (int64, bool) {
	id, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func userID(r *http.Request) (int64, bool) { return parseID(r.Header.Get(HeaderUserID)) }

// Routes 装配 fleet-svc 的路由（spec §6）。
// 租户路由统一挂在 /api/v1/orgs/{org}/ 子树上，先过租户中间件再由内层 mux 精确匹配。
func Routes(svc *Service, version string) http.Handler {
	tenant := http.NewServeMux()
	registerTenant(tenant, svc)
	guarded := svc.tenantMW(tenant)

	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", httpx.Healthz("fleet-svc", version))
	root.Handle("GET /metrics", svc.M.Handler())
	registerAlarmTargets(root, svc) // /internal/alarm-targets：alarm-svc 内部调用，不走租户中间件
	root.HandleFunc("POST /api/v1/orgs", func(w http.ResponseWriter, r *http.Request) {
		uid, ok := userID(r)
		if !ok {
			httpx.Error(w, http.StatusUnauthorized, httpx.CodeDenied, "X-User-Id required")
			return
		}
		var req struct {
			Name   string `json:"name"`
			Type   string `json:"type"`
			Region string `json:"region"`
		}
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		org, err := svc.CreateOrg(r.Context(), req.Name, req.Type, req.Region, uid)
		if err != nil {
			fail(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: org})
	})
	root.Handle("GET /api/v1/orgs/{org}", guarded)
	root.Handle("/api/v1/orgs/{org}/", guarded)

	ipKey := func(r *http.Request) *string {
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			return nil
		}
		ip := httpx.ClientIP(r)
		return &ip
	}
	return httpx.Chain(root, httpx.Recover, httpx.Logging, httpx.RateLimitByKey(ipKey, 50, 50))
}

// tenantMW 是租户中间件（路由组级）：解析 X-Org-Id / 路径 {org} 与 X-User-Id，查成员角色。
// 非成员、组织不存在、或头里的 org 与路径不一致 → 一律 404（不泄露存在性，§4.4 第二道锁）。
func (s *Service) tenantMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		notFound := func() {
			s.M.Inc(MAuthzNotFound)
			httpx.Error(w, http.StatusNotFound, httpx.CodeNotFound, "not found")
		}
		uid, ok := userID(r)
		if !ok {
			httpx.Error(w, http.StatusUnauthorized, httpx.CodeDenied, "X-User-Id required")
			return
		}
		orgID, ok := parseID(r.PathValue("org"))
		if !ok {
			notFound()
			return
		}
		if h := strings.TrimSpace(r.Header.Get(HeaderOrgID)); h != "" {
			hid, ok := parseID(h)
			if !ok || hid != orgID {
				notFound()
				return
			}
		}
		role, err := s.Store.GetRole(r.Context(), orgID, uid)
		if err != nil {
			if errors.Is(err, ErrNotMember) || errors.Is(err, ErrNotFound) {
				notFound()
				return
			}
			fail(w, err)
			return
		}
		ctx := context.WithValue(r.Context(), ctxOrg, orgID)
		ctx = context.WithValue(ctx, ctxRole, role)
		ctx = context.WithValue(ctx, ctxUser, uid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// guard 包住需要某动作权限的 handler：角色不足 → 403（组织存在性已由中间件确认）。
func (s *Service) guard(action Action, fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !Authorize(RoleOf(r.Context()), action) {
			s.M.Inc(MAuthzDenied)
			httpx.Error(w, http.StatusForbidden, httpx.CodeDenied, "role not allowed: "+string(action))
			return
		}
		fn(w, r)
	}
}

func registerTenant(mux *http.ServeMux, svc *Service) {
	registerOTA(mux, svc) // ---- 批量 OTA（ota.go） ----

	// ---- 组织 / 站点 / 成员 ----
	mux.HandleFunc("GET /api/v1/orgs/{org}", svc.guard(ActView, func(w http.ResponseWriter, r *http.Request) {
		org, err := svc.Store.GetOrg(r.Context(), OrgOf(r.Context()))
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"org": org, "role": RoleOf(r.Context())})
	}))
	mux.HandleFunc("GET /api/v1/orgs/{org}/members", svc.guard(ActView, func(w http.ResponseWriter, r *http.Request) {
		list, err := svc.Store.ListMembers(r.Context(), OrgOf(r.Context()))
		if err != nil {
			fail(w, err)
			return
		}
		if list == nil {
			list = []Member{}
		}
		httpx.OK(w, map[string]any{"members": list})
	}))
	mux.HandleFunc("POST /api/v1/orgs/{org}/members", svc.guard(ActManageMembers, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID int64  `json:"user_id"`
			Role   string `json:"role"`
		}
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		ctx := r.Context()
		if err := svc.AddMember(ctx, OrgOf(ctx), req.UserID, Role(req.Role), UserOf(ctx)); err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"status": "ok"})
	}))
	mux.HandleFunc("DELETE /api/v1/orgs/{org}/members/{uid}", svc.guard(ActManageMembers, func(w http.ResponseWriter, r *http.Request) {
		uid, ok := parseID(r.PathValue("uid"))
		if !ok {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad user id")
			return
		}
		if err := svc.Store.RemoveMember(r.Context(), OrgOf(r.Context()), uid); err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"status": "removed"})
	}))
	mux.HandleFunc("GET /api/v1/orgs/{org}/sites", svc.guard(ActView, func(w http.ResponseWriter, r *http.Request) {
		list, err := svc.Store.ListSites(r.Context(), OrgOf(r.Context()))
		if err != nil {
			fail(w, err)
			return
		}
		if list == nil {
			list = []Site{}
		}
		httpx.OK(w, map[string]any{"sites": list})
	}))
	mux.HandleFunc("POST /api/v1/orgs/{org}/sites", svc.guard(ActManageMembers, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name string `json:"name"`
			TZ   string `json:"tz"`
		}
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		site, err := svc.CreateSite(r.Context(), OrgOf(r.Context()), req.Name, req.TZ)
		if err != nil {
			fail(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: site})
	}))

	// ---- 设备归属与看板 ----
	mux.HandleFunc("GET /api/v1/orgs/{org}/devices", svc.guard(ActView, func(w http.ResponseWriter, r *http.Request) {
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if offset < 0 {
			offset = 0
		}
		if limit <= 0 || limit > 500 {
			limit = 100
		}
		list, total, err := svc.Store.ListDevices(r.Context(), OrgOf(r.Context()), offset, limit)
		if err != nil {
			fail(w, err)
			return
		}
		if list == nil {
			list = []DeviceOrg{}
		}
		httpx.OK(w, map[string]any{"devices": list, "total": total, "offset": offset, "limit": limit})
	}))
	mux.HandleFunc("POST /api/v1/orgs/{org}/devices", svc.guard(ActAttachDevice, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SN     string `json:"sn"`
			SiteID int64  `json:"site_id"`
		}
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		ctx := r.Context()
		if err := svc.Attach(ctx, OrgOf(ctx), UserOf(ctx), req.SN, req.SiteID); err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"sn": req.SN, "site_id": req.SiteID, "status": "attached"})
	}))
	mux.HandleFunc("DELETE /api/v1/orgs/{org}/devices/{sn}", svc.guard(ActAttachDevice, func(w http.ResponseWriter, r *http.Request) {
		if err := svc.Store.DetachDevice(r.Context(), OrgOf(r.Context()), r.PathValue("sn")); err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"status": "detached"})
	}))
	mux.HandleFunc("GET /api/v1/orgs/{org}/fleet/summary", svc.guard(ActView, func(w http.ResponseWriter, r *http.Request) {
		resp, err := svc.Summary(r.Context(), OrgOf(r.Context()))
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, resp)
	}))

	// ---- 课表与解锁 ----
	mux.HandleFunc("PUT /api/v1/orgs/{org}/sites/{site}/policy", svc.guard(ActEditSchedule, func(w http.ResponseWriter, r *http.Request) {
		siteID, ok := parseID(r.PathValue("site"))
		if !ok {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad site id")
			return
		}
		var p schedule.Policy
		if err := httpx.Decode(r, &p); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		ctx := r.Context()
		version, err := svc.PutPolicy(ctx, OrgOf(ctx), siteID, p, UserOf(ctx))
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"site_id": siteID, "version": version})
	}))
	mux.HandleFunc("GET /api/v1/orgs/{org}/sites/{site}/policy", svc.guard(ActView, func(w http.ResponseWriter, r *http.Request) {
		siteID, ok := parseID(r.PathValue("site"))
		if !ok {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad site id")
			return
		}
		pr, err := svc.Store.GetPolicy(r.Context(), OrgOf(r.Context()), siteID)
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"site_id": pr.SiteID, "version": pr.Version, "policy": pr.Policy})
	}))
	mux.HandleFunc("POST /api/v1/orgs/{org}/devices/{sn}/unlock", svc.guard(ActUnlock, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Minutes int `json:"minutes"`
		}
		_ = httpx.Decode(r, &req) // 空 body 合法：用默认 TTL
		ctx := r.Context()
		expires, err := svc.Unlock(ctx, OrgOf(ctx), UserOf(ctx), r.PathValue("sn"), req.Minutes)
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"sn": r.PathValue("sn"), "lock": false, "lock_expires_at": expires})
	}))

	// ---- 任务队列 ----
	mux.HandleFunc("GET /api/v1/orgs/{org}/queues", svc.guard(ActView, func(w http.ResponseWriter, r *http.Request) {
		list, err := svc.Store.ListQueues(r.Context(), OrgOf(r.Context()))
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"queues": list})
	}))
	mux.HandleFunc("POST /api/v1/orgs/{org}/queues", svc.guard(ActManageQueue, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SiteID    int64    `json:"site_id"`
			Name      string   `json:"name"`
			DeviceSNs []string `json:"device_sns"`
		}
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		ctx := r.Context()
		if strings.TrimSpace(req.Name) == "" {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "name required")
			return
		}
		if _, err := svc.Store.GetSite(ctx, OrgOf(ctx), req.SiteID); err != nil {
			fail(w, err)
			return
		}
		q, err := svc.Store.CreateQueue(ctx, OrgOf(ctx), req.SiteID, req.Name, req.DeviceSNs, UserOf(ctx))
		if err != nil {
			fail(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: q})
	}))
	mux.HandleFunc("POST /api/v1/orgs/{org}/queues/{q}/items", svc.guard(ActSubmit, func(w http.ResponseWriter, r *http.Request) {
		queueID, ok := parseID(r.PathValue("q"))
		if !ok {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad queue id")
			return
		}
		var req SubmitReq
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		ctx := r.Context()
		it, err := svc.Submit(ctx, OrgOf(ctx), queueID, UserOf(ctx), req)
		if err != nil {
			fail(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: it})
	}))
	mux.HandleFunc("GET /api/v1/orgs/{org}/queues/{q}/items", svc.guard(ActView, func(w http.ResponseWriter, r *http.Request) {
		queueID, ok := parseID(r.PathValue("q"))
		if !ok {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad queue id")
			return
		}
		ctx := r.Context()
		if _, err := svc.Store.GetQueue(ctx, OrgOf(ctx), queueID); err != nil {
			fail(w, err)
			return
		}
		items, err := svc.Store.ListItems(ctx, OrgOf(ctx), queueID, r.URL.Query().Get("status"))
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"items": items})
	}))
	mux.HandleFunc("POST /api/v1/orgs/{org}/items/{id}/approve", svc.guard(ActApprove, itemStatusHandler(svc, StApproved)))
	mux.HandleFunc("POST /api/v1/orgs/{org}/items/{id}/reject", svc.guard(ActApprove, itemStatusHandler(svc, StRejected)))
	mux.HandleFunc("POST /api/v1/orgs/{org}/items/{id}/cancel", svc.guard(ActSubmit, itemStatusHandler(svc, StCanceled)))

	// ---- 耗材 ----
	mux.HandleFunc("GET /api/v1/orgs/{org}/consumables", svc.guard(ActViewConsumabl, func(w http.ResponseWriter, r *http.Request) {
		resp, err := svc.Consumables(r.Context(), OrgOf(r.Context()))
		if err != nil {
			fail(w, err)
			return
		}
		if r.URL.Query().Get("format") == "csv" {
			w.Header().Set("Content-Type", "text/csv; charset=utf-8")
			w.Header().Set("Content-Disposition", `attachment; filename="consumables.csv"`)
			_, _ = w.Write([]byte(ConsumablesCSV(resp.Low)))
			return
		}
		httpx.OK(w, resp)
	}))
}

// itemStatusHandler 走同一条路径：model.Transition 校验 → 条件 UPDATE。
func itemStatusHandler(svc *Service, to string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Reason string `json:"reason"`
		}
		_ = httpx.Decode(r, &req)
		ctx := r.Context()
		it, err := svc.SetItemStatus(ctx, OrgOf(ctx), RoleOf(ctx), r.PathValue("id"), to, UserOf(ctx), req.Reason)
		if err != nil {
			fail(w, err)
			return
		}
		httpx.OK(w, map[string]any{"item_id": it.ItemID, "status": it.Status})
	}
}
