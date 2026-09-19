package job

import (
	"errors"
	"net/http"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// HeaderDeviceSN 由 BFF 在校验 user ↔ sn 归属后注入；本服务只校验它与 job_record.sn 一致。
const HeaderDeviceSN = "X-Device-Sn"

// Routes 注册 healthz / metrics / 标记接口。
func Routes(svc *Service, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz("job-svc", version))
	mux.Handle("GET /metrics", svc.M.Handler())

	mux.HandleFunc("POST /api/v1/jobs/{job_id}/feedback", func(w http.ResponseWriter, r *http.Request) {
		sn := r.Header.Get(HeaderDeviceSN)
		if sn == "" {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, HeaderDeviceSN+" required")
			return
		}
		var body struct {
			Rating string `json:"rating"`
		}
		if err := httpx.Decode(r, &body); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		st, err := svc.Feedback(r.Context(), r.PathValue("job_id"), sn, body.Rating)
		switch {
		case err == nil && st == FeedbackPending:
			httpx.JSON(w, http.StatusAccepted, httpx.Resp{Code: httpx.CodeOK, Data: map[string]any{"status": "pending"}})
		case err == nil:
			httpx.OK(w, map[string]any{"status": "stored"})
		case errors.Is(err, ErrBadRating):
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, err.Error())
		case errors.Is(err, ErrOptInDenied), errors.Is(err, ErrSNMismatch):
			httpx.Error(w, http.StatusForbidden, httpx.CodeDenied, err.Error())
		default:
			httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
		}
	})

	return httpx.Chain(mux, httpx.Recover, httpx.Logging)
}
