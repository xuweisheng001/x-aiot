package alarm

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// Routes 注册 §6 的三个接口 + healthz + metrics。
func Routes(svc *Service, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz("alarm-svc", version))
	mux.Handle("GET /metrics", svc.M.Handler())

	mux.HandleFunc("GET /api/v1/alarms", func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		list, err := svc.List(r.Context(), r.URL.Query().Get("status"), limit)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
			return
		}
		httpx.OK(w, list)
	})

	mux.HandleFunc("POST /api/v1/alarms/{id}/ack", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		if err := svc.Ack(r.Context(), id); err != nil {
			writeTransitionErr(w, err)
			return
		}
		httpx.OK(w, map[string]any{"id": id, "status": StatusAcked})
	})

	mux.HandleFunc("POST /api/v1/alarms/{id}/close", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		var body struct {
			ClosedBy string `json:"closed_by"`
		}
		if err := httpx.Decode(r, &body); err != nil || body.ClosedBy == "" {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "closed_by required")
			return
		}
		if err := svc.Close(r.Context(), id, body.ClosedBy); err != nil {
			writeTransitionErr(w, err)
			return
		}
		httpx.OK(w, map[string]any{"id": id, "status": StatusClosed})
	})

	return httpx.Chain(mux, httpx.Recover, httpx.Logging)
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad id")
		return 0, false
	}
	return id, true
}

func writeTransitionErr(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrIllegalTransition) {
		httpx.Error(w, http.StatusConflict, httpx.CodeConflict, "illegal state transition")
		return
	}
	httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
}
