package ota

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

func Routes(svc *Service, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz("ota-svc", version))
	mux.Handle("GET /metrics", svc.M.Handler())

	mux.HandleFunc("POST /api/v1/firmwares", func(w http.ResponseWriter, r *http.Request) {
		var req FirmwareReq
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		id, err := svc.CreateFirmware(r.Context(), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: map[string]any{"id": id, "status": "released"}})
	})

	mux.HandleFunc("POST /api/v1/ota/batches", func(w http.ResponseWriter, r *http.Request) {
		var req BatchReq
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		b, err := svc.CreateBatch(r.Context(), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: b})
	})

	mux.HandleFunc("GET /api/v1/ota/batches/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		v, err := svc.GetBatchView(r.Context(), id)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, v)
	})

	mux.HandleFunc("POST /api/v1/ota/batches/{id}/pause", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		if err := svc.Pause(r.Context(), id); err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, map[string]any{"id": id, "status": BatchPaused})
	})

	mux.HandleFunc("POST /api/v1/ota/batches/{id}/resume", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		if err := svc.Resume(r.Context(), id); err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, map[string]any{"id": id, "status": BatchRunning})
	})

	mux.HandleFunc("POST /api/v1/ota/batches/{id}/advance", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		var body struct {
			CreatedBy  string `json:"created_by"`
			ApprovedBy string `json:"approved_by"`
		}
		if r.ContentLength != 0 {
			if err := httpx.Decode(r, &body); err != nil {
				httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
				return
			}
		}
		nb, err := svc.Advance(r.Context(), id, body.CreatedBy, body.ApprovedBy)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: nb})
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

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNeedsApproval):
		httpx.Error(w, http.StatusForbidden, httpx.CodeDenied, "full-stage requires dual approval")
	case errors.Is(err, ErrBadParam):
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, err.Error())
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, http.StatusNotFound, httpx.CodeNotFound, err.Error())
	case errors.Is(err, ErrConflict):
		httpx.Error(w, http.StatusConflict, httpx.CodeConflict, err.Error())
	default:
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
	}
}
