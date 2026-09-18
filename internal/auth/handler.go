package auth

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

type Server struct{ Store Store }

type result struct {
	Result string `json:"result"` // allow | deny
}

func (s *Server) Handler(name, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz(name, version))
	mux.HandleFunc("POST /auth", s.handleAuth)
	mux.HandleFunc("POST /acl", s.handleACL)
	return httpx.Chain(mux, httpx.Recover, httpx.Logging)
}

func (s *Server) handleAuth(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientID string `json:"clientid"`
		Username string `json:"username"`
		CertFP   string `json:"cert_fp"`
	}
	if err := httpx.Decode(r, &req); err != nil {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	ok, reason, err := Authenticate(ctx, s.Store, req.ClientID, req.CertFP)
	if err != nil {
		slog.Error("auth store", "clientid", req.ClientID, "err", err)
	}
	if !ok {
		slog.Info("auth deny", "clientid", req.ClientID, "reason", reason)
		httpx.JSON(w, http.StatusOK, result{"deny"})
		return
	}
	httpx.JSON(w, http.StatusOK, result{"allow"})
}

func (s *Server) handleACL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientID string `json:"clientid"`
		Topic    string `json:"topic"`
		Action   string `json:"action"`
	}
	if err := httpx.Decode(r, &req); err != nil {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
		return
	}
	if AllowTopic(req.ClientID, req.Topic, ParseAction(req.Action)) {
		httpx.JSON(w, http.StatusOK, result{"allow"})
		return
	}
	slog.Info("acl deny", "clientid", req.ClientID, "topic", req.Topic, "action", req.Action)
	httpx.JSON(w, http.StatusOK, result{"deny"})
}
