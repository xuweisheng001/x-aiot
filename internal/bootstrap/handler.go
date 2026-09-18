package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/cellmap"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

const (
	DefaultProductKey = "LM_S1"
	DefaultRegion     = "US"
	DefaultCellMapVer = 1
)

// Server 装配 handler。
type Server struct {
	Devices  DeviceRepo
	Cells    *CellCache
	CellRepo CellRepo
	NCells   int
}

// Handler 返回带限流/日志/恢复中间件的路由。
func (s *Server) Handler(name, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz(name, version))
	mux.HandleFunc("GET /api/v1/bootstrap", s.handleBootstrap)
	mux.HandleFunc("POST /internal/cells/{id}/overload", s.handleOverload)
	mux.HandleFunc("POST /internal/cells/{id}/drain", s.handleDrain)
	ipKey := func(r *http.Request) *string {
		if r.URL.Path == "/healthz" {
			return nil
		}
		ip := httpx.ClientIP(r)
		return &ip
	}
	return httpx.Chain(mux, httpx.Recover, httpx.Logging, httpx.RateLimitByKey(ipKey, 50, 50))
}

func (s *Server) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	sn := r.URL.Query().Get("sn")
	if !ValidSN(sn) {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid sn")
		return
	}
	pk := r.URL.Query().Get("pk")
	if pk == "" {
		pk = DefaultProductKey
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	dev, err := s.lookupOrCreate(ctx, sn, pk)
	if err != nil {
		slog.Error("bootstrap device lookup", "sn", sn, "err", err)
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, "device lookup failed")
		return
	}
	resp, ok := Decide(s.Cells.Snapshot(), dev.CellID, dev.CellMapVer)
	if !ok {
		slog.Error("bootstrap cell missing", "sn", sn, "cell", dev.CellID)
		httpx.Error(w, http.StatusServiceUnavailable, httpx.CodeNotFound, "cell not found")
		return
	}
	if resp.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(resp.RetryAfter))
	}
	httpx.OK(w, resp)
}

func (s *Server) lookupOrCreate(ctx context.Context, sn, pk string) (*Device, error) {
	dev, err := s.Devices.GetDevice(ctx, sn)
	if err == nil {
		return dev, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	n := s.NCells
	if n <= 0 {
		n = 1
	}
	d := Device{SN: sn, ProductKey: pk, Region: DefaultRegion, CellID: cellmap.CellOf(sn, n),
		CellMapVer: DefaultCellMapVer, Status: "activated"}
	if err := s.Devices.InsertDevice(ctx, d); err != nil {
		return nil, err
	}
	// 并发首次接入可能被别的副本抢先插入；以库内为准。
	if got, err := s.Devices.GetDevice(ctx, sn); err == nil {
		return got, nil
	}
	return &d, nil
}

func (s *Server) handleOverload(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Overloaded bool `json:"overloaded"`
	}
	if err := httpx.Decode(r, &body); err != nil {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
		return
	}
	status := StatusActive
	if body.Overloaded {
		status = StatusOverloaded
	}
	s.setStatus(w, r, status)
}

func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	s.setStatus(w, r, StatusDraining)
}

func (s *Server) setStatus(w http.ResponseWriter, r *http.Request, status string) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || id <= 0 {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad cell id")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	found, err := s.CellRepo.SetCellStatus(ctx, id, status)
	if err != nil {
		slog.Error("set cell status", "cell", id, "status", status, "err", err)
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, "update failed")
		return
	}
	if !found {
		httpx.Error(w, http.StatusNotFound, httpx.CodeNotFound, "cell not found")
		return
	}
	s.Cells.SetStatus(id, status)
	slog.Info("cell status changed", "cell", id, "status", status)
	httpx.OK(w, map[string]any{"cell_id": id, "status": status})
}
