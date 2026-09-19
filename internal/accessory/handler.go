package accessory

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// Service 是 HTTP 层依赖的聚合：配对、留痕查询、滤芯查询、手动触发。
type Service struct {
	Store  Store
	RDB    *redis.Client
	Engine *Engine
	Filter *FilterBatch
	Act    Actuator
	M      *Metrics
	// Owner 是配对归属对账器（INC-2-06 / INC-2-12）；nil 时 /internal/reconcile/owners 返回 503。
	Owner *OwnerReconciler
}

// PairReq 是 POST /api/v1/pairings 的请求体。
type PairReq struct {
	HostSN         string `json:"host_sn"`
	AccSN          string `json:"acc_sn"`
	AccType        string `json:"acc_type"`
	LinkageEnabled *bool  `json:"linkage_enabled"`
	OffDelayS      *int   `json:"off_delay_s"`
}

// Pair 校验双方存在、配件机型为 ACC_*、owner 一致，然后配对；成功后向配件 desired 写 paired_sn（尽力）。
func (s *Service) Pair(ctx context.Context, r PairReq) (*Pairing, bool, error) {
	if !ValidSN(r.HostSN) || !ValidSN(r.AccSN) || r.HostSN == r.AccSN {
		return nil, false, ErrBadParam
	}
	if _, found, err := s.Store.DeviceProduct(ctx, r.HostSN); err != nil {
		return nil, false, err
	} else if !found {
		return nil, false, errors.New("host " + ErrNotFound.Error())
	}
	accPK, found, err := s.Store.DeviceProduct(ctx, r.AccSN)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, errors.New("accessory " + ErrNotFound.Error())
	}
	if !strings.HasPrefix(accPK, "ACC_") {
		return nil, false, errors.New("bad param: acc_sn is not an accessory product")
	}
	ho, err := s.Store.Owners(ctx, r.HostSN)
	if err != nil {
		return nil, false, err
	}
	ao, err := s.Store.Owners(ctx, r.AccSN)
	if err != nil {
		return nil, false, err
	}
	if !OwnersConsistent(ho, ao) {
		s.M.Inc("pairings_owner_mismatch")
		return nil, false, ErrOwnerMismatch
	}
	accType := r.AccType
	if accType == "" {
		accType = strings.ToLower(strings.TrimPrefix(accPK, "ACC_"))
	}
	enabled := true
	if r.LinkageEnabled != nil {
		enabled = *r.LinkageEnabled
	}
	off := int(DefaultOffDelay / time.Second)
	if r.OffDelayS != nil {
		off = *r.OffDelayS
	}
	p, created, err := s.Store.Pair(ctx, r.HostSN, r.AccSN, accType, enabled, off, NewPairKey())
	if err != nil {
		return nil, false, err
	}
	InvalidatePairings(ctx, s.RDB, r.HostSN)
	if created {
		s.M.Inc("pairings_created")
		if s.Act != nil {
			if err := s.Act.SetDesired(ctx, r.AccSN, map[string]any{"paired_sn": r.HostSN}); err != nil {
				// 尽力：配件下次上线仍会拉到 desired
				_ = err
			}
		}
	}
	return p, created, nil
}

// Unpair 解除并失效缓存。
func (s *Service) Unpair(ctx context.Context, id int64) (*Pairing, error) {
	p, err := s.Store.Unpair(ctx, id)
	if err != nil {
		return nil, err
	}
	InvalidatePairings(ctx, s.RDB, p.HostSN)
	s.M.Inc("pairings_removed")
	if s.Engine != nil {
		_, _ = s.Engine.CancelOff(ctx, p.AccSN)
	}
	return p, nil
}

// Update 改联动开关 / 延时。
func (s *Service) Update(ctx context.Context, id int64, enabled *bool, off *int) (*Pairing, error) {
	if off != nil && (*off < int(MinOffDelay/time.Second) || *off > int(MaxOffDelay/time.Second)) {
		return nil, errors.New("bad param: off_delay_s must be 60..600")
	}
	p, err := s.Store.UpdatePairing(ctx, id, enabled, off)
	if err != nil {
		return nil, err
	}
	InvalidatePairings(ctx, s.RDB, p.HostSN)
	return p, nil
}

// Routes 组装路由（技术方案 §11）。用户归属校验由 BFF 做，这里只认 SN。
func Routes(svc *Service, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz("accessory-svc", version))
	mux.Handle("GET /metrics", svc.M.Handler())

	mux.HandleFunc("POST /api/v1/pairings", func(w http.ResponseWriter, r *http.Request) {
		var req PairReq
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		p, created, err := svc.Pair(r.Context(), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		httpx.JSON(w, status, httpx.Resp{Code: httpx.CodeOK, Data: map[string]any{"pairing": p, "created": created}})
	})
	mux.HandleFunc("DELETE /api/v1/pairings/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad id")
			return
		}
		p, err := svc.Unpair(r.Context(), id)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, p)
	})
	mux.HandleFunc("PATCH /api/v1/pairings/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad id")
			return
		}
		var body struct {
			LinkageEnabled *bool `json:"linkage_enabled"`
			OffDelayS      *int  `json:"off_delay_s"`
		}
		if err := httpx.Decode(r, &body); err != nil || (body.LinkageEnabled == nil && body.OffDelayS == nil) {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "linkage_enabled or off_delay_s required")
			return
		}
		p, err := svc.Update(r.Context(), id, body.LinkageEnabled, body.OffDelayS)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, p)
	})
	mux.HandleFunc("GET /api/v1/pairings", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case q.Get("host_sn") != "":
			ps, err := svc.Store.PairingsByHost(r.Context(), q.Get("host_sn"))
			if err != nil {
				writeErr(w, err)
				return
			}
			if ps == nil {
				ps = []Pairing{}
			}
			httpx.OK(w, ps)
		case q.Get("acc_sn") != "":
			p, err := svc.Store.PairingByAcc(r.Context(), q.Get("acc_sn"))
			if err != nil {
				writeErr(w, err)
				return
			}
			httpx.OK(w, []Pairing{*p})
		default:
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "host_sn or acc_sn required")
		}
	})
	mux.HandleFunc("GET /api/v1/devices/{host_sn}/linkage-audit", func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 500 {
			limit = 50
		}
		rows, err := svc.Store.ListAudit(r.Context(), r.PathValue("host_sn"), limit)
		if err != nil {
			writeErr(w, err)
			return
		}
		if rows == nil {
			rows = []LinkageAudit{}
		}
		httpx.OK(w, rows)
	})
	mux.HandleFunc("GET /api/v1/alarms/{acc_sn}/context", func(w http.ResponseWriter, r *http.Request) {
		code := r.URL.Query().Get("code")
		ms, err := strconv.ParseInt(r.URL.Query().Get("event_ts"), 10, 64)
		if code == "" || err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "code and event_ts (ms) required")
			return
		}
		c, err := svc.Store.GetAlarmContext(r.Context(), r.PathValue("acc_sn"), code, time.UnixMilli(ms).UTC())
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, c)
	})
	mux.HandleFunc("GET /api/v1/devices/{acc_sn}/filter", func(w http.ResponseWriter, r *http.Request) {
		f, err := svc.Store.FilterLifeGet(r.Context(), r.PathValue("acc_sn"))
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, f)
	})
	mux.HandleFunc("POST /internal/filter/run-once", func(w http.ResponseWriter, r *http.Request) {
		if svc.Filter == nil {
			httpx.Error(w, http.StatusServiceUnavailable, httpx.CodeInternal, "filter batch not configured")
			return
		}
		d, h, err := svc.Filter.RunOnce(r.Context())
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, map[string]any{"devices": d, "hours": h})
	})
	mux.HandleFunc("POST /internal/reconcile/run-once", func(w http.ResponseWriter, r *http.Request) {
		if svc.Engine == nil {
			httpx.Error(w, http.StatusServiceUnavailable, httpx.CodeInternal, "engine not configured")
			return
		}
		n, err := svc.Engine.ReconcileOnce(r.Context())
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, map[string]any{"actions": n})
	})
	// 配对归属对账（INC-2-06 / INC-2-12）：越权配对停联动、不解绑。
	mux.HandleFunc("POST /internal/reconcile/owners", func(w http.ResponseWriter, r *http.Request) {
		if svc.Owner == nil {
			httpx.Error(w, http.StatusServiceUnavailable, httpx.CodeInternal, "owner reconciler not configured")
			return
		}
		rep, err := svc.Owner.RunOnce(r.Context())
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, rep)
	})
	return httpx.Chain(mux, httpx.Recover, httpx.Logging)
}

func writeErr(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case errors.Is(err, ErrNotFound) || strings.Contains(msg, ErrNotFound.Error()):
		httpx.Error(w, http.StatusNotFound, httpx.CodeNotFound, msg)
	case errors.Is(err, ErrOwnerMismatch):
		httpx.Error(w, http.StatusForbidden, httpx.CodeDenied, msg)
	case errors.Is(err, ErrConflict):
		httpx.Error(w, http.StatusConflict, httpx.CodeConflict, msg)
	case errors.Is(err, ErrBadParam) || strings.HasPrefix(msg, "bad param"):
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, msg)
	default:
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, msg)
	}
}
