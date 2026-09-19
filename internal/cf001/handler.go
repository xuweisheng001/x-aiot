package cf001

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// IPLimiter：/sign 按 IP 10 次/分钟（rps 10/60，burst 10）。第 11 次 429 code 100012。
func IPLimiter() httpx.Middleware {
	return httpx.RateLimitByKey(func(r *http.Request) *string {
		k := "ip:" + httpx.ClientIP(r)
		return &k
	}, 10.0/60.0, 10)
}

// Security 是「假」认证中间件：从 X-Device-Id 或 JSON body 的 sn 字段取设备身份，写回 Header，
// 缺失则 400。真实系统里这一步是证书/JWT 校验；限流必须放在它之后才能拿到 deviceId 维度。
func Security(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Device-Id")
		if id == "" && r.Body != nil {
			b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(b))
			var peek struct {
				SN string `json:"sn"`
			}
			_ = json.Unmarshal(b, &peek)
			id = peek.SN
		}
		if id == "" {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "device id required (X-Device-Id or sn)")
			return
		}
		r.Header.Set("X-Device-Id", id)
		next.ServeHTTP(w, r)
	})
}

// DeviceLimiter：deviceId+path，rps 1 burst 2 → 同设备 1 秒内第 3 次 429。
func DeviceLimiter() httpx.Middleware {
	return httpx.RateLimitByKey(func(r *http.Request) *string {
		id := r.Header.Get("X-Device-Id")
		if id == "" {
			return nil
		}
		k := "dev:" + id + "|" + r.URL.Path
		return &k
	}, 1, 2)
}

type signBody struct {
	OrderNo    string `json:"order_no"`
	Line       string `json:"line"`
	PayloadB64 string `json:"payload_b64"`
	UUID       string `json:"uuid"`
	MCUSN      string `json:"mcu_sn"`
	SOCSN      string `json:"soc_sn"`
	MAC        string `json:"mac"`
}

func Routes(svc *Service, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz("cf001-svc", version))
	if svc.C == nil {
		svc.C = &Counters{}
	}
	mux.Handle("GET /metrics", svc.C)

	mux.HandleFunc("GET /api/v1/oem/pubkey", func(w http.ResponseWriter, r *http.Request) {
		pemB, err := PublicKeyPEM(&svc.Key.PublicKey)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/x-pem-file")
		_, _ = w.Write(pemB)
	})

	mux.HandleFunc("POST /api/v1/oem/quotas", func(w http.ResponseWriter, r *http.Request) {
		var req QuotaReq
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		q, err := svc.CreateQuota(r.Context(), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: q})
	})
	mux.HandleFunc("POST /api/v1/oem/quotas/{order_no}/append", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := httpx.Decode(r, &body); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "bad json")
			return
		}
		q, err := svc.AppendQuota(r.Context(), r.PathValue("order_no"), body.Delta)
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, q)
	})
	mux.HandleFunc("GET /api/v1/oem/quotas/{order_no}", func(w http.ResponseWriter, r *http.Request) {
		q, err := svc.GetQuota(r.Context(), r.PathValue("order_no"))
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, q)
	})

	sign := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b signBody
		if err := httpx.Decode(r, &b); err != nil || b.OrderNo == "" || b.PayloadB64 == "" {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "order_no and payload_b64 required")
			return
		}
		res, err := svc.Sign(r.Context(), b.OrderNo, b.Line, b.PayloadB64,
			SignInput{UUID: b.UUID, MCUSN: b.MCUSN, SOCSN: b.SOCSN, MAC: b.MAC})
		if err != nil {
			writeErr(w, err)
			return
		}
		httpx.OK(w, res)
	})
	mux.Handle("POST /api/v1/oem/sign", IPLimiter()(sign))

	verify := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			SN           string `json:"sn"`
			Digest       string `json:"digest"`
			SignatureB64 string `json:"signature_b64"`
		}
		if err := httpx.Decode(r, &b); err != nil || b.SN == "" || b.Digest == "" || b.SignatureB64 == "" {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "sn, digest, signature_b64 required")
			return
		}
		res, err := svc.Verify(r.Context(), b.SN, b.Digest, b.SignatureB64)
		if err != nil {
			writeErr(w, err)
			return
		}
		if !res.OK {
			httpx.JSON(w, http.StatusBadRequest, httpx.Resp{Code: httpx.CodeBadParam, Msg: res.Msg, Data: res})
			return
		}
		httpx.OK(w, res)
	})
	// Security（拿到 deviceId）之后再按 deviceId+path 限流
	mux.Handle("POST /api/v1/oem/verify", httpx.Chain(verify, Security, DeviceLimiter()))

	return httpx.Chain(mux, httpx.Recover, httpx.Logging)
}

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNoQuota):
		httpx.Error(w, http.StatusConflict, httpx.CodeNoQuota, ErrNoQuota.Error())
	case errors.Is(err, ErrBadParam):
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, err.Error())
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, http.StatusNotFound, httpx.CodeNotFound, err.Error())
	case errors.Is(err, ErrConflict):
		httpx.Error(w, http.StatusConflict, httpx.CodeConflict, err.Error())
	case errors.Is(err, ErrUnavailable):
		httpx.Error(w, http.StatusServiceUnavailable, httpx.CodeInternal, err.Error())
	default:
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
	}
}
