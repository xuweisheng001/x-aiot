// Package httpx 提供 JSON 响应、业务码、限流与通用中间件。
package httpx

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	CodeOK          = 0
	CodeBadParam    = 10001
	CodeDenied      = 10003
	CodeNotFound    = 10004
	CodeConflict    = 10009
	CodeNoQuota     = 11010
	CodeRateLimited = 100012
	CodeInternal    = 50000
)

type Resp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg,omitempty"`
	Data any    `json:"data,omitempty"`
}

func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func OK(w http.ResponseWriter, data any) { JSON(w, http.StatusOK, Resp{Code: CodeOK, Data: data}) }

func Error(w http.ResponseWriter, status, bizCode int, msg string) {
	JSON(w, status, Resp{Code: bizCode, Msg: msg})
}

func Decode(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	return dec.Decode(v)
}

type Middleware func(http.Handler) http.Handler

func Chain(h http.Handler, ms ...Middleware) http.Handler {
	for i := len(ms) - 1; i >= 0; i-- {
		h = ms[i](h)
	}
	return h
}

// RateLimitByKey 进程内令牌桶按 key 分桶。keyFn 返回 nil 表示不限。
// 超限返回 HTTP 429 + code 100012。多副本下会被放大 N 倍（已知缺陷，方案已标注）。
func RateLimitByKey(keyFn func(*http.Request) *string, rps float64, burst int) Middleware {
	var mu sync.Mutex
	type ent struct {
		l    *rate.Limiter
		seen time.Time
	}
	buckets := map[string]*ent{}
	go func() { // 低频清理
		for range time.Tick(time.Minute) {
			mu.Lock()
			for k, e := range buckets {
				if time.Since(e.seen) > 10*time.Minute {
					delete(buckets, k)
				}
			}
			mu.Unlock()
		}
	}()
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			k := keyFn(r)
			if k == nil {
				next.ServeHTTP(w, r)
				return
			}
			mu.Lock()
			e, ok := buckets[*k]
			if !ok {
				e = &ent{l: rate.NewLimiter(rate.Limit(rps), burst)}
				buckets[*k] = e
			}
			e.seen = time.Now()
			allow := e.l.Allow()
			mu.Unlock()
			if !allow {
				Error(w, http.StatusTooManyRequests, CodeRateLimited, "rate limited")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ClientIP 取真实来源 IP（原型信任 X-Forwarded-For 第一跳）。
func ClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic", "err", rec, "stack", string(debug.Stack()))
				Error(w, http.StatusInternalServerError, CodeInternal, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("http", "method", r.Method, "path", r.URL.Path, "ms", time.Since(start).Milliseconds())
	})
}

// Healthz 只返回 name/version，不检查任何依赖——避免依赖故障放大成滚动重启。
func Healthz(name, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		OK(w, map[string]string{"name": name, "version": version})
	}
}

// Serve 带优雅停机的 http server 启动辅助（阻塞直到 ctx 取消）。
func Serve(addr string, h http.Handler, stop <-chan struct{}) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case err := <-errc:
		return err
	case <-stop:
		return srv.Close()
	}
}
