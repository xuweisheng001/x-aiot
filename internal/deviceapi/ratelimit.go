package deviceapi

import (
	"context"
	"net/http"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

type ctxKey int

const deviceIDKey ctxKey = 1

// DeviceIDFrom 从 ctx 取 DeviceAuth 写入的设备标识。
func DeviceIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(deviceIDKey).(string)
	return v
}

// DeviceAuth 是"认证式"中间件：X-Device-Id 头优先，否则路径 {sn}；都没有 → 400/10001。
// 原型不校验签名，只负责把设备标识放进 ctx 供后续限流使用。
func DeviceAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Device-Id")
		if id == "" {
			id = r.PathValue("sn")
		}
		if id == "" {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "missing device id")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), deviceIDKey, id)))
	})
}

// DeviceRateKey 是 deviceId + 接口 维度的限流 key（DeviceAuth 之后使用）。
// 接口用路由模式（如 "POST /api/v1/devices/{sn}/cmd"）而非实际路径，保证同一设备同一接口共享一个桶。
func DeviceRateKey(r *http.Request) *string {
	id := DeviceIDFrom(r.Context())
	if id == "" {
		return nil
	}
	path := r.Pattern
	if path == "" {
		path = r.URL.Path
	}
	k := id + "|" + path
	return &k
}

// IPRateKey 按来源 IP 限流。
func IPRateKey(r *http.Request) *string {
	ip := httpx.ClientIP(r)
	return &ip
}

// DeviceRateLimit 组合：DeviceAuth → 设备维度令牌桶（默认 rps 1 burst 2：同设备第 3 次即 100012）。
func DeviceRateLimit(rps float64, burst int) httpx.Middleware {
	limit := httpx.RateLimitByKey(DeviceRateKey, rps, burst)
	return func(next http.Handler) http.Handler {
		return DeviceAuth(limit(next))
	}
}
