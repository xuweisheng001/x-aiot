package auth

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// AuthCache 记录最近一次成功认证的时间，供 PG 不可达时的 fail-open 兜底（INC-02）。
// key = clientID + "|" + certFP。并发安全；惰性清理：Recent 命中过期项时顺手删除，
// 另有 Sweep 供定时全量清理。
type AuthCache struct {
	mu sync.Mutex
	m  map[string]time.Time
}

func NewAuthCache() *AuthCache { return &AuthCache{m: map[string]time.Time{}} }

// CacheKey 统一 key 规则。
func CacheKey(clientID, certFP string) string { return clientID + "|" + certFP }

// Remember 记录 key 在 now 成功认证。
func (c *AuthCache) Remember(key string, now time.Time) {
	c.mu.Lock()
	c.m[key] = now
	c.mu.Unlock()
}

// Recent 报告 key 是否在 (now-ttl, now] 内成功认证过；过期项顺手删除。
func (c *AuthCache) Recent(key string, now time.Time, ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.m[key]
	if !ok {
		return false
	}
	if now.Sub(t) > ttl {
		delete(c.m, key)
		return false
	}
	return true
}

// Sweep 删除所有相对 now 过期的项，返回删除数。
func (c *AuthCache) Sweep(now time.Time, ttl time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, t := range c.m {
		if now.Sub(t) > ttl {
			delete(c.m, k)
			n++
		}
	}
	return n
}

// Len 返回缓存项数（指标用）。
func (c *AuthCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

// Metrics 是 auth-svc 的进程内文本计数器（GET /metrics）。
// 固定名：auth_allow auth_deny auth_failopen_allowed auth_store_error acl_allow acl_deny cache_size；
// deny 原因分布用 deny_reason_<slug>。
type Metrics struct {
	mu sync.RWMutex
	c  map[string]*atomic.Int64
}

const (
	MAuthAllow       = "auth_allow"
	MAuthDeny        = "auth_deny"
	MFailOpenAllowed = "auth_failopen_allowed"
	MStoreError      = "auth_store_error"
	MACLAllow        = "acl_allow"
	MACLDeny         = "acl_deny"
	MCacheSize       = "cache_size"
)

func NewMetrics() *Metrics {
	m := &Metrics{c: map[string]*atomic.Int64{}}
	for _, n := range []string{MAuthAllow, MAuthDeny, MFailOpenAllowed, MStoreError, MACLAllow, MACLDeny, MCacheSize} {
		m.c[n] = &atomic.Int64{}
	}
	return m
}

func (m *Metrics) counter(name string) *atomic.Int64 {
	m.mu.RLock()
	c, ok := m.c[name]
	m.mu.RUnlock()
	if ok {
		return c
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok = m.c[name]; !ok {
		c = &atomic.Int64{}
		m.c[name] = c
	}
	return c
}

func (m *Metrics) Inc(name string)          { m.counter(name).Add(1) }
func (m *Metrics) Set(name string, n int64) { m.counter(name).Store(n) }
func (m *Metrics) Get(name string) int64    { return m.counter(name).Load() }

// IncDenyReason 把 reason 归一成 deny_reason_<slug> 计数（空格→下划线，取前 3 个词，避免带 SN 等高基数值）。
func (m *Metrics) IncDenyReason(reason string) {
	m.Inc("deny_reason_" + reasonSlug(reason))
}

func reasonSlug(reason string) string {
	out := make([]byte, 0, len(reason))
	words := 0
	for i := 0; i < len(reason); i++ {
		ch := reason[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9':
			out = append(out, ch)
		case ch >= 'A' && ch <= 'Z':
			out = append(out, ch+32)
		case ch == ' ' || ch == '-' || ch == '_':
			if len(out) > 0 && out[len(out)-1] != '_' {
				words++
				if words >= 3 {
					return string(out)
				}
				out = append(out, '_')
			}
		}
	}
	if len(out) == 0 {
		return "unknown"
	}
	return string(out)
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		m.mu.RLock()
		names := make([]string, 0, len(m.c))
		for n := range m.c {
			names = append(names, n)
		}
		m.mu.RUnlock()
		sort.Strings(names)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		for _, n := range names {
			fmt.Fprintf(w, "%s %d\n", n, m.Get(n))
		}
	})
}
