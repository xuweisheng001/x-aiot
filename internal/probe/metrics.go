package probe

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Metric names（/metrics 前缀 probe_）。
const (
	MRuns           = "runs"
	MOK             = "ok"
	MSlow           = "slow"
	MMissing        = "missing"
	MErrors         = "errors"
	MCleaned        = "cleaned"
	MCleanupErr     = "cleanup_err"
	MSquelchCleared = "squelch_cleared"
	MSquelchErr     = "squelch_err"
)

// Metrics 是文本计数器 + 端到端延迟分位。
type Metrics struct {
	mu  sync.Mutex
	c   map[string]int64
	lat []time.Duration // 保留最近 N 次，够算分位就行
}

// maxSamples 限制延迟样本内存占用；探针每 10 分钟一次，200 个样本约覆盖 33 小时。
const maxSamples = 200

func NewMetrics() *Metrics {
	m := &Metrics{c: map[string]int64{}}
	for _, k := range []string{MRuns, MOK, MSlow, MMissing, MErrors, MCleaned, MCleanupErr, MSquelchCleared, MSquelchErr} {
		m.c[k] = 0
	}
	return m
}

func (m *Metrics) Inc(name string) { m.Add(name, 1) }

func (m *Metrics) Add(name string, n int64) {
	m.mu.Lock()
	m.c[name] += n
	m.mu.Unlock()
}

func (m *Metrics) Get(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.c[name]
}

// Observe 记一次端到端延迟。
func (m *Metrics) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lat = append(m.lat, d)
	if len(m.lat) > maxSamples {
		m.lat = m.lat[len(m.lat)-maxSamples:]
	}
}

// Quantile 纯计算：返回最近样本的分位值（0..1）；无样本返回 0。
func (m *Metrics) Quantile(q float64) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return quantile(m.lat, q)
}

func quantile(xs []time.Duration, q float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), xs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	if q <= 0 {
		return s[0]
	}
	if q >= 1 {
		return s[len(s)-1]
	}
	i := int(q * float64(len(s)))
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}

func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.mu.Lock()
		defer m.mu.Unlock()
		keys := make([]string, 0, len(m.c))
		for k := range m.c {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "probe_%s %d\n", k, m.c[k])
		}
		fmt.Fprintf(w, "probe_latency_ms_count %d\n", len(m.lat))
		for _, q := range []float64{0.5, 0.99} {
			fmt.Fprintf(w, "probe_latency_ms{quantile=\"%g\"} %d\n", q, quantile(m.lat, q).Milliseconds())
		}
	}
}
