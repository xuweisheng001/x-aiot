package alarm

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Metrics 是进程内计数器 + 通知时延蓄水池（reservoir sampling，近似 p50/p99）。
type Metrics struct {
	mu       sync.Mutex
	counters map[string]int64
	res      *Reservoir
}

func NewMetrics() *Metrics {
	return &Metrics{counters: map[string]int64{}, res: NewReservoir(1024)}
}

func (m *Metrics) Inc(name string) { m.Add(name, 1) }

func (m *Metrics) Add(name string, n int64) {
	m.mu.Lock()
	m.counters[name] += n
	m.mu.Unlock()
}

func (m *Metrics) Get(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

// ObserveNotifyLatency 记录 event_ts→notified_at 的 SLO 时延。
func (m *Metrics) ObserveNotifyLatency(d time.Duration) {
	m.mu.Lock()
	m.res.Add(float64(d.Milliseconds()))
	m.mu.Unlock()
}

// Handler 输出 Prometheus 文本风格（无依赖，仅 gauge/counter 行）。
func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.mu.Lock()
		defer m.mu.Unlock()
		names := make([]string, 0, len(m.counters))
		for k := range m.counters {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			fmt.Fprintf(w, "alarm_%s %d\n", k, m.counters[k])
		}
		fmt.Fprintf(w, "alarm_notify_latency_ms_count %d\n", m.res.Count())
		for _, q := range []float64{0.5, 0.99} {
			fmt.Fprintf(w, "alarm_notify_latency_ms{quantile=\"%g\"} %g\n", q, m.res.Quantile(q))
		}
	}
}

// Reservoir 是 Algorithm R 定容蓄水池：内存有界，分位数是近似值。
type Reservoir struct {
	cap   int
	n     int64
	items []float64
}

func NewReservoir(capacity int) *Reservoir {
	if capacity <= 0 {
		capacity = 1
	}
	return &Reservoir{cap: capacity}
}

func (r *Reservoir) Add(v float64) {
	r.n++
	if len(r.items) < r.cap {
		r.items = append(r.items, v)
		return
	}
	if j := rand.Int64N(r.n); j < int64(r.cap) {
		r.items[j] = v
	}
}

func (r *Reservoir) Count() int64 { return r.n }

// Quantile 返回 q∈[0,1] 分位（最近秩法）；空池返回 0。
func (r *Reservoir) Quantile(q float64) float64 {
	if len(r.items) == 0 {
		return 0
	}
	s := make([]float64, len(r.items))
	copy(s, r.items)
	sort.Float64s(s)
	if q <= 0 {
		return s[0]
	}
	if q >= 1 {
		return s[len(s)-1]
	}
	idx := int(q*float64(len(s))+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}
