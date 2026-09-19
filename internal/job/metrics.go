package job

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// Metrics 是进程内计数器，/metrics 以 Prometheus 文本风格输出，前缀 job_。
type Metrics struct {
	mu       sync.Mutex
	counters map[string]int64
}

// 预注册的计数名，保证 /metrics 输出稳定。
var MetricNames = []string{
	"consumed", "envelope_bad", "payload_bad", "ignored_code",
	"records_started", "records_finished", "late_start",
	"dropped_optin_false", "dropped_optin_missing", "dropped_optin_err", "dropped_bad_fields",
	"feedback_ok", "feedback_pending", "feedback_denied", "pending_merged",
	"purge_runs", "purged_rows", "db_errors",
}

func NewMetrics() *Metrics {
	m := &Metrics{counters: map[string]int64{}}
	for _, n := range MetricNames {
		m.counters[n] = 0
	}
	return m
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

func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.mu.Lock()
		defer m.mu.Unlock()
		names := make([]string, 0, len(m.counters))
		for k := range m.counters {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			fmt.Fprintf(w, "job_%s %d\n", k, m.counters[k])
		}
	}
}
