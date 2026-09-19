package accessory

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// Metrics 是进程内计数器，/metrics 以 Prometheus 文本风格输出，前缀 acc_。
type Metrics struct {
	mu       sync.Mutex
	counters map[string]int64
	latency  []int64 // 联动延迟样本 ms（环形，最多 1024）
}

// MetricNames 预注册，保证 /metrics 输出稳定。
var MetricNames = []string{
	"consumed", "envelope_bad", "payload_bad", "stale_dropped", "dedup_skipped", "no_pairing", "work_state_debounced",
	"linkage_triggered", "linkage_succeeded", "linkage_failed", "action_dedup",
	"off_scheduled", "off_cancelled", "off_sent", "off_suppressed", "off_requeued",
	"safety_vent", "host_stop_sent", "host_stop_failed", "host_stop_no_evidence",
	"context_written", "context_failed",
	"reconcile_runs", "reconcile_off_while_working", "reconcile_long_on",
	"filter_batch_runs", "filter_devices", "filter_hours", "filter_errors",
	"pairings_created", "pairings_removed", "pairings_owner_mismatch", "db_errors",
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

// ObserveLatency 记录一条联动延迟（事件设备时间 → 下发成功）。
func (m *Metrics) ObserveLatency(ms int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.latency) >= 1024 {
		m.latency = m.latency[1:]
	}
	m.latency = append(m.latency, ms)
}

func (m *Metrics) quantile(q float64) int64 {
	if len(m.latency) == 0 {
		return 0
	}
	s := append([]int64(nil), m.latency...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(float64(len(s)-1) * q)
	return s[i]
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
			fmt.Fprintf(w, "acc_%s %d\n", k, m.counters[k])
		}
		fmt.Fprintf(w, "acc_linkage_latency_ms_count %d\n", len(m.latency))
		fmt.Fprintf(w, "acc_linkage_latency_ms{quantile=\"0.5\"} %d\n", m.quantile(0.5))
		fmt.Fprintf(w, "acc_linkage_latency_ms{quantile=\"0.99\"} %d\n", m.quantile(0.99))
	}
}
