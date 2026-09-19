package fleet

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// Metrics 是文本计数器（GET /metrics，前缀 fleet_）。与 health/metrics.go 同写法。
type Metrics struct {
	mu sync.Mutex
	c  map[string]int64
}

// 指标名（任务清单 §9）。
const (
	MReconcileRuns    = "reconcile_runs"
	MLocksApplied     = "locks_applied"
	MUnlocks          = "unlocks"
	MDispatchAttempts = "dispatch_attempts"
	MDispatchClaimed  = "dispatch_claimed"
	MDispatchOK       = "dispatch_ok"
	MDispatchFailed   = "dispatch_failed"
	MItemsExpired     = "items_expired"
	MAuthzDenied      = "authz_denied"
	MAuthzNotFound    = "authz_notfound"
	// BL5 §08 / §09：批量 OTA 与告警目标
	MOTAWindowRejected  = "ota_window_rejected"
	MOTABatchesCreated  = "ota_batches_created"
	MAlarmTargetsServed = "alarm_targets_served"
)

var metricNames = []string{MReconcileRuns, MLocksApplied, MUnlocks, MDispatchAttempts, MDispatchClaimed,
	MDispatchOK, MDispatchFailed, MItemsExpired, MAuthzDenied, MAuthzNotFound,
	MOTAWindowRejected, MOTABatchesCreated, MAlarmTargetsServed}

func NewMetrics() *Metrics {
	m := &Metrics{c: map[string]int64{}}
	for _, k := range metricNames {
		m.c[k] = 0
	}
	return m
}

func (m *Metrics) Inc(name string) { m.Add(name, 1) }

func (m *Metrics) Add(name string, n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.c[name] += n
	m.mu.Unlock()
}

func (m *Metrics) Get(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.c[name]
}

func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.mu.Lock()
		defer m.mu.Unlock()
		keys := make([]string, 0, len(m.c))
		for k := range m.c {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "fleet_%s %d\n", k, m.c[k])
		}
	}
}
