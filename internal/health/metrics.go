package health

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// Metrics 是文本计数器（GET /metrics，前缀 health_）。
type Metrics struct {
	mu sync.Mutex
	c  map[string]int64
}

// Metric names（任务清单要求的 9 个 + 诊断用补充）。
const (
	MBatchRuns       = "batch_runs"
	MDevicesScored   = "devices_scored"
	MSkippedMissing  = "skipped_missing"
	MBatchFused      = "batch_fused"
	MRemindersSent   = "reminders_sent"
	MRemindersCooled = "reminders_cooled"
	MSkuHits         = "sku_hits"
	MVerifyOK        = "verify_ok"
	MVerifyBad       = "verify_bad"

	MBatchErrors       = "batch_errors"
	MHoursRegress      = "hours_regress"
	MHoursClipped      = "hours_clipped"
	MHealthNonmono     = "health_nonmonotonic"
	MSwaps             = "module_swaps"
	MRemindersDeferred = "reminders_deferred"
	MRemindersOptout   = "reminders_optout"
	MNotifyErr         = "notify_publish_err"
	MAttributed        = "orders_attributed"
	MAttributionReject = "orders_attribution_rejected"
	MClicks            = "reminder_clicks"
)

func NewMetrics() *Metrics {
	m := &Metrics{c: map[string]int64{}}
	for _, k := range []string{MBatchRuns, MDevicesScored, MSkippedMissing, MBatchFused, MRemindersSent, MRemindersCooled,
		MSkuHits, MVerifyOK, MVerifyBad, MBatchErrors, MHoursRegress, MHoursClipped, MHealthNonmono, MSwaps,
		MRemindersDeferred, MRemindersOptout, MNotifyErr, MAttributed, MAttributionReject, MClicks} {
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
			fmt.Fprintf(w, "health_%s %d\n", k, m.c[k])
		}
	}
}
