package param

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// Metrics 是文本计数器（GET /metrics，前缀 param_）。
type Metrics struct {
	mu sync.Mutex
	c  map[string]int64
}

// Metric names.
const (
	MPublished        = "releases_published"
	MRollbacks        = "rollbacks"
	MDiffRejected     = "diff_rejected"
	MDiffForced       = "diff_forced"
	MDeltaServed      = "delta_served"
	MFullServed       = "full_served"
	MNotModified      = "not_modified"
	MSnapshotServed   = "snapshot_served"
	MUserParamPut     = "user_param_put"
	MUserParamRefused = "user_param_conflicts"
	MCorrection       = "correction_served"
	MCorrectionMiss   = "correction_input_missing"
	MRecoPublished    = "recommendations_published"
)

func NewMetrics() *Metrics {
	m := &Metrics{c: map[string]int64{}}
	for _, k := range []string{MPublished, MRollbacks, MDiffRejected, MDiffForced, MDeltaServed, MFullServed, MNotModified,
		MSnapshotServed, MUserParamPut, MUserParamRefused, MCorrection, MCorrectionMiss, MRecoPublished} {
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
			fmt.Fprintf(w, "param_%s %d\n", k, m.c[k])
		}
	}
}
