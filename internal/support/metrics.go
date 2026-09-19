package support

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// Metrics 是文本计数器（GET /metrics，前缀 support_）。
type Metrics struct {
	mu sync.Mutex
	c  map[string]int64
}

// Metric names.
const (
	MBundles           = "bundles_generated"
	MBundleSourceFail  = "bundle_source_unavailable"
	MBundleDegraded    = "bundles_degraded"
	MGrantRequested    = "grants_requested"
	MGrantApproved     = "grants_approved"
	MGrantDenied       = "grants_denied"
	MGrantRevoked      = "grants_revoked"
	MGrantConflict     = "grant_transition_conflicts"
	MSelfCheckOK       = "selfcheck_dispatched"
	MSelfCheckRefused  = "selfcheck_refused"
	MSelfCheckUnknown  = "selfcheck_result_unknown"
	MAgentCalls        = "agent_calls"
	MAgentBundleReused = "agent_bundle_reused"
	MAgentRefused      = "agent_refused"
	MDictServed        = "dict_served"
	MDictUnknown       = "dict_unknown_code"
	MDictPublished     = "dict_published"
	MDefectRuns        = "defect_runs"
	MDefectCombos      = "defect_combos"
	MDefectAlerts      = "defect_alerts_created"
	MDefectCooldown    = "defect_cooldown_suppressed"
	MDefectFWBackfill  = "defect_fw_backfilled"
	MDefectErrors      = "defect_errors"
	MWarrantyGenerated = "warranty_generated"
)

func NewMetrics() *Metrics {
	m := &Metrics{c: map[string]int64{}}
	for _, k := range []string{MBundles, MBundleSourceFail, MBundleDegraded, MGrantRequested, MGrantApproved, MGrantDenied,
		MGrantRevoked, MGrantConflict, MSelfCheckOK, MSelfCheckRefused, MSelfCheckUnknown, MAgentCalls, MAgentBundleReused, MAgentRefused,
		MDictServed, MDictUnknown, MDictPublished, MDefectRuns, MDefectCombos, MDefectAlerts, MDefectCooldown,
		MDefectFWBackfill, MDefectErrors, MWarrantyGenerated} {
		m.c[k] = 0
	}
	return m
}

func (m *Metrics) Inc(k string) { m.Add(k, 1) }

func (m *Metrics) Add(k string, n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.c[k] += n
	m.mu.Unlock()
}

func (m *Metrics) Get(k string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.c[k]
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.mu.Lock()
		keys := make([]string, 0, len(m.c))
		for k := range m.c {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "support_%s %d\n", k, m.c[k])
		}
		m.mu.Unlock()
	})
}
