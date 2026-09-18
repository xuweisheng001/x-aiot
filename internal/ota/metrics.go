package ota

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

type Metrics struct {
	mu sync.Mutex
	c  map[string]int64
}

func NewMetrics() *Metrics { return &Metrics{c: map[string]int64{}} }

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
			fmt.Fprintf(w, "ota_%s %d\n", k, m.c[k])
		}
	}
}
