package pipeline

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

// Metrics 是进程内文本计数器集合（GET /metrics 输出 "name value" 行）。
type Metrics struct {
	mu sync.RWMutex
	c  map[string]*atomic.Int64
}

func NewMetrics(names ...string) *Metrics {
	m := &Metrics{c: map[string]*atomic.Int64{}}
	for _, n := range names {
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
func (m *Metrics) Add(name string, n int64) { m.counter(name).Add(n) }
func (m *Metrics) Set(name string, n int64) { m.counter(name).Store(n) }
func (m *Metrics) Get(name string) int64    { return m.counter(name).Load() }

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
