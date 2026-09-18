package conngate

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
)

// Metrics 是文本计数器（GET /metrics）。
type Metrics struct {
	Accepted        atomic.Int64
	RejectedRate    atomic.Int64
	RejectedBreaker atomic.Int64
	Active          atomic.Int64
	UpstreamFail    atomic.Int64
	BreakerOpen     atomic.Int64 // 1 = 当前熔断中（快照）
}

func (m *Metrics) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w, "accepted %d\nrejected_rate %d\nrejected_breaker %d\nactive %d\nupstream_fail %d\nbreaker_open %d\n",
		m.Accepted.Load(), m.RejectedRate.Load(), m.RejectedBreaker.Load(), m.Active.Load(), m.UpstreamFail.Load(), m.BreakerOpen.Load())
	return int64(n), err
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = m.WriteTo(w)
}
