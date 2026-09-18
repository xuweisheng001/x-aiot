package conngate

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Proxy 是 accept → 令牌 → 熔断 → dial 上游 → 双向拷贝 的 TCP 代理。
type Proxy struct {
	Limiter     *rate.Limiter
	Breaker     *Breaker
	Dial        func(ctx context.Context) (net.Conn, error)
	DialTimeout time.Duration
	Metrics     *Metrics

	wg    sync.WaitGroup
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// NewProxy 构造默认代理：上游 upstream（host:port）。
func NewProxy(upstream string, rps float64, burst int, breaker *Breaker, m *Metrics) *Proxy {
	d := &net.Dialer{}
	return &Proxy{
		Limiter:     rate.NewLimiter(rate.Limit(rps), burst),
		Breaker:     breaker,
		Dial:        func(ctx context.Context) (net.Conn, error) { return d.DialContext(ctx, "tcp", upstream) },
		DialTimeout: 2 * time.Second,
		Metrics:     m,
		conns:       map[net.Conn]struct{}{},
	}
}

// Reject 在任何握手前发 RST：SetLinger(0) 让 Close 丢弃缓冲并直接 RST。
func Reject(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = conn.Close()
}

// Serve 阻塞 accept 直到 ctx 取消；退出时关闭监听与全部活动连接，等待 handler 结束（无 goroutine 泄漏）。
func (p *Proxy) Serve(ctx context.Context, ln net.Listener) error {
	if p.conns == nil {
		p.conns = map[net.Conn]struct{}{}
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	var retErr error
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				break
			}
			slog.Warn("accept", "err", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.Handle(ctx, conn)
		}()
	}
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
	return retErr
}

// Handle 处理单条已 accept 的连接（导出便于单测）。
func (p *Proxy) Handle(ctx context.Context, conn net.Conn) {
	if p.Metrics != nil && p.Breaker != nil {
		if p.Breaker.State() == Open {
			p.Metrics.BreakerOpen.Store(1)
		} else {
			p.Metrics.BreakerOpen.Store(0)
		}
	}
	if p.Limiter != nil && !p.Limiter.Allow() {
		p.Metrics.RejectedRate.Add(1)
		Reject(conn)
		return
	}
	if p.Breaker != nil && !p.Breaker.Allow() {
		p.Metrics.RejectedBreaker.Add(1)
		p.Metrics.BreakerOpen.Store(1)
		Reject(conn)
		return
	}
	dctx, cancel := context.WithTimeout(ctx, p.dialTimeout())
	up, err := p.Dial(dctx)
	cancel()
	if err != nil {
		p.Metrics.UpstreamFail.Add(1)
		if p.Breaker != nil {
			p.Breaker.Failure()
			if p.Breaker.State() == Open {
				p.Metrics.BreakerOpen.Store(1)
				slog.Warn("upstream breaker opened", "failures", p.Breaker.Failures(), "err", err)
			}
		}
		Reject(conn)
		return
	}
	if p.Breaker != nil {
		p.Breaker.Success()
	}
	p.Metrics.Accepted.Add(1)
	p.Metrics.Active.Add(1)
	defer p.Metrics.Active.Add(-1)
	p.track(conn, up, true)
	defer p.track(conn, up, false)
	pipe(conn, up)
}

func (p *Proxy) dialTimeout() time.Duration {
	if p.DialTimeout <= 0 {
		return 2 * time.Second
	}
	return p.DialTimeout
}

func (p *Proxy) track(a, b net.Conn, add bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conns == nil {
		p.conns = map[net.Conn]struct{}{}
	}
	if add {
		p.conns[a], p.conns[b] = struct{}{}, struct{}{}
	} else {
		delete(p.conns, a)
		delete(p.conns, b)
	}
}

// pipe 双向拷贝；任一方向结束即关闭双端并等待另一方向退出。
func pipe(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(a, b); done <- struct{}{} }()
	go func() { _, _ = io.Copy(b, a); done <- struct{}{} }()
	<-done
	_ = a.Close()
	_ = b.Close()
	<-done
}
