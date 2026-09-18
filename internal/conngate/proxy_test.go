package conngate

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func startEcho(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					if _, err := c.Write(buf[:n]); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln
}

func startProxy(t *testing.T, p *Proxy) (string, context.CancelFunc, chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Serve(ctx, ln); close(done) }()
	return ln.Addr().String(), cancel, done
}

// probe 模拟 loadgen 的 1 字节读探测：读错误 = RST，超时 = alive。
func probe(t *testing.T, addr string) (rst bool) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		// RST 可能早于 connect 返回：macOS 会把它报成 ECONNRESET —— 这本身就是「拒绝在 TLS 之前」的证据
		if strings.Contains(err.Error(), "connection reset") {
			return true
		}
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	time.Sleep(50 * time.Millisecond)
	_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err = c.Read(make([]byte, 1))
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return false
	}
	return true
}

func TestNoTokenRejectsWithRST(t *testing.T) {
	m := &Metrics{}
	p := NewProxy(startEcho(t).Addr().String(), 0, 0, NewBreaker(5, 30*time.Second, nil), m)
	p.Limiter = rate.NewLimiter(0, 0) // 永远没有令牌
	addr, cancel, done := startProxy(t, p)
	defer func() { cancel(); <-done }()

	if !probe(t, addr) {
		t.Fatal("expected RST when no token")
	}
	waitFor(t, func() bool { return m.RejectedRate.Load() == 1 && m.Accepted.Load() == 0 })
}

func TestBreakerOpensAfterUpstreamFailures(t *testing.T) {
	m := &Metrics{}
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := NewBreaker(3, 30*time.Second, clk.now)
	p := NewProxy("127.0.0.1:1", 1000, 1000, b, m)
	echo := startEcho(t)
	var upstreamHealthy atomic.Bool
	d := &net.Dialer{}
	p.Dial = func(ctx context.Context) (net.Conn, error) {
		if !upstreamHealthy.Load() {
			return nil, errors.New("upstream down")
		}
		return d.DialContext(ctx, "tcp", echo.Addr().String())
	}
	p.DialTimeout = 200 * time.Millisecond
	addr, cancel, done := startProxy(t, p)
	defer func() { cancel(); <-done }()

	for i := 0; i < 3; i++ {
		if !probe(t, addr) {
			t.Fatalf("attempt %d: expected RST from dial failure", i)
		}
	}
	waitFor(t, func() bool { return m.UpstreamFail.Load() == 3 })
	if b.State() != Open {
		t.Fatal("breaker should be open after 3 consecutive failures")
	}
	// 熔断期间：不再 dial，直接 RST
	if !probe(t, addr) {
		t.Fatal("expected RST during open breaker")
	}
	waitFor(t, func() bool { return m.RejectedBreaker.Load() == 1 && m.UpstreamFail.Load() == 3 })

	// 窗口过后恢复放行到上游（这次上游健康）
	upstreamHealthy.Store(true)
	clk.advance(31 * time.Second)
	if probe(t, addr) {
		t.Fatal("expected alive after breaker window")
	}
	waitFor(t, func() bool { return m.Accepted.Load() == 1 })
}

func TestProxyPassesTrafficAndTracksActive(t *testing.T) {
	m := &Metrics{}
	p := NewProxy(startEcho(t).Addr().String(), 100, 10, NewBreaker(5, 30*time.Second, nil), m)
	addr, cancel, done := startProxy(t, p)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io_ReadFull(c, buf); err != nil || string(buf) != "hello" {
		t.Fatalf("echo through proxy: %q %v", buf, err)
	}
	waitFor(t, func() bool { return m.Active.Load() == 1 && m.Accepted.Load() == 1 })
	c.Close()
	waitFor(t, func() bool { return m.Active.Load() == 0 })

	// 停机时活动连接被关闭且 Serve 返回（无泄漏）
	c2, _ := net.Dial("tcp", addr)
	defer c2.Close()
	waitFor(t, func() bool { return m.Active.Load() == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
	if m.Active.Load() != 0 {
		t.Fatal("active should drop to 0 on shutdown")
	}
}

func io_ReadFull(c net.Conn, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		k, err := c.Read(buf[n:])
		n += k
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
