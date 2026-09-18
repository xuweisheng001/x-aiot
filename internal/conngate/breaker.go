// Package conngate 是连接许可网关：TCP 代理 + 令牌桶 + 上游熔断。
// 无令牌 / 熔断期间在任何 TLS/MQTT 握手前 SetLinger(0)+Close 发 RST，让上游只承接"有票"的连接。
package conngate

import (
	"sync"
	"time"
)

type State int

const (
	Closed State = iota // 正常放行
	Open                // 熔断中：一律拒绝，直到 openUntil
)

func (s State) String() string {
	if s == Open {
		return "open"
	}
	return "closed"
}

// Breaker 是纯状态机：连续失败 >= Threshold 打开 OpenFor；到期后回到 Closed 并清零计数。
// 时钟可注入，便于单测。
type Breaker struct {
	Threshold int
	OpenFor   time.Duration
	Now       func() time.Time

	mu        sync.Mutex
	failures  int
	openUntil time.Time
	trips     int64
}

func NewBreaker(threshold int, openFor time.Duration, now func() time.Time) *Breaker {
	if now == nil {
		now = time.Now
	}
	if threshold <= 0 {
		threshold = 1
	}
	return &Breaker{Threshold: threshold, OpenFor: openFor, Now: now}
}

// Allow 报告当前是否放行；熔断到期时顺带复位。
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stateLocked() == Closed
}

func (b *Breaker) stateLocked() State {
	if b.openUntil.IsZero() {
		return Closed
	}
	if b.Now().Before(b.openUntil) {
		return Open
	}
	// 到期：半开即闭合，从零重新计失败
	b.openUntil = time.Time{}
	b.failures = 0
	return Closed
}

// Failure 记一次上游失败；达到阈值即打开。
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stateLocked() == Open {
		return
	}
	b.failures++
	if b.failures >= b.Threshold {
		b.openUntil = b.Now().Add(b.OpenFor)
		b.trips++
	}
}

// Success 清零连续失败计数。
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stateLocked() == Closed {
		b.failures = 0
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stateLocked()
}

func (b *Breaker) Failures() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failures
}

// Trips 返回累计打开次数。
func (b *Breaker) Trips() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.trips
}
