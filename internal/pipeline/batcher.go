// Package pipeline 消费 JetStream iot.up.* 并写 TDengine / Redis 影子。
package pipeline

import "time"

// Batcher 是纯攒批策略：满 Size 或首条入批距今 ≥ Window 即应刷出。时钟可注入。
type Batcher[T any] struct {
	Size   int
	Window time.Duration
	now    func() time.Time

	items []T
	first time.Time
}

func NewBatcher[T any](size int, window time.Duration, now func() time.Time) *Batcher[T] {
	if size <= 0 {
		size = 500
	}
	if window <= 0 {
		window = 120 * time.Millisecond
	}
	if now == nil {
		now = time.Now
	}
	return &Batcher[T]{Size: size, Window: window, now: now, items: make([]T, 0, size)}
}

// Add 追加一条；首条记录入批时间。
func (b *Batcher[T]) Add(item T) {
	if len(b.items) == 0 {
		b.first = b.now()
	}
	b.items = append(b.items, item)
}

func (b *Batcher[T]) Len() int { return len(b.items) }

// ShouldFlush 满员或超窗。
func (b *Batcher[T]) ShouldFlush() bool {
	n := len(b.items)
	if n == 0 {
		return false
	}
	if n >= b.Size {
		return true
	}
	return b.now().Sub(b.first) >= b.Window
}

// TimeToFlush 返回距离窗口到期的剩余时间；空批返回 Window；已到期返回 0。
func (b *Batcher[T]) TimeToFlush() time.Duration {
	if len(b.items) == 0 {
		return b.Window
	}
	if len(b.items) >= b.Size {
		return 0
	}
	rem := b.Window - b.now().Sub(b.first)
	if rem < 0 {
		return 0
	}
	return rem
}

// Drain 取走当前批并清空。
func (b *Batcher[T]) Drain() []T {
	if len(b.items) == 0 {
		return nil
	}
	out := b.items
	b.items = make([]T, 0, b.Size)
	b.first = time.Time{}
	return out
}
