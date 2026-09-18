package pipeline

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func TestBatcherFlushBySize(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := NewBatcher[int](3, time.Second, clk.Now)
	if b.ShouldFlush() {
		t.Fatal("empty batch should not flush")
	}
	b.Add(1)
	b.Add(2)
	if b.ShouldFlush() {
		t.Fatal("2 < 3 should not flush")
	}
	b.Add(3)
	if !b.ShouldFlush() || b.TimeToFlush() != 0 {
		t.Fatal("size reached should flush")
	}
	got := b.Drain()
	if len(got) != 3 || b.Len() != 0 || b.ShouldFlush() {
		t.Fatalf("drain: %v len=%d", got, b.Len())
	}
	if b.Drain() != nil {
		t.Fatal("drain of empty should be nil")
	}
}

func TestBatcherFlushByWindow(t *testing.T) {
	clk := &fakeClock{t: time.Unix(100, 0)}
	b := NewBatcher[string](500, 120*time.Millisecond, clk.Now)
	if b.TimeToFlush() != 120*time.Millisecond {
		t.Fatalf("empty ttf=%v", b.TimeToFlush())
	}
	b.Add("a")
	clk.Advance(50 * time.Millisecond)
	b.Add("b") // 不重置首条时间
	if b.ShouldFlush() {
		t.Fatal("50ms < 120ms")
	}
	if ttf := b.TimeToFlush(); ttf != 70*time.Millisecond {
		t.Fatalf("ttf=%v want 70ms", ttf)
	}
	clk.Advance(70 * time.Millisecond)
	if !b.ShouldFlush() {
		t.Fatal("window elapsed should flush")
	}
	clk.Advance(time.Second)
	if b.TimeToFlush() != 0 {
		t.Fatal("overdue ttf should be 0")
	}
	if got := b.Drain(); len(got) != 2 || got[0] != "a" {
		t.Fatalf("drain=%v", got)
	}
	// 新一批重新计时
	b.Add("c")
	if b.ShouldFlush() {
		t.Fatal("fresh batch should not flush")
	}
}

func TestBatcherDefaults(t *testing.T) {
	b := NewBatcher[int](0, 0, nil)
	if b.Size != 500 || b.Window != 120*time.Millisecond {
		t.Fatalf("defaults size=%d window=%v", b.Size, b.Window)
	}
}
