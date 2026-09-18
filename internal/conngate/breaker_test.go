package conngate

import (
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

func TestBreakerStateMachine(t *testing.T) {
	type step struct {
		op     string // fail | ok | tick
		d      time.Duration
		want   State
		wantOK bool
	}
	tests := []struct {
		name  string
		steps []step
	}{
		{"below threshold stays closed", []step{
			{"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true},
		}},
		{"threshold opens", []step{
			{"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true},
			{"fail", 0, Open, false},
		}},
		{"success resets counter", []step{
			{"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true},
			{"ok", 0, Closed, true},
			{"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true},
			{"fail", 0, Open, false},
		}},
		{"open expires after window and counter restarts", []step{
			{"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Closed, true}, {"fail", 0, Open, false},
			{"tick", 29 * time.Second, Open, false},
			{"fail", 0, Open, false}, // failures during open don't extend
			{"tick", time.Second, Closed, true},
			{"fail", 0, Closed, true}, // starts again from 1
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
			b := NewBreaker(5, 30*time.Second, clk.now)
			for i, s := range tc.steps {
				switch s.op {
				case "fail":
					b.Failure()
				case "ok":
					b.Success()
				case "tick":
					clk.advance(s.d)
				}
				if got := b.State(); got != s.want {
					t.Fatalf("step %d (%s): state %v want %v", i, s.op, got, s.want)
				}
				if got := b.Allow(); got != s.wantOK {
					t.Fatalf("step %d (%s): allow %v want %v", i, s.op, got, s.wantOK)
				}
			}
		})
	}
}

func TestBreakerTripsAndFailureCount(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := NewBreaker(2, time.Minute, clk.now)
	b.Failure()
	if b.Failures() != 1 || b.Trips() != 0 {
		t.Fatal("first failure")
	}
	b.Failure()
	if b.State() != Open || b.Trips() != 1 {
		t.Fatal("should trip")
	}
	clk.advance(time.Minute)
	if b.State() != Closed || b.Failures() != 0 {
		t.Fatal("should reset after window")
	}
	b.Failure()
	b.Failure()
	if b.Trips() != 2 {
		t.Fatal("second trip")
	}
}
