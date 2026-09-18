package backoff

import (
	"testing"
	"time"
)

func TestNextBoundsAndJitter(t *testing.T) {
	for n := 0; n < 40; n++ {
		d := Next(n, time.Second)
		exp := time.Second << uint(min(n, 20))
		if exp > MaxInterval {
			exp = MaxInterval
		}
		if d < exp || d > exp+MaxJitter {
			t.Fatalf("n=%d got %v, want [%v, %v]", n, d, exp, exp+MaxJitter)
		}
	}
	if Next(100, time.Second) > MaxInterval+MaxJitter {
		t.Fatal("cap broken")
	}
}
