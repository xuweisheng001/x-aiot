package cellmap

import "testing"

func TestCellOfRangeAndStable(t *testing.T) {
	seen := map[int]int{}
	for i := 0; i < 10000; i++ {
		sn := "XT" + string(rune('A'+i%26)) + string(rune('0'+i%10)) + string(rune('a'+i%7))
		c := CellOf(sn, 4)
		if c < 1 || c > 4 {
			t.Fatalf("cell out of range: %d", c)
		}
		if CellOf(sn, 4) != c {
			t.Fatal("not stable")
		}
		seen[c]++
	}
	for c := 1; c <= 4; c++ {
		if seen[c] == 0 {
			t.Fatalf("cell %d never chosen", c)
		}
	}
	if CellOf("anything", 1) != 1 {
		t.Fatal("n=1 must map to 1")
	}
}
