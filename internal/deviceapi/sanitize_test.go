package deviceapi

import "testing"

func TestSanitizeSN(t *testing.T) {
	cases := map[string]string{
		"XT-001.A":    "xt_001_a",
		"ABC123":      "abc123",
		"a b/c":       "a_b_c",
		"":            "",
		"已经_ok":       "___ok",
		"XT001":       "xt001",
		"x'; DROP --": "x___drop___",
	}
	for in, want := range cases {
		if got := SanitizeSN(in); got != want {
			t.Errorf("SanitizeSN(%q)=%q want %q", in, got, want)
		}
	}
}
