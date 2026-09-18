package deviceapi

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

func TestDecideCmd(t *testing.T) {
	cases := []struct {
		action  string
		allowed bool
		status  int
		code    int
	}{
		{"pause", true, http.StatusOK, httpx.CodeOK},
		{"stop", true, http.StatusOK, httpx.CodeOK},
		{"self_check", true, http.StatusOK, httpx.CodeOK},
		{"remote_restart", false, http.StatusForbidden, httpx.CodeDenied},
		{"format_disk", false, http.StatusBadRequest, httpx.CodeBadParam},
		{"PAUSE", false, http.StatusBadRequest, httpx.CodeBadParam},
		{"", false, http.StatusBadRequest, httpx.CodeBadParam},
	}
	for _, c := range cases {
		d := DecideCmd(c.action)
		if d.Allowed != c.allowed || d.HTTPStatus != c.status || d.BizCode != c.code {
			t.Errorf("%q: got %+v", c.action, d)
		}
	}
	if DecideCmd("remote_restart").BizCode != 10003 || DecideCmd("x").BizCode != 10001 {
		t.Fatal("business codes must be 10003 / 10001")
	}
}

func TestNewUUIDv4(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		u := NewUUIDv4()
		if !re.MatchString(u) {
			t.Fatalf("bad uuid %q", u)
		}
		if seen[u] {
			t.Fatalf("duplicate uuid %q", u)
		}
		seen[u] = true
	}
}
