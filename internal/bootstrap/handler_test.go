package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/xtool/xtool-aiot/internal/pkg/cellmap"
)

type fakeRepo struct {
	mu      sync.Mutex
	devices map[string]Device
	cells   map[int]Cell
	inserts int
}

func (f *fakeRepo) GetDevice(_ context.Context, sn string) (*Device, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.devices[sn]
	if !ok {
		return nil, ErrNotFound
	}
	return &d, nil
}
func (f *fakeRepo) InsertDevice(_ context.Context, d Device) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inserts++
	if _, ok := f.devices[d.SN]; !ok {
		f.devices[d.SN] = d
	}
	return nil
}
func (f *fakeRepo) ListCells(context.Context) ([]Cell, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Cell
	for _, c := range f.cells {
		out = append(out, c)
	}
	return out, nil
}
func (f *fakeRepo) SetCellStatus(_ context.Context, id int, status string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.cells[id]
	if !ok {
		return false, nil
	}
	c.Status = status
	f.cells[id] = c
	return true, nil
}

func newTestServer(t *testing.T) (*Server, *fakeRepo) {
	t.Helper()
	repo := &fakeRepo{devices: map[string]Device{}, cells: map[int]Cell{
		1: {ID: 1, Region: "US", MQTTHost: "127.0.0.1", MQTTPort: 1884, Status: StatusActive},
		2: {ID: 2, Region: "US", MQTTHost: "127.0.0.1", MQTTPort: 1884, Status: StatusActive},
		3: {ID: 3, Region: "US", MQTTHost: "127.0.0.1", MQTTPort: 1884, Status: StatusStandby},
	}}
	cache := NewCellCache(repo, 0)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &Server{Devices: repo, Cells: cache, CellRepo: repo, NCells: 2}, repo
}

type resp struct {
	Code int             `json:"code"`
	Data json.RawMessage `json:"data"`
}

func do(t *testing.T, h http.Handler, method, path, body string) (int, resp) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "10.0.0.1:1234"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var r resp
	_ = json.Unmarshal(rec.Body.Bytes(), &r)
	return rec.Code, r
}

func TestBootstrapCreatesDeviceAndRoutes(t *testing.T) {
	s, repo := newTestServer(t)
	h := s.Handler("bootstrap-svc", "test")

	code, r := do(t, h, "GET", "/api/v1/bootstrap?sn=SIM00001", "")
	if code != 200 || r.Code != 0 {
		t.Fatalf("status %d code %d", code, r.Code)
	}
	var br Response
	_ = json.Unmarshal(r.Data, &br)
	if br.CellID != cellmap.CellOf("SIM00001", 2) || br.MQTTPort != 1884 || br.RetryAfter != 0 || br.CellMapVer != 1 {
		t.Fatalf("unexpected %+v", br)
	}
	d := repo.devices["SIM00001"]
	if d.Status != "activated" || d.ProductKey != "LM_S1" || d.Region != "US" {
		t.Fatalf("device row %+v", d)
	}
	// second call: no new insert
	do(t, h, "GET", "/api/v1/bootstrap?sn=SIM00001", "")
	if repo.inserts != 1 {
		t.Fatalf("inserts=%d", repo.inserts)
	}
	// custom pk
	do(t, h, "GET", "/api/v1/bootstrap?sn=SIM00002&pk=LM_P2", "")
	if repo.devices["SIM00002"].ProductKey != "LM_P2" {
		t.Fatal("pk not honoured")
	}
	// bad sn
	if code, _ := do(t, h, "GET", "/api/v1/bootstrap?sn=ab", ""); code != 400 {
		t.Fatalf("bad sn status %d", code)
	}
}

func TestOverloadAndDrainFlipStatus(t *testing.T) {
	s, _ := newTestServer(t)
	h := s.Handler("bootstrap-svc", "test")
	sn := "SIM00001"
	home := cellmap.CellOf(sn, 2)

	if code, _ := do(t, h, "POST", "/internal/cells/"+itoa(home)+"/overload", `{"overloaded":true}`); code != 200 {
		t.Fatalf("overload status %d", code)
	}
	_, r := do(t, h, "GET", "/api/v1/bootstrap?sn="+sn, "")
	var br Response
	_ = json.Unmarshal(r.Data, &br)
	if br.RetryAfter != RetryAfterSeconds || br.CellID != home {
		t.Fatalf("overloaded: %+v", br)
	}
	do(t, h, "POST", "/internal/cells/"+itoa(home)+"/overload", `{"overloaded":false}`)
	_, r = do(t, h, "GET", "/api/v1/bootstrap?sn="+sn, "")
	_ = json.Unmarshal(r.Data, &br)
	if br.RetryAfter != 0 {
		t.Fatalf("recovered: %+v", br)
	}
	do(t, h, "POST", "/internal/cells/"+itoa(home)+"/drain", "")
	_, r = do(t, h, "GET", "/api/v1/bootstrap?sn="+sn, "")
	_ = json.Unmarshal(r.Data, &br)
	if br.RetryAfter != RetryAfterSeconds || br.CellID != 3 {
		t.Fatalf("draining should redirect to standby 3: %+v", br)
	}
	if code, _ := do(t, h, "POST", "/internal/cells/99/drain", ""); code != 404 {
		t.Fatalf("unknown cell status %d", code)
	}
}

func TestRateLimitByIP(t *testing.T) {
	s, _ := newTestServer(t)
	h := s.Handler("bootstrap-svc", "test")
	limited := 0
	for i := 0; i < 80; i++ {
		if code, _ := do(t, h, "GET", "/api/v1/bootstrap?sn=SIM00001", ""); code == 429 {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("expected some 429s beyond burst 50")
	}
	if code, _ := do(t, h, "GET", "/healthz", ""); code != 200 {
		t.Fatal("healthz must not be rate limited")
	}
}

func itoa(i int) string { return string(rune('0' + i)) }
