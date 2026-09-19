package deviceapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

func init() { slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) }

// order 记录 audit / mqtt 发布的先后，用于断言"审计先于指令"。
type order struct {
	mu sync.Mutex
	ev []string
}

func (o *order) add(s string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.ev = append(o.ev, s)
	o.mu.Unlock()
}

type fakeMQTT struct {
	mu   sync.Mutex
	msgs []struct{ topic, payload string }
	err  error
	ord  *order
}

func (f *fakeMQTT) Publish(topic string, payload []byte) error {
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, struct{ topic, payload string }{topic, string(payload)})
	f.ord.add("mqtt")
	return nil
}

type fakeJS struct {
	mu   sync.Mutex
	subj []string
	data [][]byte
	err  error
	ord  *order
}

func (f *fakeJS) Publish(_ context.Context, subject string, payload []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subj = append(f.subj, subject)
	f.data = append(f.data, payload)
	f.ord.add("audit")
	return &jetstream.PubAck{}, nil
}

type fakeStore struct {
	desired map[string]json.RawMessage
	version map[string]int64
	audits  []AuditRecord
	acked   map[string]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{desired: map[string]json.RawMessage{}, version: map[string]int64{}, acked: map[string]string{}}
}
func (s *fakeStore) GetDesired(_ context.Context, sn string) (json.RawMessage, int64, error) {
	if d, ok := s.desired[sn]; ok {
		return d, s.version[sn], nil
	}
	return json.RawMessage(`{}`), 0, nil
}
func (s *fakeStore) MergeDesired(_ context.Context, sn string, patch json.RawMessage) (int64, json.RawMessage, error) {
	cur := map[string]json.RawMessage{}
	if d, ok := s.desired[sn]; ok {
		_ = json.Unmarshal(d, &cur)
	}
	var p map[string]json.RawMessage
	_ = json.Unmarshal(patch, &p)
	for k, v := range p {
		cur[k] = v
	}
	b, _ := json.Marshal(cur)
	s.desired[sn] = b
	s.version[sn]++
	return s.version[sn], b, nil
}
func (s *fakeStore) InsertAudit(_ context.Context, r AuditRecord) error {
	s.audits = append(s.audits, r)
	return nil
}
func (s *fakeStore) MarkAcked(_ context.Context, id, result string) error {
	s.acked[id] = result
	return nil
}
func (s *fakeStore) EnsureAuditPartitions(_ context.Context, monthsAhead int) (int, error) {
	return monthsAhead, nil
}

type fakeTD struct {
	sql string
	res *tdengine.Result
	err error
}

func (f *fakeTD) Query(_ context.Context, sql string) (*tdengine.Result, error) {
	f.sql = sql
	return f.res, f.err
}

func newTestServer() (*Server, *fakeMQTT, *fakeJS, *fakeStore, *fakeTD) {
	ord := &order{}
	mq, js, st, td := &fakeMQTT{ord: ord}, &fakeJS{ord: ord}, newFakeStore(), &fakeTD{res: &tdengine.Result{}}
	s := &Server{Store: st, MQTT: mq, JS: js, TD: td, Now: func() time.Time { return time.UnixMilli(1_726_300_000_000) }}
	return s, mq, js, st, td
}

func do(h http.Handler, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "10.0.0.1:1234"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) (code int, data map[string]any) {
	t.Helper()
	var resp struct {
		Code int            `json:"code"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad json: %s", rec.Body.String())
	}
	return resp.Code, resp.Data
}

func TestPostCmdDispatchAndAudit(t *testing.T) {
	s, mq, js, _, _ := newTestServer()
	h := s.Handler()
	rec := do(h, "POST", "/api/v1/devices/XT001/cmd", `{"action":"pause","params":{"reason":"x"},"operator":"alice"}`, nil)
	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	code, data := decode(t, rec)
	cmdID, _ := data["cmd_id"].(string)
	if code != 0 || len(cmdID) != 36 {
		t.Fatalf("code=%d data=%v", code, data)
	}
	if len(mq.msgs) != 1 || mq.msgs[0].topic != "down/XT001/cmd" || !strings.Contains(mq.msgs[0].payload, cmdID) || !strings.Contains(mq.msgs[0].payload, `"action":"pause"`) {
		t.Fatalf("mqtt=%+v", mq.msgs)
	}
	if len(js.subj) != 1 || js.subj[0] != envelope.SubjectCmdAudit {
		t.Fatalf("js=%v", js.subj)
	}
	var rec2 AuditRecord
	if err := json.Unmarshal(js.data[0], &rec2); err != nil || rec2.CmdID != cmdID || rec2.Operator != "alice" || rec2.Source != "console" || rec2.Result != "dispatched" || rec2.SN != "XT001" {
		t.Fatalf("audit=%+v err=%v", rec2, err)
	}
	// 审计先于指令
	if got := strings.Join(js.ord.ev, ","); got != "audit,mqtt" {
		t.Fatalf("publish order=%s want audit,mqtt", got)
	}
}

// 审计流不可用 → 503/100013，且 MQTT 未收到任何指令。
func TestPostCmdAuditUnavailableRefusesDispatch(t *testing.T) {
	s, mq, js, _, _ := newTestServer()
	js.err = errors.New("jetstream: no responders")
	rec := do(s.Handler(), "POST", "/api/v1/devices/XT001/cmd", `{"action":"pause"}`, nil)
	code, _ := decode(t, rec)
	if rec.Code != 503 || code != 100013 {
		t.Fatalf("status=%d code=%d body=%s", rec.Code, code, rec.Body.String())
	}
	if len(mq.msgs) != 0 {
		t.Fatalf("command must not be dispatched without audit: %+v", mq.msgs)
	}
}

func TestPostCmdWhitelist(t *testing.T) {
	s, mq, js, _, _ := newTestServer()
	h := s.Handler()
	cases := []struct {
		action string
		status int
		code   int
	}{
		{"remote_restart", 403, 10003},
		{"reboot", 400, 10001},
	}
	for i, c := range cases {
		// 不同设备避免触发设备限流
		rec := do(h, "POST", "/api/v1/devices/XT00"+string(rune('A'+i))+"/cmd", `{"action":"`+c.action+`"}`, nil)
		code, _ := decode(t, rec)
		if rec.Code != c.status || code != c.code {
			t.Errorf("%s: status=%d code=%d", c.action, rec.Code, code)
		}
	}
	if rec := do(h, "POST", "/api/v1/devices/XT00Z/cmd", `not json`, nil); rec.Code != 400 {
		t.Errorf("bad json status=%d", rec.Code)
	}
	if len(mq.msgs) != 0 || len(js.subj) != 0 {
		t.Fatal("rejected commands must not publish")
	}
}

func TestPostCmdMQTTFailure(t *testing.T) {
	s, mq, js, _, _ := newTestServer()
	mq.err = errors.New("broker down")
	rec := do(s.Handler(), "POST", "/api/v1/devices/XT001/cmd", `{"action":"stop"}`, nil)
	if rec.Code != 502 || len(js.subj) != 2 {
		t.Fatalf("status=%d js=%v", rec.Code, js.subj)
	}
	var first, second AuditRecord
	_ = json.Unmarshal(js.data[0], &first)
	_ = json.Unmarshal(js.data[1], &second)
	if first.Result != "dispatched" || second.Result != "dispatch_failed" || first.CmdID != second.CmdID {
		t.Fatalf("audits=%+v / %+v", first, second)
	}
}

// 同设备第 3 次即 100012（rps 1, burst 2）。
func TestDeviceRateLimitThirdCall(t *testing.T) {
	s, _, _, _, _ := newTestServer()
	h := s.Handler()
	for i := 1; i <= 2; i++ {
		if rec := do(h, "POST", "/api/v1/devices/XT001/cmd", `{"action":"pause"}`, nil); rec.Code != 200 {
			t.Fatalf("call %d status=%d", i, rec.Code)
		}
	}
	rec := do(h, "POST", "/api/v1/devices/XT001/cmd", `{"action":"pause"}`, nil)
	code, _ := decode(t, rec)
	if rec.Code != 429 || code != 100012 {
		t.Fatalf("3rd call status=%d code=%d", rec.Code, code)
	}
	// 其他设备不受影响（按设备分桶）
	if rec := do(h, "POST", "/api/v1/devices/XT002/cmd", `{"action":"pause"}`, nil); rec.Code != 200 {
		t.Fatalf("other device status=%d", rec.Code)
	}
	// X-Device-Id 头优先于路径
	hdr := map[string]string{"X-Device-Id": "XT001"}
	if rec := do(h, "POST", "/api/v1/devices/XT003/cmd", `{"action":"pause"}`, hdr); rec.Code != 429 {
		t.Fatalf("header device should share bucket: status=%d", rec.Code)
	}
}

func TestPatchDesired(t *testing.T) {
	s, mq, _, st, _ := newTestServer()
	h := s.Handler()
	rec := do(h, "PATCH", "/api/v1/devices/XT001/desired", `{"power_limit":80}`, nil)
	code, data := decode(t, rec)
	if rec.Code != 200 || code != 0 || data["version"] != float64(1) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	rec = do(h, "PATCH", "/api/v1/devices/XT001/desired", `{"fan":"auto"}`, nil)
	_, data = decode(t, rec)
	if data["version"] != float64(2) {
		t.Fatalf("version=%v", data["version"])
	}
	d := data["desired"].(map[string]any)
	if d["power_limit"] != float64(80) || d["fan"] != "auto" {
		t.Fatalf("merged desired=%v", d)
	}
	if len(mq.msgs) != 2 || mq.msgs[1].topic != "down/XT001/desired" || !strings.Contains(mq.msgs[1].payload, `"version":2`) {
		t.Fatalf("mqtt=%+v", mq.msgs)
	}
	if st.version["XT001"] != 2 {
		t.Fatal("store version")
	}
	if rec := do(h, "PATCH", "/api/v1/devices/XT001/desired", `{}`, nil); rec.Code != 400 {
		t.Fatalf("empty patch status=%d", rec.Code)
	}
	if rec := do(h, "PATCH", "/api/v1/devices/XT001/desired", `[1]`, nil); rec.Code != 400 {
		t.Fatalf("array patch status=%d", rec.Code)
	}
}

func TestGetTelemetry(t *testing.T) {
	s, _, _, _, td := newTestServer()
	h := s.Handler()
	td.res = &tdengine.Result{ColumnMeta: [][]any{{"ts"}, {"seq"}}, Data: [][]any{{"2024-01-01T00:00:00Z", float64(3)}}}
	rec := do(h, "GET", "/api/v1/devices/XT-001/telemetry?from=10&to=20&limit=5", "", nil)
	if rec.Code != 200 {
		t.Fatalf("status=%d %s", rec.Code, rec.Body.String())
	}
	if td.sql != "SELECT ts,seq,work_state,power_level,temp_cavity,temp_water,fan_rpm,laser_hours,progress FROM iot.t_xt_001 WHERE ts >= 10 AND ts <= 20 ORDER BY ts DESC LIMIT 5" {
		t.Fatalf("sql=%q", td.sql)
	}
	_, data := decode(t, rec)
	if rows := data["rows"].([]any); len(rows) != 1 || rows[0].(map[string]any)["seq"] != float64(3) {
		t.Fatalf("rows=%v", data["rows"])
	}
	if rec := do(h, "GET", "/api/v1/devices/XT-001/telemetry?from=x", "", nil); rec.Code != 400 {
		t.Fatalf("bad from status=%d", rec.Code)
	}
	td.err = errors.New("tdengine code 9826: Table does not exist")
	rec = do(h, "GET", "/api/v1/devices/NEW/telemetry", "", nil)
	_, data = decode(t, rec)
	if rec.Code != 200 || len(data["rows"].([]any)) != 0 {
		t.Fatalf("missing table should be empty: %d %s", rec.Code, rec.Body.String())
	}
	td.err = errors.New("tdengine http: connection refused")
	if rec := do(h, "GET", "/api/v1/devices/NEW/telemetry", "", nil); rec.Code != 502 {
		t.Fatalf("td down status=%d", rec.Code)
	}
}

func TestHealthz(t *testing.T) {
	s, _, _, _, _ := newTestServer()
	rec := do(s.Handler(), "GET", "/healthz", "", nil)
	_, data := decode(t, rec)
	if rec.Code != 200 || data["name"] != Name {
		t.Fatalf("healthz: %d %s", rec.Code, rec.Body.String())
	}
}
