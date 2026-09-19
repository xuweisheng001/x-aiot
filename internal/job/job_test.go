package job

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

const goodUUID = "6631a519-de5f-4e51-972e-243165c7b370"

var goodHash = strings.Repeat("ab", 32)

func TestValidateJobFields(t *testing.T) {
	cases := []struct {
		name string
		f    JobFields
		ok   bool
	}{
		{"minimal", JobFields{JobID: goodUUID}, true},
		{"full", JobFields{JobID: goodUUID, MaterialID: "BASSWOOD_3MM", ParamProfileID: "pp-12", ParamsHash: goodHash}, true},
		{"uppercase uuid ok", JobFields{JobID: strings.ToUpper(goodUUID)}, true},
		{"uuid missing", JobFields{}, false},
		{"uuid 35 chars", JobFields{JobID: goodUUID[:35]}, false},
		{"uuid not hex", JobFields{JobID: "zzzzzzzz-de5f-4e51-972e-243165c7b370"}, false},
		{"material 32 chars", JobFields{JobID: goodUUID, MaterialID: strings.Repeat("A", 32)}, true},
		{"material 33 chars", JobFields{JobID: goodUUID, MaterialID: strings.Repeat("A", 33)}, false},
		{"material filename injection", JobFields{JobID: goodUUID, MaterialID: "客户订单 Logo.xcs"}, false},
		{"material path injection", JobFields{JobID: goodUUID, MaterialID: "../etc"}, false},
		{"profile sql injection", JobFields{JobID: goodUUID, ParamProfileID: "1; DROP TABLE"}, false},
		{"hash uppercase", JobFields{JobID: goodUUID, ParamsHash: strings.ToUpper(goodHash)}, false},
		{"hash 63", JobFields{JobID: goodUUID, ParamsHash: goodHash[:63]}, false},
		{"hash json blob", JobFields{JobID: goodUUID, ParamsHash: `{"power":80}`}, false},
		{"material code ok", JobFields{JobID: goodUUID, MaterialCodeID: strings.Repeat("A", 15) + strings.Repeat("7", 16)}, true},
		{"material code 30 chars", JobFields{JobID: goodUUID, MaterialCodeID: strings.Repeat("A", 30)}, false},
		{"material code lowercase", JobFields{JobID: goodUUID, MaterialCodeID: strings.Repeat("a", 31)}, false},
		{"material code digit 1 not in alphabet", JobFields{JobID: goodUUID, MaterialCodeID: strings.Repeat("1", 31)}, false},
	}
	for _, c := range cases {
		err := ValidateJobFields(c.f)
		if (err == nil) != c.ok {
			t.Errorf("%s: err=%v want ok=%v", c.name, err, c.ok)
		}
		if err != nil {
			if !errors.Is(err, ErrBadFields) {
				t.Errorf("%s: error should wrap ErrBadFields", c.name)
			}
			// 错误信息不得含字段原文
			for _, raw := range []string{c.f.MaterialID, c.f.ParamProfileID, c.f.ParamsHash, c.f.MaterialCodeID} {
				if raw != "" && strings.Contains(err.Error(), raw) {
					t.Errorf("%s: error leaks field value", c.name)
				}
			}
		}
	}
}

func TestDecideOptIn(t *testing.T) {
	cases := []struct {
		name string
		rep  map[string]string
		err  error
		want OptInState
	}{
		{"true", map[string]string{OptInField: "true"}, nil, OptInAllowed},
		{"1 (go-redis bool)", map[string]string{OptInField: "1"}, nil, OptInAllowed},
		{"TRUE", map[string]string{OptInField: "TRUE"}, nil, OptInAllowed},
		{"false", map[string]string{OptInField: "false"}, nil, OptInFalse},
		{"0", map[string]string{OptInField: "0"}, nil, OptInFalse},
		{"missing field", map[string]string{"work_state": "2"}, nil, OptInMissing},
		{"empty shadow", map[string]string{}, nil, OptInMissing},
		{"nil shadow", nil, nil, OptInMissing},
		{"garbage", map[string]string{OptInField: "maybe"}, nil, OptInMissing},
		{"redis error wins", map[string]string{OptInField: "true"}, errors.New("conn refused"), OptInErr},
	}
	for _, c := range cases {
		if got := DecideOptIn(c.rep, c.err); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
	if OptInAllowed.Metric() != "" || OptInErr.Metric() != "dropped_optin_err" {
		t.Error("metric names")
	}
}

func TestNeedPurge(t *testing.T) {
	cases := []struct {
		d    map[string]any
		want bool
	}{
		{map[string]any{OptInField: false}, true},
		{map[string]any{OptInField: "false"}, true},
		{map[string]any{OptInField: true}, false},
		{map[string]any{OptInField: "1"}, false},
		{map[string]any{}, false},
		{nil, false},
		{map[string]any{OptInField: 0.0}, false}, // 未知类型不触发删除
	}
	for i, c := range cases {
		if got := NeedPurge(c.d); got != c.want {
			t.Errorf("case %d: got %v want %v", i, got, c.want)
		}
	}
}

func TestCodes(t *testing.T) {
	for code, out := range map[string]string{CodeStart: "", CodeDone: OutcomeDone, CodeFail: OutcomeFail, CodePause: OutcomePause} {
		if !IsJobCode(code) || OutcomeFor(code) != out {
			t.Errorf("%s", code)
		}
	}
	if IsJobCode("FLAME_DETECTED") || IsJobCode("") {
		t.Error("non-job codes")
	}
}

// ---- fakes ----

type fakeStore struct {
	records  map[string]string // job_id → sn
	finished map[string]string // job_id → outcome
	feedback map[string]string
	pending  map[string][2]string
	optedOut []string
	purged   map[string]int64
	fail     error
}

func newFakeStore() *fakeStore {
	return &fakeStore{records: map[string]string{}, finished: map[string]string{}, feedback: map[string]string{},
		pending: map[string][2]string{}, purged: map[string]int64{}}
}

func (f *fakeStore) RecordStart(_ context.Context, jf JobFields, sn string, _ time.Time) (bool, error) {
	if f.fail != nil {
		return false, f.fail
	}
	if _, ok := f.records[jf.JobID]; ok {
		return false, nil
	}
	f.records[jf.JobID] = sn
	return true, nil
}
func (f *fakeStore) RecordFinish(_ context.Context, jf JobFields, sn string, _ time.Time, outcome string) (bool, error) {
	if f.fail != nil {
		return false, f.fail
	}
	_, existed := f.records[jf.JobID]
	f.records[jf.JobID] = sn
	f.finished[jf.JobID] = outcome
	return !existed, nil
}
func (f *fakeStore) MergePending(_ context.Context, jobID string) (bool, error) {
	p, ok := f.pending[jobID]
	if !ok {
		return false, nil
	}
	delete(f.pending, jobID)
	f.feedback[jobID] = p[1]
	return true, nil
}
func (f *fakeStore) RecordSN(_ context.Context, jobID string) (string, bool, error) {
	sn, ok := f.records[jobID]
	return sn, ok, nil
}
func (f *fakeStore) UpsertFeedback(_ context.Context, jobID, _, rating string) error {
	f.feedback[jobID] = rating
	return nil
}
func (f *fakeStore) InsertPending(_ context.Context, jobID, sn, rating string) error {
	f.pending[jobID] = [2]string{sn, rating}
	return nil
}
func (f *fakeStore) OptedOutSNs(context.Context) ([]string, error) { return f.optedOut, nil }
func (f *fakeStore) PurgeSN(_ context.Context, sn string) (int64, error) {
	var n int64
	for id, s := range f.records {
		if s == sn {
			delete(f.records, id)
			delete(f.feedback, id)
			n++
		}
	}
	f.purged[sn] = n
	return n, nil
}

type fakeShadow struct {
	m   map[string]map[string]string
	err error
}

func (s fakeShadow) Read(_ context.Context, sn string) (map[string]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.m[sn], nil
}

func env(sn, code string, f JobFields, ts int64) *envelope.Envelope {
	p := EventPayload{Seq: 1, Ts: ts, Code: code, JobFields: f}
	b, _ := json.Marshal(p)
	return &envelope.Envelope{PK: "LM_S1", SN: sn, Kind: envelope.KindEvent, Seq: 1, RecvTs: ts, Payload: b}
}

func TestHandleEvent(t *testing.T) {
	optin := fakeShadow{m: map[string]map[string]string{
		"SN_YES": {OptInField: "1"}, "SN_NO": {OptInField: "false"}, "SN_NONE": {"work_state": "2"},
	}}
	f := JobFields{JobID: goodUUID, MaterialID: "BASSWOOD_3MM", ParamsHash: goodHash}
	ts := time.Now().UnixMilli()

	t.Run("optin true: start then done, pending merged", func(t *testing.T) {
		st := newFakeStore()
		st.pending[goodUUID] = [2]string{"SN_YES", "good"}
		svc := NewService(st, optin, NewMetrics())
		out, err := svc.HandleEvent(context.Background(), env("SN_YES", CodeStart, f, ts))
		if err != nil || out != OutcomeStarted {
			t.Fatalf("start: %v %v", out, err)
		}
		if st.feedback[goodUUID] != "good" || svc.M.Get("pending_merged") != 1 {
			t.Fatal("pending not merged")
		}
		out, _ = svc.HandleEvent(context.Background(), env("SN_YES", CodeStart, f, ts))
		if out != OutcomeDupStart {
			t.Fatalf("dup start: %v", out)
		}
		out, err = svc.HandleEvent(context.Background(), env("SN_YES", CodeDone, f, ts+5000))
		if err != nil || out != OutcomeFinished || st.finished[goodUUID] != OutcomeDone {
			t.Fatalf("done: %v %v %v", out, err, st.finished)
		}
		if svc.M.Get("late_start") != 0 {
			t.Fatal("late_start should be 0")
		}
	})
	t.Run("done without start counts late_start", func(t *testing.T) {
		st := newFakeStore()
		svc := NewService(st, optin, NewMetrics())
		out, _ := svc.HandleEvent(context.Background(), env("SN_YES", CodeFail, f, ts))
		if out != OutcomeFinished || svc.M.Get("late_start") != 1 || st.finished[goodUUID] != OutcomeFail {
			t.Fatal("late finish")
		}
	})
	t.Run("optin false / missing / err all dropped before store", func(t *testing.T) {
		for _, c := range []struct {
			sn, metric string
			sh         ShadowReader
		}{
			{"SN_NO", "dropped_optin_false", optin},
			{"SN_NONE", "dropped_optin_missing", optin},
			{"SN_UNKNOWN", "dropped_optin_missing", optin},
			{"SN_YES", "dropped_optin_err", fakeShadow{err: errors.New("redis down")}},
		} {
			st := newFakeStore()
			svc := NewService(st, c.sh, NewMetrics())
			out, err := svc.HandleEvent(context.Background(), env(c.sn, CodeStart, f, ts))
			if err != nil || out != OutcomeDropped || len(st.records) != 0 || svc.M.Get(c.metric) != 1 {
				t.Errorf("%s: out=%v err=%v records=%d %s=%d", c.sn, out, err, len(st.records), c.metric, svc.M.Get(c.metric))
			}
		}
	})
	t.Run("bad fields dropped", func(t *testing.T) {
		st := newFakeStore()
		svc := NewService(st, optin, NewMetrics())
		bad := JobFields{JobID: goodUUID, MaterialID: "订单 Logo.xcs"}
		out, err := svc.HandleEvent(context.Background(), env("SN_YES", CodeStart, bad, ts))
		if err != nil || out != OutcomeDropped || svc.M.Get("dropped_bad_fields") != 1 || len(st.records) != 0 {
			t.Fatal("bad fields should be dropped")
		}
	})
	t.Run("non-job code ignored, bad payload", func(t *testing.T) {
		svc := NewService(newFakeStore(), optin, NewMetrics())
		out, _ := svc.HandleEvent(context.Background(), env("SN_YES", "FLAME_DETECTED", JobFields{}, ts))
		if out != OutcomeIgnored {
			t.Fatal("ignored")
		}
		out, _ = svc.HandleEvent(context.Background(), &envelope.Envelope{SN: "SN_YES", Kind: envelope.KindEvent, Payload: []byte(`{`)})
		if out != OutcomeBad {
			t.Fatal("bad payload")
		}
	})
	t.Run("store error → err (nak)", func(t *testing.T) {
		st := newFakeStore()
		st.fail = errors.New("pg down")
		svc := NewService(st, optin, NewMetrics())
		if _, err := svc.HandleEvent(context.Background(), env("SN_YES", CodeStart, f, ts)); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestFeedbackHandler(t *testing.T) {
	optin := fakeShadow{m: map[string]map[string]string{"SN_YES": {OptInField: "true"}, "SN_NO": {OptInField: "false"}}}
	st := newFakeStore()
	st.records[goodUUID] = "SN_YES"
	svc := NewService(st, optin, NewMetrics())
	h := Routes(svc, "test")

	post := func(jobID, sn, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/feedback", strings.NewReader(body))
		if sn != "" {
			req.Header.Set(HeaderDeviceSN, sn)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	other := "11111111-2222-4333-8444-555555555555"
	cases := []struct {
		name       string
		jobID, sn  string
		body       string
		wantStatus int
	}{
		{"stored", goodUUID, "SN_YES", `{"rating":"good"}`, 200},
		{"pending when record missing", other, "SN_YES", `{"rating":"burnt"}`, 202},
		{"bad rating", goodUUID, "SN_YES", `{"rating":"meh"}`, 400},
		{"missing sn header", goodUUID, "", `{"rating":"good"}`, 400},
		{"optin false", goodUUID, "SN_NO", `{"rating":"good"}`, 403},
		{"sn mismatch", goodUUID, "SN_OTHER_YES", `{"rating":"good"}`, 403},
		{"bad job id", "not-a-uuid", "SN_YES", `{"rating":"good"}`, 400},
	}
	optin.m["SN_OTHER_YES"] = map[string]string{OptInField: "true"}
	for _, c := range cases {
		rec := post(c.jobID, c.sn, c.body)
		if rec.Code != c.wantStatus {
			t.Errorf("%s: status %d want %d body=%s", c.name, rec.Code, c.wantStatus, rec.Body.String())
		}
	}
	if st.feedback[goodUUID] != "good" || st.pending[other][1] != "burnt" {
		t.Fatalf("store state: %v %v", st.feedback, st.pending)
	}
}

func TestPurgeOnce(t *testing.T) {
	st := newFakeStore()
	st.records["a"], st.records["b"], st.records["c"] = "SN_OUT", "SN_OUT", "SN_KEEP"
	st.optedOut = []string{"SN_OUT"}
	svc := NewService(st, fakeShadow{}, NewMetrics())
	n, err := svc.PurgeOnce(context.Background())
	if err != nil || n != 2 || len(st.records) != 1 || svc.M.Get("purged_rows") != 2 {
		t.Fatalf("purge: n=%d err=%v records=%v", n, err, st.records)
	}
}
