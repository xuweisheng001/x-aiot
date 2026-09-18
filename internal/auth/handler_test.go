package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeStore struct {
	devices map[string]string    // sn -> status
	certs   map[string][2]string // fp -> {sn,status}
	fail    bool
}

func (f *fakeStore) DeviceStatus(_ context.Context, sn string) (string, bool, error) {
	if f.fail {
		return "", false, errors.New("pg down")
	}
	st, ok := f.devices[sn]
	return st, ok, nil
}
func (f *fakeStore) Cert(_ context.Context, fp string) (string, string, bool, error) {
	if f.fail {
		return "", "", false, errors.New("pg down")
	}
	c, ok := f.certs[fp]
	return c[0], c[1], ok, nil
}

func TestAuthenticate(t *testing.T) {
	st := &fakeStore{
		devices: map[string]string{"SIM00001": "activated", "SIM00002": "manufactured", "SIM00003": "activated"},
		certs:   map[string][2]string{"fp1": {"SIM00001", "active"}, "fp2": {"SIM00001", "revoked"}, "fp3": {"SIM00003", "active"}},
	}
	tests := []struct {
		name, client, fp string
		want             bool
	}{
		{"activated no cert", "SIM00001", "", true},
		{"activated with own active cert", "SIM00001", "fp1", true},
		{"revoked cert", "SIM00001", "fp2", false},
		{"cert of other device", "SIM00001", "fp3", false},
		{"unknown cert", "SIM00001", "nope", false},
		{"not activated", "SIM00002", "", false},
		{"unknown device", "SIM00009", "", false},
		{"bad sn format", "sim", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason, err := Authenticate(context.Background(), st, tc.client, tc.fp)
			if err != nil {
				t.Fatal(err)
			}
			if ok != tc.want {
				t.Fatalf("got %v (%s) want %v", ok, reason, tc.want)
			}
		})
	}
	ok, _, err := Authenticate(context.Background(), &fakeStore{fail: true}, "SIM00001", "")
	if ok || err == nil {
		t.Fatal("store error must fail closed with err")
	}
}

func TestHandlers(t *testing.T) {
	s := &Server{Store: &fakeStore{devices: map[string]string{"SIM00001": "activated"}}}
	h := s.Handler("auth-svc", "test")
	call := func(path, body string) string {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("POST", path, strings.NewReader(body)))
		if rec.Code != 200 {
			t.Fatalf("%s status %d", path, rec.Code)
		}
		var r result
		_ = json.Unmarshal(rec.Body.Bytes(), &r)
		return r.Result
	}
	if call("/auth", `{"clientid":"SIM00001","username":"SIM00001"}`) != "allow" {
		t.Fatal("expected allow")
	}
	if call("/auth", `{"clientid":"SIM00002"}`) != "deny" {
		t.Fatal("expected deny")
	}
	if call("/acl", `{"clientid":"SIM00001","topic":"up/LM_S1/SIM00001/telemetry","action":"publish"}`) != "allow" {
		t.Fatal("acl allow")
	}
	if call("/acl", `{"clientid":"SIM00001","topic":"down/SIM00002/#","action":"subscribe"}`) != "deny" {
		t.Fatal("acl deny")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/auth", strings.NewReader("{bad")))
	if rec.Code != 400 {
		t.Fatalf("bad json status %d", rec.Code)
	}
}
