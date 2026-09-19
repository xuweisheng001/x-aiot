package accessory

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

var t0 = time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

func pairing(id int64, host, acc string, enabled bool, off int) Pairing {
	return Pairing{ID: id, HostSN: host, AccSN: acc, AccType: "purifier", LinkageEnabled: enabled, OffDelayS: off, PairedAt: t0}
}

func baseState() EngineState {
	return EngineState{Now: t0, AllowOff: true, StopHostOnFire: true, DefaultLevel: DefaultFanLevel,
		PrevWorkState: map[string]int{}, LastJobStart: map[string]time.Time{}, SafetyVentUntil: map[string]time.Time{},
		Current: map[string]DesiredState{}, HostWorkState: map[string]int{},
		Rules: map[string]int{"ACRYLIC_3MM": 3, "BASSWOOD_3MM": 2}}
}

func types(as []Action) []ActionType {
	out := make([]ActionType, 0, len(as))
	for _, a := range as {
		out = append(out, a.Type)
	}
	return out
}

func eqTypes(a, b []ActionType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------- 规则纯函数 ----------

func TestLevelFor(t *testing.T) {
	rules := map[string]int{"A": 3, "BAD": 9}
	cases := []struct {
		mat  string
		def  int
		want int
	}{
		{"A", 2, 3}, {"missing", 2, 2}, {"BAD", 2, 2}, {"missing", 0, DefaultFanLevel}, {"missing", 7, DefaultFanLevel},
	}
	for _, c := range cases {
		if got := LevelFor(rules, c.mat, c.def); got != c.want {
			t.Errorf("LevelFor(%q,%d)=%d want %d", c.mat, c.def, got, c.want)
		}
	}
}

func TestIsStaleAndClamp(t *testing.T) {
	if IsStale(0, t0, MaxEventAge) {
		t.Fatal("no recv_ts must not be stale")
	}
	if IsStale(t0.Add(-30*time.Second).UnixMilli(), t0, MaxEventAge) {
		t.Fatal("30s old is fresh")
	}
	if !IsStale(t0.Add(-61*time.Second).UnixMilli(), t0, MaxEventAge) {
		t.Fatal("61s old is stale")
	}
	for sec, want := range map[int]time.Duration{10: MinOffDelay, 60: 60 * time.Second, 180: 180 * time.Second, 600: MaxOffDelay, 9999: MaxOffDelay} {
		if got := ClampOffDelay(sec); got != want {
			t.Errorf("ClampOffDelay(%d)=%s want %s", sec, got, want)
		}
	}
	if ShouldCancelOff(time.Time{}, t0) {
		t.Fatal("no job start → keep off")
	}
	if !ShouldCancelOff(t0.Add(time.Second), t0) || ShouldCancelOff(t0.Add(-time.Second), t0) {
		t.Fatal("ShouldCancelOff ordering")
	}
	if !HostStopEvidence("FIRE_SUPPRESSED", WorkStateUnknown) || !HostStopEvidence("FIRE_SUPPRESSED", 2) || HostStopEvidence("FIRE_SUPPRESSED", 0) || HostStopEvidence("SMOKE_HIGH", 2) {
		t.Fatal("HostStopEvidence")
	}
	if !IsHostSafetyCode("FLAME_DETECTED") || IsHostSafetyCode("FIRE_SUPPRESSED") || IsHostSafetyCode("JOB_START") {
		t.Fatal("IsHostSafetyCode")
	}
	if !OwnersConsistent(nil, []int64{1}) || !OwnersConsistent([]int64{1, 2}, []int64{2}) || OwnersConsistent([]int64{1}, []int64{2}) {
		t.Fatal("OwnersConsistent")
	}
}

func TestDecide(t *testing.T) {
	host := "HOST0001"
	p1 := pairing(1, host, "ACC00001", true, 180)
	p2 := pairing(2, host, "ACC00002", true, 300)
	pDis := pairing(3, host, "ACC00003", false, 180)
	recv := t0.UnixMilli()

	cases := []struct {
		name     string
		evt      Event
		pairings []Pairing
		mut      func(*EngineState)
		want     []ActionType
		check    func(*testing.T, []Action)
	}{
		{name: "no pairing", evt: Event{SN: host, Code: "JOB_START", RecvTs: recv}, pairings: nil, want: nil},
		{name: "stale", evt: Event{SN: host, Code: "JOB_START", RecvTs: t0.Add(-2 * time.Minute).UnixMilli()}, pairings: []Pairing{p1}, want: nil},
		{name: "job start material level", evt: Event{SN: host, Code: "JOB_START", MaterialID: "ACRYLIC_3MM", RecvTs: recv}, pairings: []Pairing{p1},
			want: []ActionType{ActCancelOff, ActSetDesired},
			check: func(t *testing.T, as []Action) {
				if as[1].Desired != (DesiredState{PowerOn: true, FanLevel: 3}) {
					t.Fatalf("desired %+v", as[1].Desired)
				}
			}},
		{name: "job start unknown material default", evt: Event{SN: host, Code: "JOB_START", MaterialID: "X", RecvTs: recv}, pairings: []Pairing{p1},
			want: []ActionType{ActCancelOff, ActSetDesired},
			check: func(t *testing.T, as []Action) {
				if as[1].Desired.FanLevel != DefaultFanLevel {
					t.Fatalf("level %d", as[1].Desired.FanLevel)
				}
			}},
		{name: "job start idempotent when current equal", evt: Event{SN: host, Code: "JOB_START", MaterialID: "ACRYLIC_3MM", RecvTs: recv}, pairings: []Pairing{p1},
			mut:  func(st *EngineState) { st.Current["ACC00001"] = DesiredState{PowerOn: true, FanLevel: 3} },
			want: []ActionType{ActCancelOff}},
		{name: "job start skips disabled pairing", evt: Event{SN: host, Code: "JOB_START", RecvTs: recv}, pairings: []Pairing{p1, pDis},
			want: []ActionType{ActCancelOff, ActSetDesired}},
		{name: "job done schedules off with clamped delay", evt: Event{SN: host, Code: "JOB_DONE", RecvTs: recv}, pairings: []Pairing{p1, p2, pDis},
			want: []ActionType{ActScheduleOff, ActScheduleOff},
			check: func(t *testing.T, as []Action) {
				if as[0].Delay != 180*time.Second || as[1].Delay != 300*time.Second {
					t.Fatalf("delays %s %s", as[0].Delay, as[1].Delay)
				}
			}},
		{name: "job fail also schedules off", evt: Event{SN: host, Code: "JOB_FAIL", RecvTs: recv}, pairings: []Pairing{p1}, want: []ActionType{ActScheduleOff}},
		{name: "allow off false → no schedule", evt: Event{SN: host, Code: "JOB_DONE", RecvTs: recv}, pairings: []Pairing{p1},
			mut: func(st *EngineState) { st.AllowOff = false }, want: nil},
		{name: "safety vent window extends off delay", evt: Event{SN: host, Code: "JOB_DONE", RecvTs: recv}, pairings: []Pairing{p1},
			mut:  func(st *EngineState) { st.SafetyVentUntil["ACC00001"] = t0.Add(9 * time.Minute) },
			want: []ActionType{ActScheduleOff},
			check: func(t *testing.T, as []Action) {
				if as[0].Delay != 9*time.Minute {
					t.Fatalf("delay %s", as[0].Delay)
				}
			}},
		{name: "telemetry unknown prev → no action", evt: Event{SN: host, IsTelemetry: true, WorkState: 2, RecvTs: recv}, pairings: []Pairing{p1}, want: nil},
		{name: "telemetry same state debounced", evt: Event{SN: host, IsTelemetry: true, WorkState: 2, RecvTs: recv}, pairings: []Pairing{p1},
			mut: func(st *EngineState) { st.PrevWorkState[host] = 2 }, want: nil},
		{name: "telemetry 0→2 starts", evt: Event{SN: host, IsTelemetry: true, WorkState: 2, RecvTs: recv}, pairings: []Pairing{p1},
			mut: func(st *EngineState) { st.PrevWorkState[host] = 0 }, want: []ActionType{ActCancelOff, ActSetDesired}},
		{name: "telemetry 0→2 right after JOB_START debounced", evt: Event{SN: host, IsTelemetry: true, WorkState: 2, RecvTs: recv}, pairings: []Pairing{p1},
			mut: func(st *EngineState) { st.PrevWorkState[host] = 0; st.LastJobStart[host] = t0.Add(-3 * time.Second) }, want: nil},
		{name: "telemetry 2→0 ends", evt: Event{SN: host, IsTelemetry: true, WorkState: 0, RecvTs: recv}, pairings: []Pairing{p1},
			mut: func(st *EngineState) { st.PrevWorkState[host] = 2 }, want: []ActionType{ActScheduleOff}},
		{name: "host safety code vents all incl disabled", evt: Event{SN: host, Code: "FLAME_DETECTED", RecvTs: recv}, pairings: []Pairing{p1, pDis},
			want: []ActionType{ActCancelOff, ActSafetyVent, ActSetDesired, ActCancelOff, ActSafetyVent, ActSetDesired},
			check: func(t *testing.T, as []Action) {
				if as[2].Desired.FanLevel != MaxFanLevel || as[2].Trigger != TrigHostSafety {
					t.Fatalf("vent %+v", as[2])
				}
			}},
		{name: "smoke from accessory vents itself", evt: Event{SN: "ACC00001", Code: "SMOKE_HIGH", RecvTs: recv}, pairings: []Pairing{p1, p2},
			want: []ActionType{ActCancelOff, ActSafetyVent, ActSetDesired},
			check: func(t *testing.T, as []Action) {
				if as[2].AccSN != "ACC00001" {
					t.Fatalf("vented %s", as[2].AccSN)
				}
			}},
		{name: "fire from accessory stops host (unknown state) and vents others", evt: Event{SN: "ACC00001", Code: "FIRE_SUPPRESSED", RecvTs: recv}, pairings: []Pairing{p1, p2},
			want: []ActionType{ActStopHost, ActCancelOff, ActSafetyVent, ActSetDesired},
			check: func(t *testing.T, as []Action) {
				if as[0].HostSN != host || as[3].AccSN != "ACC00002" {
					t.Fatalf("fire actions %+v", as)
				}
			}},
		{name: "fire with idle host → no stop", evt: Event{SN: "ACC00001", Code: "FIRE_SUPPRESSED", RecvTs: recv}, pairings: []Pairing{p1},
			mut: func(st *EngineState) { st.HostWorkState[host] = 0 }, want: nil},
		{name: "fire with stop disabled", evt: Event{SN: "ACC00001", Code: "FIRE_SUPPRESSED", RecvTs: recv}, pairings: []Pairing{p1, p2},
			mut: func(st *EngineState) { st.StopHostOnFire = false }, want: []ActionType{ActCancelOff, ActSafetyVent, ActSetDesired}},
		{name: "accessory other event ignored", evt: Event{SN: "ACC00001", Code: "HEARTBEAT", RecvTs: recv}, pairings: []Pairing{p1}, want: nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := baseState()
			if c.mut != nil {
				c.mut(&st)
			}
			got := Decide(c.evt, c.pairings, st)
			if !eqTypes(types(got), c.want) {
				t.Fatalf("actions %v want %v", types(got), c.want)
			}
			if c.check != nil {
				c.check(t, got)
			}
		})
	}
}

func TestReconcileDecide(t *testing.T) {
	p := pairing(1, "H", "A", true, 120)
	st := baseState()
	as, f := ReconcileDecide(p, HostView{WorkState: 2}, AccView{Known: true, PowerOn: false}, false, st)
	if len(f) != 1 || f[0] != "off_while_working" || len(as) != 1 || as[0].Type != ActSetDesired || !as[0].Desired.PowerOn {
		t.Fatalf("off_while_working: %v %v", as, f)
	}
	as, f = ReconcileDecide(p, HostView{WorkState: 0}, AccView{Known: true, PowerOn: true, TriggerSource: "cloud"}, false, st)
	if len(f) != 1 || f[0] != "long_on" || as[0].Type != ActScheduleOff || as[0].Delay != 120*time.Second {
		t.Fatalf("long_on: %v %v", as, f)
	}
	if as, _ := ReconcileDecide(p, HostView{WorkState: 0}, AccView{Known: true, PowerOn: true, TriggerSource: "cloud"}, true, st); len(as) != 0 {
		t.Fatal("timer exists → nothing")
	}
	if as, _ := ReconcileDecide(p, HostView{WorkState: 0}, AccView{Known: true, PowerOn: true, TriggerSource: "local"}, false, st); len(as) != 0 {
		t.Fatal("locally switched on → cloud must not turn off")
	}
	if as, _ := ReconcileDecide(p, HostView{WorkState: 2}, AccView{Known: false}, false, st); len(as) != 0 {
		t.Fatal("unknown accessory state → nothing")
	}
}

// ---------- 滤芯寿命纯函数 ----------

func TestFilterPure(t *testing.T) {
	cfg := DefaultFilterModel()
	for rpm, want := range map[float64]int{0: 0, -5: 0, 500: 1, 800: 1, 1700: 2, 2400: 3, 9000: 4} {
		if got := LevelFromRPM(rpm, cfg.RPMLevels); got != want {
			t.Errorf("LevelFromRPM(%v)=%d want %d", rpm, got, want)
		}
	}
	if PressureFactor(40, cfg) != 1 || PressureFactor(150, cfg) != 1.5 {
		t.Fatalf("PressureFactor %v %v", PressureFactor(40, cfg), PressureFactor(150, cfg))
	}
	st := ResetFilter(t0)
	if st.Health != 1 || !st.LastHour.Equal(t0) {
		t.Fatal("ResetFilter")
	}
	// 2 档一小时、无压差折损：等效风量 120 m³
	st = FilterStep(st, HourAgg{Hour: t0.Add(time.Hour), RunSeconds: 3600, Level: 2, PressureDiff: 50}, cfg)
	if st.EqAirVolume != 120 || st.RunSeconds != 3600 || !st.LastHour.Equal(t0.Add(time.Hour)) {
		t.Fatalf("step %+v", st)
	}
	wantHealth := 1 - 120/cfg.RatedVolume
	if diff := st.Health - wantHealth; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("health %v want %v", st.Health, wantHealth)
	}
	// 停机小时只推进 LastHour
	st2 := FilterStep(st, HourAgg{Hour: t0.Add(2 * time.Hour), RunSeconds: 0, Level: 0}, cfg)
	if st2.EqAirVolume != st.EqAirVolume || !st2.LastHour.Equal(t0.Add(2*time.Hour)) {
		t.Fatal("idle hour must only advance LastHour")
	}
	// 耗尽 → health 0
	ex := FilterStep(FilterState{EqAirVolume: cfg.RatedVolume * 2}, HourAgg{Hour: t0}, cfg)
	if ex.Health != 0 {
		t.Fatalf("exhausted health %v", ex.Health)
	}
	if eol, ok := PredictEOL(ex, 10, cfg, t0); !ok || !eol.Equal(t0) {
		t.Fatal("exhausted → EOL now")
	}
	if _, ok := PredictEOL(st, 0, cfg, t0); ok {
		t.Fatal("no daily usage → unknown")
	}
	if eol, ok := PredictEOL(FilterState{EqAirVolume: cfg.RatedVolume - 2400}, 1200, cfg, t0); !ok || !eol.Equal(t0.Add(48*time.Hour)) {
		t.Fatalf("eol %v %v", eol, ok)
	}
	q := BuildHourlyQuery("ACC'; DROP TABLE x;--", t0, t0.Add(time.Hour))
	if strings.Contains(q, "'; DROP") || !strings.Contains(q, "sn='ACCDROPTABLEx--'") {
		t.Fatalf("sanitize failed: %s", q)
	}
	res := &tdengine.Result{Data: [][]any{
		{"2026-09-19T11:00:00.000Z", 1650.0, 720.0},
		{"2026-09-19T10:00:00.000Z", 900.0, 360.0},
		{"bad", 1, 2},
	}}
	aggs, err := ParseHourly(res, 5)
	if err != nil || len(aggs) != 2 || !aggs[0].Hour.Equal(t0) || aggs[0].RunSeconds != 1800 || aggs[1].AvgRPM != 1650 {
		t.Fatalf("ParseHourly %+v %v", aggs, err)
	}
}

// ---------- 引擎与 handler（fake store / actuator）----------

type fakeStore struct {
	mu        sync.Mutex
	pairings  []Pairing
	products  map[string]string
	owners    map[string][]int64
	rules     map[string]int
	audits    []LinkageAudit
	contexts  []AlarmContext
	nextID    int64
	pairErr   error
	unpairErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{products: map[string]string{}, owners: map[string][]int64{}, rules: map[string]int{"ACRYLIC_3MM": 3}, nextID: 1}
}

func (f *fakeStore) Pair(_ context.Context, host, acc, accType string, enabled bool, off int, key string) (*Pairing, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pairErr != nil {
		return nil, false, f.pairErr
	}
	for i := range f.pairings {
		p := &f.pairings[i]
		if p.AccSN == acc && p.UnpairedAt == nil {
			if p.HostSN == host {
				return p, false, nil
			}
			now := t0
			p.UnpairedAt = &now
		}
	}
	p := Pairing{ID: f.nextID, HostSN: host, AccSN: acc, AccType: accType, LinkageEnabled: enabled, OffDelayS: off, PairedAt: t0}
	f.nextID++
	f.pairings = append(f.pairings, p)
	return &p, true, nil
}
func (f *fakeStore) Unpair(_ context.Context, id int64) (*Pairing, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.pairings {
		if f.pairings[i].ID == id && f.pairings[i].UnpairedAt == nil {
			now := t0
			f.pairings[i].UnpairedAt = &now
			return &f.pairings[i], nil
		}
	}
	return nil, ErrNotFound
}
func (f *fakeStore) UpdatePairing(_ context.Context, id int64, enabled *bool, off *int) (*Pairing, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.pairings {
		if f.pairings[i].ID == id && f.pairings[i].UnpairedAt == nil {
			if enabled != nil {
				f.pairings[i].LinkageEnabled = *enabled
			}
			if off != nil {
				f.pairings[i].OffDelayS = *off
			}
			return &f.pairings[i], nil
		}
	}
	return nil, ErrNotFound
}
func (f *fakeStore) PairingByID(_ context.Context, id int64) (*Pairing, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.pairings {
		if f.pairings[i].ID == id {
			return &f.pairings[i], nil
		}
	}
	return nil, ErrNotFound
}
func (f *fakeStore) PairingsByHost(_ context.Context, host string) ([]Pairing, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Pairing
	for _, p := range f.pairings {
		if p.HostSN == host && p.UnpairedAt == nil {
			out = append(out, p)
		}
	}
	return out, nil
}
func (f *fakeStore) PairingByAcc(_ context.Context, acc string) (*Pairing, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.pairings {
		if f.pairings[i].AccSN == acc && f.pairings[i].UnpairedAt == nil {
			return &f.pairings[i], nil
		}
	}
	return nil, ErrNotFound
}
func (f *fakeStore) AllActivePairings(_ context.Context) ([]Pairing, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Pairing
	for _, p := range f.pairings {
		if p.UnpairedAt == nil {
			out = append(out, p)
		}
	}
	return out, nil
}
func (f *fakeStore) DeviceProduct(_ context.Context, sn string) (string, bool, error) {
	pk, ok := f.products[sn]
	return pk, ok, nil
}
func (f *fakeStore) Owners(_ context.Context, sn string) ([]int64, error) { return f.owners[sn], nil }
func (f *fakeStore) Rules(_ context.Context, _ string) (map[string]int, error) {
	return f.rules, nil
}
func (f *fakeStore) InsertAudit(_ context.Context, a LinkageAudit) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audits = append(f.audits, a)
	return nil
}
func (f *fakeStore) ListAudit(_ context.Context, host string, limit int) ([]LinkageAudit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []LinkageAudit
	for _, a := range f.audits {
		if a.HostSN == host {
			out = append(out, a)
		}
	}
	return out, nil
}
func (f *fakeStore) InsertAlarmContext(_ context.Context, c AlarmContext) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.contexts = append(f.contexts, c)
	return nil
}
func (f *fakeStore) GetAlarmContext(context.Context, string, string, time.Time) (*AlarmContext, error) {
	return nil, ErrNotFound
}
func (f *fakeStore) FilterModel(context.Context, string) (*FilterModelCfg, error) {
	return nil, ErrNotFound
}
func (f *fakeStore) FilterLifeGet(context.Context, string) (*FilterLife, error) {
	return nil, ErrNotFound
}
func (f *fakeStore) FilterLifeUpsert(context.Context, FilterLife) error      { return nil }
func (f *fakeStore) AccessorySNs(context.Context, string) ([]string, error)  { return nil, nil }
func (f *fakeStore) EnsureAuditPartitions(context.Context, int) (int, error) { return 0, nil }

type fakeAct struct {
	mu       sync.Mutex
	desired  map[string][]map[string]any
	stops    []string
	failNext bool
}

func newFakeAct() *fakeAct { return &fakeAct{desired: map[string][]map[string]any{}} }
func (a *fakeAct) SetDesired(_ context.Context, sn string, f map[string]any) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failNext {
		a.failNext = false
		return errors.New("deviceapi down")
	}
	a.desired[sn] = append(a.desired[sn], f)
	return nil
}
func (a *fakeAct) StopHost(_ context.Context, host string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stops = append(a.stops, host)
	return nil
}

func eventEnv(sn, code, material string, seq int64, recv time.Time) *envelope.Envelope {
	p, _ := json.Marshal(map[string]any{"seq": seq, "ts": recv.UnixMilli(), "code": code, "material_id": material})
	return &envelope.Envelope{PK: "LM_S1", SN: sn, Kind: envelope.KindEvent, Seq: seq, RecvTs: recv.UnixMilli(), Payload: p}
}

func TestNormalize(t *testing.T) {
	if _, _, ok := Normalize(&envelope.Envelope{Kind: envelope.KindTelemetry, Payload: []byte(`{"seq":1}`)}); ok {
		t.Fatal("telemetry without work_state is irrelevant")
	}
	evt, job, ok := Normalize(&envelope.Envelope{Kind: envelope.KindEvent, SN: "H", Seq: 7, RecvTs: 99, Payload: []byte(`{"code":"JOB_START","material_id":"M","job_id":"J"}`)})
	if !ok || evt.Code != "JOB_START" || evt.MaterialID != "M" || job != "J" || evt.Seq != 7 || evt.Ts != 99 {
		t.Fatalf("event normalize %+v %s %v", evt, job, ok)
	}
	if _, _, ok := Normalize(&envelope.Envelope{Kind: envelope.KindCmdAck}); ok {
		t.Fatal("cmd_ack irrelevant")
	}
}

func TestEngineHandle(t *testing.T) {
	st := newFakeStore()
	act := newFakeAct()
	st.pairings = []Pairing{pairing(1, "HOST0001", "ACC00001", true, 180)}
	eng := NewEngine(st, nil, act, nil, Options{AllowOff: true, StopHostOnFire: true, Now: func() time.Time { return t0 }})
	ctx := context.Background()

	if out, err := eng.Handle(ctx, eventEnv("HOST0009", "JOB_START", "", 1, t0)); err != nil || out != OutcomeNoPairing {
		t.Fatalf("unpaired host: %v %v", out, err)
	}
	out, err := eng.Handle(ctx, eventEnv("HOST0001", "JOB_START", "ACRYLIC_3MM", 2, t0))
	if err != nil || out != OutcomeActed {
		t.Fatalf("job start: %v %v", out, err)
	}
	if got := act.desired["ACC00001"]; len(got) != 1 || got[0]["power_on"] != true || got[0]["fan_level"] != 3 {
		t.Fatalf("desired %v", got)
	}
	if len(st.audits) != 1 || st.audits[0].Result != "ok" || st.audits[0].Trigger != string(TrigJobStart) {
		t.Fatalf("audit %+v", st.audits)
	}
	// 同目标状态再来一次 JOB_START：只剩 cancel_off（无定时器不留痕），不重复下发 desired
	if out, _ := eng.Handle(ctx, eventEnv("HOST0001", "JOB_START", "ACRYLIC_3MM", 3, t0)); out != OutcomeActed {
		t.Fatalf("idempotent outcome: %v", out)
	}
	if len(act.desired["ACC00001"]) != 1 || len(st.audits) != 1 {
		t.Fatalf("idempotent must not re-dispatch: desired=%d audits=%d", len(act.desired["ACC00001"]), len(st.audits))
	}
	// 过期事件
	if out, _ := eng.Handle(ctx, eventEnv("HOST0001", "JOB_START", "", 4, t0.Add(-5*time.Minute))); out != OutcomeStale {
		t.Fatalf("stale: %v", out)
	}
	// 配件灭火：写 alarm_context + 停主机（主机状态未知）
	out, err = eng.Handle(ctx, eventEnv("ACC00001", "FIRE_SUPPRESSED", "", 5, t0))
	if err != nil || out != OutcomeActed || len(st.contexts) != 1 || st.contexts[0].HostSN != "HOST0001" || len(act.stops) != 1 {
		t.Fatalf("fire: %v %v ctx=%+v stops=%v", out, err, st.contexts, act.stops)
	}
	// 下发失败不 Nak，留 failed 审计
	act.failNext = true
	st.pairings[0].LinkageEnabled = true
	eng.mu.Lock()
	delete(eng.current, "ACC00001")
	eng.mu.Unlock()
	if out, err := eng.Handle(ctx, eventEnv("HOST0001", "JOB_START", "BASSWOOD_3MM", 6, t0)); err != nil || out != OutcomeActed {
		t.Fatalf("failed dispatch must still ack: %v %v", out, err)
	}
	last := st.audits[len(st.audits)-1]
	if last.Result != "failed" {
		t.Fatalf("expected failed audit, got %+v", last)
	}
}

func newTestService(t *testing.T) (*Service, *fakeStore, *fakeAct) {
	t.Helper()
	st := newFakeStore()
	st.products = map[string]string{"HOST0001": "LM_S1", "ACC00001": "ACC_PURIFIER", "LM_OTHER": "LM_S1"}
	st.owners = map[string][]int64{"HOST0001": {7}, "ACC00001": {7}, "LM_OTHER": {8}}
	act := newFakeAct()
	svc := &Service{Store: st, Act: act, M: NewMetrics()}
	return svc, st, act
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPairingHandler(t *testing.T) {
	svc, st, act := newTestService(t)
	h := Routes(svc, "test")

	rec := do(h, http.MethodPost, "/api/v1/pairings", `{"host_sn":"HOST0001","acc_sn":"ACC00001"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	if got := act.desired["ACC00001"]; len(got) != 1 || got[0]["paired_sn"] != "HOST0001" {
		t.Fatalf("paired_sn desired %v", got)
	}
	// 幂等：同主机再配 → 200 created=false
	rec = do(h, http.MethodPost, "/api/v1/pairings", `{"host_sn":"HOST0001","acc_sn":"ACC00001"}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"created":false`) {
		t.Fatalf("idempotent: %d %s", rec.Code, rec.Body)
	}
	// 配件不是 ACC_* 机型 → 400
	if rec = do(h, http.MethodPost, "/api/v1/pairings", `{"host_sn":"HOST0001","acc_sn":"LM_OTHER"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("non-accessory: %d %s", rec.Code, rec.Body)
	}
	// owner 不一致 → 403
	st.owners["ACC00001"] = []int64{99}
	if rec = do(h, http.MethodPost, "/api/v1/pairings", `{"host_sn":"HOST0001","acc_sn":"ACC00001"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("owner mismatch: %d %s", rec.Code, rec.Body)
	}
	st.owners["ACC00001"] = []int64{7}
	// 未知设备 → 404；非法 SN → 400
	if rec = do(h, http.MethodPost, "/api/v1/pairings", `{"host_sn":"NOPE0001","acc_sn":"ACC00001"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown host: %d", rec.Code)
	}
	if rec = do(h, http.MethodPost, "/api/v1/pairings", `{"host_sn":"bad sn","acc_sn":"ACC00001"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad sn: %d", rec.Code)
	}
	// 查询、修改、解除
	if rec = do(h, http.MethodGet, "/api/v1/pairings?host_sn=HOST0001", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ACC00001") {
		t.Fatalf("list: %d %s", rec.Code, rec.Body)
	}
	if rec = do(h, http.MethodPatch, "/api/v1/pairings/1", `{"off_delay_s":30}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("off_delay below 60 must be 400: %d", rec.Code)
	}
	if rec = do(h, http.MethodPatch, "/api/v1/pairings/1", `{"off_delay_s":240,"linkage_enabled":false}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"off_delay_s":240`) {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body)
	}
	if rec = do(h, http.MethodDelete, "/api/v1/pairings/1", ""); rec.Code != http.StatusOK {
		t.Fatalf("unpair: %d %s", rec.Code, rec.Body)
	}
	if rec = do(h, http.MethodDelete, "/api/v1/pairings/1", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unpair twice: %d", rec.Code)
	}
	if rec = do(h, http.MethodGet, "/api/v1/pairings?acc_sn=ACC00001", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("by acc after unpair: %d", rec.Code)
	}
}
