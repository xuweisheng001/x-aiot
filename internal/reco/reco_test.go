package reco

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

func TestCanonicalHash(t *testing.T) {
	a := json.RawMessage(`{"speed": 12, "power": 80, "passes": 1}`)
	b := json.RawMessage(`{"passes":1,"power":80,"speed":12}`)
	c := json.RawMessage(`{"passes":1,"power":80.0,"speed":12}`) // 数字原文不同 → 不同 hash（规则：数字保持原文）
	if CanonicalHash(a) != CanonicalHash(b) {
		t.Fatal("key order / whitespace must not change hash")
	}
	if CanonicalHash(a) == CanonicalHash(c) {
		t.Fatal("80 vs 80.0 are different canonical texts")
	}
	got, _ := Canonicalize(json.RawMessage(`{"b":[3,{"z":1,"a":2}],"a":"x"}`))
	if got != `{"a":"x","b":[3,{"a":2,"z":1}]}` {
		t.Fatalf("canonical: %s", got)
	}
	// 固定向量：空对象
	if CanonicalHash(json.RawMessage(`{}`)) != "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a" {
		t.Fatalf("sha256('{}') vector mismatch: %s", CanonicalHash(json.RawMessage(`{}`)))
	}
	if CanonicalHash(json.RawMessage(`{`)) != "" {
		t.Fatal("invalid json → empty hash")
	}
	if len(CanonicalHash(a)) != 64 {
		t.Fatal("hash length")
	}
}

func mkRows(n int, key GroupKey, rating string, day time.Time) []FeedbackRow {
	out := make([]FeedbackRow, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, FeedbackRow{GroupKey: key, SN: fmt.Sprintf("SN%03d", i), Rating: rating, At: day.Add(time.Duration(i) * time.Minute)})
	}
	return out
}

func TestAggregate(t *testing.T) {
	day := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	key := GroupKey{ProductKey: "LM_S1", ModuleModel: "M10W", MaterialID: "BASSWOOD_3MM", ParamsHash: "h1"}
	r := DefaultRules()

	t.Run("35 devices all good → candidate", func(t *testing.T) {
		c := Aggregate(mkRows(35, key, "good", day), r)
		if len(c) != 1 || c[0].Excluded || c[0].Samples != 35 || c[0].DistinctSN != 35 || c[0].GoodRatio != 1 {
			t.Fatalf("%+v", c)
		}
	})
	t.Run("29 devices → few_samples", func(t *testing.T) {
		c := Aggregate(mkRows(29, key, "good", day), r)
		if !c[0].Excluded || c[0].Reason != ReasonFewSamples {
			t.Fatalf("%+v", c[0])
		}
	})
	t.Run("30 devices exactly → candidate (boundary)", func(t *testing.T) {
		c := Aggregate(mkRows(30, key, "good", day), r)
		if c[0].Excluded {
			t.Fatalf("%+v", c[0])
		}
	})
	t.Run("same SN same day dedup: 1 device × 40 marks → 1 sample and device_day_flood", func(t *testing.T) {
		rows := make([]FeedbackRow, 0, 40)
		for i := 0; i < 40; i++ {
			rows = append(rows, FeedbackRow{GroupKey: key, SN: "SN1", Rating: "good", At: day.Add(time.Duration(i) * time.Second)})
		}
		c := Aggregate(rows, r)
		if c[0].Samples != 1 || !c[0].Excluded || c[0].Reason != ReasonDeviceFlood {
			t.Fatalf("%+v", c[0])
		}
	})
	t.Run("dedup keeps last rating of the day", func(t *testing.T) {
		rows := mkRows(30, key, "good", day)
		rows = append(rows, FeedbackRow{GroupKey: key, SN: "SN000", Rating: "burnt", At: day.Add(5 * time.Hour)})
		c := Aggregate(rows, r)
		if c[0].Samples != 30 || c[0].Good != 29 {
			t.Fatalf("samples=%d good=%d", c[0].Samples, c[0].Good)
		}
	})
	t.Run("same SN different days count as two samples but one device", func(t *testing.T) {
		rows := mkRows(29, key, "good", day)
		rows = append(rows, FeedbackRow{GroupKey: key, SN: "SN000", Rating: "good", At: day.Add(48 * time.Hour)})
		c := Aggregate(rows, r)
		if c[0].Samples != 30 || c[0].DistinctSN != 29 || c[0].Reason != ReasonFewDevices {
			t.Fatalf("%+v", c[0])
		}
	})
	t.Run("top3 share > 50% → excluded", func(t *testing.T) {
		rows := mkRows(30, key, "good", day) // 30 台各 1
		for d := 1; d <= 40; d++ {           // 3 台各再贡献 40 天 → 120 样本，占 120/150 = 80%
			for _, sn := range []string{"SN000", "SN001", "SN002"} {
				rows = append(rows, FeedbackRow{GroupKey: key, SN: sn, Rating: "good", At: day.Add(time.Duration(d) * 24 * time.Hour)})
			}
		}
		c := Aggregate(rows, r)
		if !c[0].Excluded || c[0].Reason != ReasonTop3Share {
			t.Fatalf("%+v", c[0])
		}
	})
	t.Run("good ratio 0.79 → low_good_ratio; 0.8 → candidate", func(t *testing.T) {
		rows := mkRows(100, key, "good", day)
		for i := 0; i < 21; i++ {
			rows[i].Rating = "burnt"
		}
		if c := Aggregate(rows, r); !c[0].Excluded || c[0].Reason != ReasonLowGood {
			t.Fatalf("%+v", c[0])
		}
		rows[20].Rating = "good"
		if c := Aggregate(rows, r); c[0].Excluded {
			t.Fatalf("%+v", c[0])
		}
	})
	t.Run("unknown dims excluded first", func(t *testing.T) {
		k := key
		k.ModuleModel = "UNKNOWN"
		c := Aggregate(mkRows(35, k, "good", day), r)
		if c[0].Reason != ReasonUnknownDims {
			t.Fatalf("%+v", c[0])
		}
	})
	t.Run("two groups sorted stably", func(t *testing.T) {
		k2 := key
		k2.ParamsHash = "h0"
		c := Aggregate(append(mkRows(35, key, "good", day), mkRows(35, k2, "good", day)...), r)
		if len(c) != 2 || c[0].ParamsHash != "h0" || c[1].ParamsHash != "h1" {
			t.Fatalf("%+v", c)
		}
	})
	if len(Aggregate(nil, r)) != 0 {
		t.Fatal("empty input")
	}
}

func TestWithinPowerCap(t *testing.T) {
	cases := []struct {
		cand, official float64
		ok             bool
	}{
		{80, 80, true}, {88, 80, true}, {88.01, 80, false}, {50, 0, true}, {100, 90, false}, {99, 90, true},
	}
	for _, c := range cases {
		if got := WithinPowerCap(c.cand, c.official); got != c.ok {
			t.Errorf("cand=%v official=%v got %v", c.cand, c.official, got)
		}
	}
}

func TestSafetyUnpublish(t *testing.T) {
	cases := []struct {
		reco, official float64
		abs            int
		want           bool
	}{
		{0.10, 0.02, 3, true},
		{0.10, 0.02, 2, false}, // 绝对数不足
		{0.04, 0.02, 5, false}, // 恰等于 2 倍不下线（严格大于）
		{0.041, 0.02, 5, true},
		{0.01, 0, 3, true},     // 无基线但绝对数达标
		{0, 0, 10, false},      // 无事故
		{0.5, 0.3, 100, false}, // 高但未到 2 倍
	}
	for _, c := range cases {
		if got := SafetyUnpublish(c.reco, c.official, c.abs); got != c.want {
			t.Errorf("reco=%v official=%v abs=%d got %v", c.reco, c.official, c.abs, got)
		}
	}
}

func TestCountJobsWithSafety(t *testing.T) {
	t0 := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	jobs := []JobSpan{
		{SN: "A", Start: t0, End: t0.Add(30 * time.Minute)},                 // 命中
		{SN: "A", Start: t0.Add(2 * time.Hour), End: t0.Add(3 * time.Hour)}, // 无事件
		{SN: "B", Start: t0},                         // 无 End，兜底 2h → 命中
		{SN: "C", Start: t0, End: t0.Add(time.Hour)}, // 事件在窗外
	}
	events := []SafetyEvent{
		{SN: "A", At: t0.Add(10 * time.Minute)},
		{SN: "B", At: t0.Add(90 * time.Minute)},
		{SN: "C", At: t0.Add(2 * time.Hour)},
	}
	if n := CountJobsWithSafety(jobs, events); n != 2 {
		t.Fatalf("got %d want 2", n)
	}
	if Rate(2, 4) != 0.5 || Rate(1, 0) != 0 {
		t.Fatal("rate")
	}
}

func TestParseSafetyEvents(t *testing.T) {
	res := &tdengine.Result{
		ColumnMeta: [][]any{{"sn", "VARCHAR", 32}, {"ts", "TIMESTAMP", 8}}, // 乱序列
		Data: [][]any{
			{"SN1", "2026-09-18T10:00:00.000Z"},
			{"SN2", float64(1_758_189_600_000)},
			{"", "2026-09-18T10:00:00.000Z"}, // 空 SN 跳过
			{"SN3"},                          // 列不足跳过
			{"SN4", "not-a-time"},            // 坏时间跳过
		},
	}
	ev, skipped := ParseSafetyEvents(res)
	if len(ev) != 2 || skipped != 3 || ev[0].SN != "SN1" || ev[1].SN != "SN2" {
		t.Fatalf("ev=%+v skipped=%d", ev, skipped)
	}
	if ev, skipped := ParseSafetyEvents(nil); ev != nil || skipped != 0 {
		t.Fatal("nil result")
	}
	// 安全码集合随业务线增加（BL2 加了 FIRE_SUPPRESSED / SMOKE_HIGH），断言从 envelope 派生而不是写死，
	// 否则每加一个码这里都要改一次，而真正要守的是「全部安全码都进查询且排序稳定」。
	codes := make([]string, 0, len(envelope.SafetyCodes))
	for c := range envelope.SafetyCodes {
		codes = append(codes, "'"+c+"'")
	}
	sort.Strings(codes)
	want := "SELECT ts, sn FROM iot.events WHERE ts >= 1000 AND code IN (" + strings.Join(codes, ",") + ")"
	q := BuildSafetyEventsQuery(time.UnixMilli(1000))
	if q != want {
		t.Fatalf("query:\n got %s\nwant %s", q, want)
	}
}

func TestCorrection(t *testing.T) {
	cfg := DefaultCorrectionCfg()
	f := func(v float64) *float64 { return &v }
	t.Run("inputs missing → 1", func(t *testing.T) {
		for _, c := range []Coefs{Correction(nil, f(100), cfg), Correction(f(90), nil, cfg), Correction(f(90), f(100), CorrectionCfg{})} {
			if c.KPower != 1 || c.KSpeed != 1 || len(c.Reasons) != 1 || c.Reasons[0] != "inputs_missing_no_correction" {
				t.Fatalf("%+v", c)
			}
		}
	})
	t.Run("new module → ~1", func(t *testing.T) {
		c := Correction(f(100), f(0), cfg)
		if c.KPower != 1 || c.KSpeed != 1 {
			t.Fatalf("%+v", c)
		}
	})
	t.Run("worn module → within bounds with reasons", func(t *testing.T) {
		c := Correction(f(60), f(320), cfg) // 1 + 0.15*0.4 + 0.05*0.032 = 1.0616
		if c.KPower < 1.06 || c.KPower > 1.07 || c.KSpeed < 0.97 || c.KSpeed > 0.99 || len(c.Reasons) != 2 {
			t.Fatalf("%+v", c)
		}
	})
	t.Run("dead module → clamped at 1.25 with cap reason", func(t *testing.T) {
		c := Correction(f(0), f(50000), cfg)
		if c.KPower != KMax || c.Reasons[len(c.Reasons)-1] != "power_at_cap_consider_replacing_module" {
			t.Fatalf("%+v", c)
		}
	})
	t.Run("health out of range clamped", func(t *testing.T) {
		if Correction(f(150), f(0), cfg).KPower != 1 || Correction(f(-5), f(0), cfg).KPower <= 1 {
			t.Fatal("health clamp")
		}
	})
	t.Run("cap share alarm", func(t *testing.T) {
		share, alarm := CapShareAlarm([]float64{1, 1, 1.25, 1.25, 1.25}, 0.2)
		if share != 0.6 || !alarm {
			t.Fatalf("%v %v", share, alarm)
		}
		if _, alarm := CapShareAlarm(nil, 0.2); alarm {
			t.Fatal("empty")
		}
	})
}
