package ota

import (
	"fmt"
	"math"
	"testing"
)

func TestShouldFuse(t *testing.T) {
	cases := []struct {
		name       string
		ok, fail   int
		threshold  float64
		minSamples int
		want       bool
	}{
		{"no samples", 0, 0, 0.02, 50, false},
		{"49 samples all failed never fuse", 0, 49, 0.02, 50, false},
		{"50 samples all failed fuses", 0, 50, 0.02, 50, true},
		{"49 ok + 1 fail = 2% equal threshold not fuse", 49, 1, 0.02, 50, false},
		{"98 ok + 2 fail = 2% equal threshold not fuse", 98, 2, 0.02, 50, false},
		{"97 ok + 3 fail = 3% > 2% fuse", 97, 3, 0.02, 50, true},
		{"48 ok + 2 fail = 4% > 2% fuse (exactly 50 samples)", 48, 2, 0.02, 50, true},
		{"47 ok + 2 fail = 49 samples below min", 47, 2, 0.02, 50, false},
		{"1000 ok + 20 fail = 2% not fuse", 1000, 20, 0.02, 50, false},
		{"1000 ok + 21 fail > 2% fuse", 1000, 21, 0.02, 50, true},
		{"threshold 0: any failure fuses once min samples reached", 60, 1, 0, 50, true},
		{"threshold 0, zero failures never fuse", 60, 0, 0, 50, false},
		{"threshold 1: never fuse (ratio cannot exceed 1)", 0, 100, 1, 50, false},
		{"minSamples 0 with 1 sample", 0, 1, 0.5, 0, true},
		{"negative inputs ignored", -1, 5, 0.02, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ShouldFuse(c.ok, c.fail, c.threshold, c.minSamples); got != c.want {
				t.Errorf("ShouldFuse(%d,%d,%v,%d)=%v want %v", c.ok, c.fail, c.threshold, c.minSamples, got, c.want)
			}
		})
	}
}

func TestFailRatio(t *testing.T) {
	if FailRatio(0, 0) != 0 {
		t.Error("empty ratio should be 0")
	}
	if r := FailRatio(3, 1); math.Abs(r-0.25) > 1e-9 {
		t.Errorf("got %v", r)
	}
}

func TestStagePctAndNext(t *testing.T) {
	for i, s := range Stages {
		if _, err := StagePct(s); err != nil {
			t.Errorf("StagePct(%q): %v", s, err)
		}
		next, ok := NextStage(s)
		if i == len(Stages)-1 {
			if ok {
				t.Errorf("stage %q should have no next", s)
			}
		} else if !ok || next != Stages[i+1] {
			t.Errorf("NextStage(%q)=%q,%v", s, next, ok)
		}
	}
	for _, bad := range []string{"", "5", "100.0", "0.01", "abc"} {
		if _, err := StagePct(bad); err == nil {
			t.Errorf("StagePct(%q) should fail", bad)
		}
	}
}

// 累进包含性质：任一 sn 若落入较小档位，必落入所有更大档位；100 全包含；0 全不包含。
func TestInStage_CumulativeInclusive(t *testing.T) {
	pcts := []float64{0.1, 1, 10, 50, 100}
	counts := make([]int, len(pcts))
	const n = 20000
	for i := 0; i < n; i++ {
		sn := fmt.Sprintf("LM_S2409180100%07d", i)
		prev := false
		for j, p := range pcts {
			in := InStage(sn, p)
			if prev && !in {
				t.Fatalf("sn %s in stage %v but not in larger stage %v", sn, pcts[j-1], p)
			}
			if in {
				counts[j]++
			}
			prev = in
		}
		if !InStage(sn, 100) {
			t.Fatalf("stage 100 must include %s", sn)
		}
		if InStage(sn, 0) {
			t.Fatalf("stage 0 must include nothing, got %s", sn)
		}
	}
	// 抽样比例大致正确（±40% 相对误差足够宽松，只检查量级）
	for j, p := range pcts {
		expect := float64(n) * p / 100
		if float64(counts[j]) < expect*0.6 || float64(counts[j]) > expect*1.4+5 {
			t.Errorf("stage %v: got %d expect ≈%.0f", p, counts[j], expect)
		}
	}
	// 单调：档位越大命中越多（非减）
	for j := 1; j < len(counts); j++ {
		if counts[j] < counts[j-1] {
			t.Errorf("counts not monotonic: %v", counts)
		}
	}
}

// 稳定性：同一 sn 多次计算落入同一桶；Bucket ∈ [0,1)。
func TestBucketStable(t *testing.T) {
	for _, sn := range []string{"SN0001", "SN0002", "LM_S24091801000000001", ""} {
		b1, b2 := Bucket(sn), Bucket(sn)
		if b1 != b2 || b1 < 0 || b1 >= 1 {
			t.Errorf("Bucket(%q)=%v,%v", sn, b1, b2)
		}
	}
	// 固定向量（md5 低 32 位 / 2^32），防止算法被无意改动
	if b := Bucket("SN0001"); math.Abs(b-0.01956838322803378) > 1e-12 {
		t.Errorf("Bucket(SN0001)=%v", b)
	}
	if !InStage("SN0001", 10) || InStage("SN0001", 1) {
		t.Error("SN0001 (bucket≈0.0196) should be in 10% but not in 1%")
	}
}

func TestPhases(t *testing.T) {
	for _, p := range []string{TaskSuccess, TaskFailed, TaskRolledBack} {
		if !IsTerminal(p) || !ValidPhase(p) {
			t.Errorf("%s should be terminal & valid", p)
		}
	}
	for _, p := range []string{TaskNotified, TaskDownloading, TaskVerifying} {
		if IsTerminal(p) || !ValidPhase(p) {
			t.Errorf("%s should be non-terminal & valid", p)
		}
	}
	if ValidPhase("pending") || ValidPhase("") || ValidPhase("bogus") {
		t.Error("pending/empty/bogus are not device-reportable phases")
	}
	if len(TerminalStates()) != 3 {
		t.Error("TerminalStates size")
	}
}

func TestNewUUIDv4(t *testing.T) {
	u := newUUID()
	if len(u) != 36 || u[14] != '4' || (u[19] != '8' && u[19] != '9' && u[19] != 'a' && u[19] != 'b') {
		t.Errorf("not a v4 uuid: %s", u)
	}
	if newUUID() == u {
		t.Error("uuid collision")
	}
}

func TestMQTTEncoding(t *testing.T) {
	cases := map[int][]byte{0: {0}, 127: {127}, 128: {0x80, 0x01}, 16383: {0xFF, 0x7F}, 16384: {0x80, 0x80, 0x01}}
	for n, want := range cases {
		got := encLen(n)
		if string(got) != string(want) {
			t.Errorf("encLen(%d)=%v want %v", n, got, want)
		}
	}
	if s := lenStr("MQTT"); string(s) != "\x00\x04MQTT" {
		t.Errorf("lenStr=%q", s)
	}
	if _, err := NewMiniMQTT("http://x", "c", "", ""); err == nil {
		t.Error("scheme should be rejected")
	}
	if m, err := NewMiniMQTT("tcp://127.0.0.1:1883", "c", "", ""); err != nil || m.addr != "127.0.0.1:1883" {
		t.Errorf("parse: %v %+v", err, m)
	}
}
