package cf001

import "testing"

func TestChecksum(t *testing.T) {
	cases := []struct{ in, want string }{
		// 需求文档差异记录：文档给出的手写示例校验码为 "93"，但按其自己定义的算法
		//（前 21 位 ASCII 求和 % 256 → 两位大写 hex），下面这个 21 位样例求和为 1143，
		// 1143 % 256 = 119 = 0x77。实现以算法为准，文档的 93 是笔误（见 docs/cf001-impl.md 差异记录）。
		{"XTCF24010101000000009", "77"},
		{"LM_S24091801000000001", "95"},
		{"", "00"},
		{"\x00", "00"},
		{"A", "41"},
		{"AB", "83"},
		{"\xff\x01", "00"},              // 255+1 = 256 → 0
		{"zzzz", "E8"},                  // 122*4 = 488 % 256 = 232 = 0xE8
		{"aaaaaaaaaaaaaaaaaaaaa", "F5"}, // 97*21 = 2037 % 256 = 245
	}
	for _, c := range cases {
		if got := Checksum(c.in); got != c.want {
			t.Errorf("Checksum(%q)=%s want %s", c.in, got, c.want)
		}
	}
}

func TestBuildSN(t *testing.T) {
	cases := []struct {
		pk, day, line string
		seq           int64
		want          string
	}{
		{"LM_S1", "240918", "01", 1, "LM_S24091801000000001" + "95"},
		{"LM_S1", "240918", "", 1, "LM_S24091801000000001" + "95"},                              // line 默认 01
		{"lm_s1", "240918", "1", 1, "LM_S24091801000000001" + "95"},                             // 大写 + 单位数产线补零
		{"LM", "240918", "02", 42, "LM__24091802000000042" + Checksum("LM__24091802000000042")}, // pk 不足 4 位补 _
		{"ACC_PURIFIER", "240101", "01", 999999999, "ACC_24010101999999999" + Checksum("ACC_24010101999999999")},
		{"ACC_PURIFIER", "240101", "01", 1_000_000_001, "ACC_24010101000000001" + Checksum("ACC_24010101000000001")}, // 9 位取模
	}
	for _, c := range cases {
		got := BuildSN(c.pk, c.day, c.line, c.seq)
		if got != c.want {
			t.Errorf("BuildSN(%q,%q,%q,%d)=%s want %s", c.pk, c.day, c.line, c.seq, got, c.want)
		}
		if len(got) != SNLen || !ValidSN(got) {
			t.Errorf("SN %s invalid (len %d)", got, len(got))
		}
	}
	// 篡改任一位校验失败
	sn := BuildSN("LM_S1", "240918", "01", 7)
	bad := []byte(sn)
	bad[10] = 'X'
	if ValidSN(string(bad)) {
		t.Error("tampered SN must not validate")
	}
	if ValidSN("short") {
		t.Error("short SN must not validate")
	}
}

func TestPrefix4AndLine(t *testing.T) {
	for in, want := range map[string]string{"LM_S1": "LM_S", "lm": "LM__", "": "____", "ABCDEFG": "ABCD"} {
		if got := Prefix4(in); got != want {
			t.Errorf("Prefix4(%q)=%q want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{"": "01", "3": "03", "12": "12", "123": "23", " 7 ": "07"} {
		if got := NormalizeLine(in); got != want {
			t.Errorf("NormalizeLine(%q)=%q want %q", in, got, want)
		}
	}
}
