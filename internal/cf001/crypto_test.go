package cf001

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"strings"
	"testing"
)

func TestDigestVector(t *testing.T) {
	// 固定向量（sha256("550e8400-...:MCU0001:SOC0001:AA:BB:CC:DD:EE:FF")），防止拼接规则被改动。
	got := Digest("550e8400-e29b-41d4-a716-446655440000", "MCU0001", "SOC0001", "AA:BB:CC:DD:EE:FF")
	want := "d1fe4cd7b0476291ef20e747a1fe16b5644410c30d93c768702452d974a654f9"
	if got != want {
		t.Fatalf("Digest=%s want %s", got, want)
	}
	if len(got) != 64 {
		t.Fatalf("len=%d", len(got))
	}
	// uuid 参与哈希：相同硬件三元组、不同 uuid → 不同 digest（供应商无法离线反推）
	if Digest("u1", "m", "s", "mac") == Digest("u2", "m", "s", "mac") {
		t.Error("uuid must influence digest")
	}
	// 分隔符防串位：("ab","c") 与 ("a","bc") 不同
	if Digest("ab", "c", "x", "y") == Digest("a", "bc", "x", "y") {
		t.Error("separator must prevent field sliding")
	}
}

func TestParsePayload(t *testing.T) {
	d := strings.Repeat("ab", 32)
	cases := []struct {
		name    string
		in      string
		wantErr bool
		want    Payload
	}{
		{"three segments", d + "|nonce1|1726300000", false, Payload{d, "nonce1", 1726300000}},
		{"fourth segment (voltage) ignored", d + "|nonce1|1726300000|3.7", false, Payload{d, "nonce1", 1726300000}},
		{"five segments ignored", d + "|n|1|a|b", false, Payload{d, "n", 1}},
		{"uppercase digest normalised", strings.ToUpper(d) + "|n|1", false, Payload{d, "n", 1}},
		{"two segments", d + "|nonce1", true, Payload{}},
		{"one segment", d, true, Payload{}},
		{"empty", "", true, Payload{}},
		{"short digest", "abcd|n|1", true, Payload{}},
		{"non-hex digest", strings.Repeat("zz", 32) + "|n|1", true, Payload{}},
		{"empty nonce", d + "||1", true, Payload{}},
		{"bad timestamp", d + "|n|notanumber", true, Payload{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParsePayload([]byte(c.in))
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if err != nil {
				if !errors.Is(err, ErrBadPayload) {
					t.Errorf("error should wrap ErrBadPayload: %v", err)
				}
				return
			}
			if got != c.want {
				t.Errorf("got %+v want %+v", got, c.want)
			}
		})
	}
}

func TestOAEPAndPSSRoundtrip(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	// 公钥 PEM 往返
	pemB, err := PublicKeyPEM(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := ParsePublicKeyPEM(pemB)
	if err != nil || pub.N.Cmp(key.N) != 0 {
		t.Fatalf("pubkey pem roundtrip: %v", err)
	}
	// OAEP：设备用公钥加密三段载荷，云端私钥解密并解析
	d := Digest("uuid", "mcu", "soc", "mac")
	pt := d + "|n0nce|1726300000|voltage"
	ct, err := EncryptOAEP(pub, []byte(pt))
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecryptOAEP(key, ct)
	if err != nil || string(back) != pt {
		t.Fatalf("oaep roundtrip: %v %q", err, back)
	}
	p, err := ParsePayload(back)
	if err != nil || p.Digest != d {
		t.Fatalf("parse: %v %+v", err, p)
	}
	// 篡改密文 → 解密失败
	ct[len(ct)-1] ^= 0xff
	if _, err := DecryptOAEP(key, ct); err == nil {
		t.Error("tampered ciphertext must fail")
	}
	// PSS：签名 → 验签；换 digest / 坏签名 / 别的公钥都失败
	sig, err := SignDigestPSS(key, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyDigestPSS(pub, d, sig); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := VerifyDigestPSS(pub, Digest("other", "m", "s", "x"), sig); err == nil {
		t.Error("digest mismatch must fail")
	}
	if err := VerifyDigestPSS(pub, d, "!!notbase64"); err == nil {
		t.Error("bad base64 must fail")
	}
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	if err := VerifyDigestPSS(&other.PublicKey, d, sig); err == nil {
		t.Error("wrong key must fail")
	}
	// PSS 是随机盐：两次签名不同但都有效
	sig2, _ := SignDigestPSS(key, d)
	if sig2 == sig {
		t.Error("PSS signatures should differ (random salt)")
	}
	if err := VerifyDigestPSS(pub, d, sig2); err != nil {
		t.Error(err)
	}
}

func TestLoadOrGenerateKey(t *testing.T) {
	k, gen, err := LoadOrGenerateKey(t.TempDir() + "/missing.pem")
	if err != nil || !gen || k == nil || k.N.BitLen() != 2048 {
		t.Fatalf("generate fallback: gen=%v err=%v", gen, err)
	}
	if _, err := ParsePrivateKeyPEM([]byte("garbage")); err == nil {
		t.Error("garbage PEM must fail")
	}
}
