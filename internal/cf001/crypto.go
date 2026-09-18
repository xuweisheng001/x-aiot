// Package cf001 实现代工机型激活体系：OAEP 认证载荷、配额事务、23 位 SN、PSS 自检签名。
package cf001

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// Digest = hex(SHA256(uuid + ":" + mcuSN + ":" + socSN + ":" + mac))。
// uuid 由设备内随机生成并参与哈希：供应商即使掌握 mcu/soc/mac 全量清单也无法离线反推出 digest，
// 从而无法用「算好的 digest」冒领配额。
func Digest(uuid, mcuSN, socSN, mac string) string {
	h := sha256.Sum256([]byte(uuid + ":" + mcuSN + ":" + socSN + ":" + mac))
	return hex.EncodeToString(h[:])
}

// Payload 是 OAEP 解密后的明文 `digest|nonce|timestamp`。
type Payload struct {
	Digest    string
	Nonce     string
	Timestamp int64
}

var ErrBadPayload = errors.New("bad payload")

// ParsePayload 解析三段载荷；多于三段时忽略第 4 段及以后（兼容需求文档里的 'voltage' 段）；
// 少于三段报错。digest 必须是 64 位十六进制（统一转小写）。
func ParsePayload(plaintext []byte) (Payload, error) {
	parts := strings.Split(string(plaintext), "|")
	if len(parts) < 3 {
		return Payload{}, fmt.Errorf("%w: want digest|nonce|timestamp, got %d segment(s)", ErrBadPayload, len(parts))
	}
	d := strings.ToLower(strings.TrimSpace(parts[0]))
	if len(d) != 64 {
		return Payload{}, fmt.Errorf("%w: digest must be 64 hex chars", ErrBadPayload)
	}
	if _, err := hex.DecodeString(d); err != nil {
		return Payload{}, fmt.Errorf("%w: digest not hex", ErrBadPayload)
	}
	nonce := strings.TrimSpace(parts[1])
	if nonce == "" {
		return Payload{}, fmt.Errorf("%w: empty nonce", ErrBadPayload)
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
	if err != nil {
		return Payload{}, fmt.Errorf("%w: timestamp not integer", ErrBadPayload)
	}
	return Payload{Digest: d, Nonce: nonce, Timestamp: ts}, nil
}

// LoadOrGenerateKey 读取 PEM 私钥（PKCS#1 或 PKCS#8）；文件缺失时生成内存 2048 位密钥并告警（仅开发）。
func LoadOrGenerateKey(path string) (key *rsa.PrivateKey, generated bool, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, false, err
		}
		slog.Warn("CF001 private key not found; generating ephemeral in-memory key (DEV ONLY, run `make keys`)", "path", path)
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, false, err
		}
		return k, true, nil
	}
	k, err := ParsePrivateKeyPEM(b)
	if err != nil {
		return nil, false, fmt.Errorf("parse %s: %w", path, err)
	}
	return k, false, nil
}

func ParsePrivateKeyPEM(b []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, errors.New("no PEM block")
	}
	switch block.Type {
	case "RSA PRIVATE KEY":
		return x509.ParsePKCS1PrivateKey(block.Bytes)
	case "PRIVATE KEY":
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("not an RSA key")
		}
		return rk, nil
	}
	return nil, fmt.Errorf("unsupported PEM type %q", block.Type)
}

// PublicKeyPEM 输出 PKIX "PUBLIC KEY" PEM，供设备/测试加密与验签。
func PublicKeyPEM(pub *rsa.PublicKey) ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}

func ParsePublicKeyPEM(b []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, errors.New("no PEM block")
	}
	k, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := k.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("not an RSA public key")
	}
	return pub, nil
}

// EncryptOAEP：设备侧（或测试）用云公钥加密认证载荷。
func EncryptOAEP(pub *rsa.PublicKey, plaintext []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plaintext, nil)
}

// DecryptOAEP：云端用私钥解密。
func DecryptOAEP(priv *rsa.PrivateKey, ciphertext []byte) ([]byte, error) {
	return rsa.DecryptOAEP(sha256.New(), nil, priv, ciphertext, nil)
}

// SignDigestPSS 对 digest 字符串的字节做 RSA-PSS(SHA-256, salt=hash len) 签名，返回 base64。
func SignDigestPSS(priv *rsa.PrivateKey, digest string) (string, error) {
	h := sha256.Sum256([]byte(digest))
	sig, err := rsa.SignPSS(rand.Reader, priv, crypto.SHA256, h[:], &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}

// VerifyDigestPSS 用公钥验证 base64 签名。
func VerifyDigestPSS(pub *rsa.PublicKey, digest, sigB64 string) error {
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("signature not base64: %w", err)
	}
	h := sha256.Sum256([]byte(digest))
	return rsa.VerifyPSS(pub, crypto.SHA256, h[:], sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
}
