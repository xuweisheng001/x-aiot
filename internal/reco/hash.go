// Package reco 是 BL4 推荐离线批（技术方案 §9）：候选生成、功率上限、安全关联下线、校正系数分布。
// 所有判定抽成纯函数并表驱动测试；SQL / Redis / TDengine 访问集中在 runner.go。
package reco

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Canonicalize 把参数 JSON 规范化为「键按字典序、无空白、数字保持原文」的紧凑形式。
// 客户端与服务端用同一规则算 params_hash（PRD §6.2 / 技术方案 §6.3），规则公开可验证。
func Canonicalize(raw json.RawMessage) (string, error) {
	var v any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return "", fmt.Errorf("canonicalize: %w", err)
	}
	var sb strings.Builder
	if err := writeCanonical(&sb, v); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func writeCanonical(sb *strings.Builder, v any) error {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			kb, _ := json.Marshal(k)
			sb.Write(kb)
			sb.WriteByte(':')
			if err := writeCanonical(sb, x[k]); err != nil {
				return err
			}
		}
		sb.WriteByte('}')
	case []any:
		sb.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				sb.WriteByte(',')
			}
			if err := writeCanonical(sb, e); err != nil {
				return err
			}
		}
		sb.WriteByte(']')
	case json.Number:
		sb.WriteString(x.String())
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return err
		}
		sb.Write(b)
	}
	return nil
}

// CanonicalHash = hex(sha256(Canonicalize(params)))，64 位小写。无法解析时返回空串。
func CanonicalHash(raw json.RawMessage) string {
	c, err := Canonicalize(raw)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256([]byte(c))
	return hex.EncodeToString(sum[:])
}
