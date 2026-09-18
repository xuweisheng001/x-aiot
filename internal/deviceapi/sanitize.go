// Package deviceapi 提供影子 / desired / 指令 / 审计 / 遥测查询 HTTP 接口。
package deviceapi

import "strings"

// SanitizeSN 与 tdengine 包的子表名规则保持一致：小写，非 [a-z0-9_] → '_'。
func SanitizeSN(sn string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(sn) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			b.WriteRune(c)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}
