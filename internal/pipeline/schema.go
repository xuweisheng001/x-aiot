package pipeline

import "strings"

// SplitStatements 把 sql/tdengine.sql 按 ';' 切成语句，去掉 "--" 行注释与空语句（纯函数）。
func SplitStatements(sql string) []string {
	var lines []string
	for _, l := range strings.Split(sql, "\n") {
		if i := strings.Index(l, "--"); i >= 0 {
			l = l[:i]
		}
		lines = append(lines, l)
	}
	var out []string
	for _, s := range strings.Split(strings.Join(lines, "\n"), ";") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
