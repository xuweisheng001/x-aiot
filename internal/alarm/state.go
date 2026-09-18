// Package alarm 实现安全事件告警：独立 durable consumer、squelch、状态机与升级。
//
// 状态机 open→notified→acked→closed 全部用「带前置状态的 UPDATE」落地，
// affected=0 即非法迁移；Allowed 是这张迁移表的纯函数形态，SQL 的 IN 列表由 Sources 生成，
// 保证代码里的表与数据库里的守卫是同一份真相。
package alarm

import "sort"

const (
	StatusOpen     = "open"
	StatusNotified = "notified"
	StatusAcked    = "acked"
	StatusClosed   = "closed"
)

// transitions[from] = 允许迁往的目标状态集合。
var transitions = map[string]map[string]bool{
	StatusOpen:     {StatusNotified: true, StatusAcked: true}, // 推送失败也允许人工直接 ack
	StatusNotified: {StatusAcked: true},
	StatusAcked:    {StatusClosed: true},
	StatusClosed:   {},
}

// Allowed 报告 from→to 是否为合法迁移。未知状态一律不允许。
func Allowed(from, to string) bool {
	tos, ok := transitions[from]
	if !ok {
		return false
	}
	return tos[to]
}

// Sources 返回所有可迁往 to 的前置状态（排序稳定），用于生成 `WHERE status = ANY($n)`。
func Sources(to string) []string {
	var out []string
	for from, tos := range transitions {
		if tos[to] {
			out = append(out, from)
		}
	}
	sort.Strings(out)
	return out
}
