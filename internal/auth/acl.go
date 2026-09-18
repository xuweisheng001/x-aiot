// Package auth 实现 EMQX HTTP 认证/ACL 后端：SN 规则 + 设备状态 + 证书指纹 + 主题白名单。
package auth

import (
	"regexp"
	"strings"
)

var snRe = regexp.MustCompile(`^[A-Z0-9_-]{4,32}$`)

// ValidSN：^[A-Z0-9_-]{4,32}$。
func ValidSN(sn string) bool { return snRe.MatchString(sn) }

// Action 是 ACL 动作（EMQX 传 publish/subscribe，兼容 pub/sub 大小写不敏感）。
type Action int

const (
	ActionUnknown Action = iota
	ActionPublish
	ActionSubscribe
)

func ParseAction(s string) Action {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "publish", "pub":
		return ActionPublish
	case "subscribe", "sub":
		return ActionSubscribe
	}
	return ActionUnknown
}

// AllowTopic 是纯 ACL 决策：
//   - PUB 仅允许 up/{pk}/{sn}/... （sn==clientid，至少 4 段，不含通配符）
//   - SUB 仅允许 down/{sn}/#     （sn==clientid）
//   - 其余一律 deny。
func AllowTopic(clientID, topic string, action Action) bool {
	if !ValidSN(clientID) || topic == "" {
		return false
	}
	parts := strings.Split(topic, "/")
	switch action {
	case ActionPublish:
		if len(parts) < 4 || parts[0] != "up" || parts[1] == "" || parts[2] != clientID {
			return false
		}
		if strings.ContainsAny(topic, "+#") {
			return false
		}
		for _, p := range parts[3:] {
			if p == "" {
				return false
			}
		}
		return true
	case ActionSubscribe:
		return len(parts) == 3 && parts[0] == "down" && parts[1] == clientID && parts[2] == "#"
	}
	return false
}
