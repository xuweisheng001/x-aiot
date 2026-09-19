package deviceapi

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"
)

// 开关三态（BL4 技术方案 §5.3 / 预推演 INC-4-06）：
// 隐私类布尔开关的显示必须以设备 reported 为准。客户端只渲染 state，不自己比较 reported 与 desired。
const (
	SwitchApplied = "applied" // reported == desired，value = desired
	SwitchPending = "pending" // 不一致且设备在线（或在线状态未知），value = reported
	SwitchOffline = "offline" // 不一致且设备离线，value = reported，上线后生效

	// OnlineWindow：影子 updated_at 在此窗口内视为在线（与 BL1 FR-05「断网 ≤ 90 s 显示离线」一致）。
	OnlineWindow = 90 * time.Second
)

// SwitchInfo 是响应里的一个开关。
type SwitchInfo struct {
	Value bool   `json:"value"`
	State string `json:"state"`
}

// ParseBoolLoose 解析 Redis 影子里的字符串布尔："true"/"1" → true，"false"/"0"/"" → false；ok=false 表示不是布尔。
func ParseBoolLoose(s string) (v, ok bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1":
		return true, true
	case "false", "0", "":
		return false, true
	}
	return false, false
}

// SwitchState 纯函数：reported 缺失（nil / 非布尔）视为 false。
//   - reported == desired → (desired, applied)
//   - 不一致且 online     → (reported, pending)
//   - 不一致且 !online    → (reported, offline)
func SwitchState(reported, desired any, online bool) (bool, string) {
	d := asBool(desired)
	r := asBool(reported)
	if r == d {
		return d, SwitchApplied
	}
	if online {
		return r, SwitchPending
	}
	return r, SwitchOffline
}

func asBool(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		b, _ := ParseBoolLoose(x)
		return b
	case float64:
		return x != 0
	case int:
		return x != 0
	}
	return false
}

// OnlineFromReported 用影子 updated_at（毫秒，pipeline 写入）判定在线；没有时间信息时 known=false。
func OnlineFromReported(reported map[string]string, now time.Time) (online, known bool) {
	raw, ok := reported["updated_at"]
	if !ok || raw == "" {
		return false, false
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return false, false
	}
	return now.Sub(time.UnixMilli(ms)) <= OnlineWindow, true
}

// BuildSwitches 对 desired 中每个布尔字段计算三态。在线状态未知时按 online=true 处理（显示 pending，宁可让用户等，不可让用户误以为已生效）。
func BuildSwitches(reported map[string]string, desired json.RawMessage, now time.Time) map[string]SwitchInfo {
	var d map[string]any
	if len(desired) > 0 {
		_ = json.Unmarshal(desired, &d)
	}
	online, known := OnlineFromReported(reported, now)
	if !known {
		online = true
	}
	out := map[string]SwitchInfo{}
	keys := make([]string, 0, len(d))
	for k, v := range d {
		if _, isBool := v.(bool); isBool {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		var rep any
		if s, ok := reported[k]; ok {
			rep = s
		}
		v, st := SwitchState(rep, d[k], online)
		out[k] = SwitchInfo{Value: v, State: st}
	}
	return out
}
