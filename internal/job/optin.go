package job

import (
	"context"
	"strings"
)

// OptInField 是影子 reported 中的反哺开关字段名（物模型 v1.1，desired 下发、设备回报）。
const OptInField = "job_feedback_optin"

// OptInState 是 opt-in 判定四态。只有 OptInAllowed 才允许落库；其余全部丢弃（fail-closed）：
// 合规校验与可用性兜底方向相反——查不到答案时不能当作同意。
type OptInState int

const (
	OptInAllowed OptInState = iota
	OptInFalse
	OptInMissing
	OptInErr
)

func (s OptInState) String() string {
	switch s {
	case OptInAllowed:
		return "allowed"
	case OptInFalse:
		return "false"
	case OptInMissing:
		return "missing"
	}
	return "err"
}

// Metric 返回该状态对应的丢弃计数名（Allowed 返回空串）。
func (s OptInState) Metric() string {
	switch s {
	case OptInFalse:
		return "dropped_optin_false"
	case OptInMissing:
		return "dropped_optin_missing"
	case OptInErr:
		return "dropped_optin_err"
	}
	return ""
}

// ParseBool 兼容影子中布尔的几种编码：go-redis 写 bool 得到 "1"/"0"，JSON 透传得到 "true"/"false"。
func ParseBool(v string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes":
		return true, true
	case "0", "false", "f", "no", "":
		return false, true
	}
	return false, false
}

// DecideOptIn 纯函数：根据影子读取结果判定。err 非空 → OptInErr；字段缺失或无法解析 → OptInMissing；
// 解析为 false → OptInFalse；true → OptInAllowed。
func DecideOptIn(reported map[string]string, err error) OptInState {
	if err != nil {
		return OptInErr
	}
	v, ok := reported[OptInField]
	if !ok {
		return OptInMissing
	}
	b, ok := ParseBool(v)
	if !ok {
		return OptInMissing
	}
	if !b {
		return OptInFalse
	}
	return OptInAllowed
}

// ShadowReader 是本包依赖的影子读取子集（shadow.Read 的包装满足）。
type ShadowReader interface {
	Read(ctx context.Context, sn string) (map[string]string, error)
}

// ShadowReaderFunc 适配函数为 ShadowReader。
type ShadowReaderFunc func(ctx context.Context, sn string) (map[string]string, error)

func (f ShadowReaderFunc) Read(ctx context.Context, sn string) (map[string]string, error) {
	return f(ctx, sn)
}
