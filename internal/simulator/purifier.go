package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// -accessory 模式：把虚拟设备当成净化器（BL2）。只上报净化器属性、响应 desired {power_on, fan_level}，
// 不跑作业周期、不发 JOB 事件；本地兜底（收到主机局域网信号自动启停）不在模拟范围，云端联动可被观察。

// PurifierState 是净化器的运行态。
type PurifierState struct {
	PowerOn       bool
	FanLevel      int    // 1..4；关机时保留上次档位便于回报
	RuntimeS      int64  // 累计运行秒
	TriggerSource string // cloud | local | ""
}

const (
	PurifierMinLevel = 1
	PurifierMaxLevel = 4
	PurifierRPMStep  = 800 // 档位 × 800 rpm，与 filter_model.rpm_levels 阈值一致
)

// ClampLevel 纯函数：档位限到 1..4。
func ClampLevel(lv int) int {
	if lv < PurifierMinLevel {
		return PurifierMinLevel
	}
	if lv > PurifierMaxLevel {
		return PurifierMaxLevel
	}
	return lv
}

// PurifierRPM 纯函数：关机 0；开机 档位×800。
func PurifierRPM(st PurifierState) int {
	if !st.PowerOn {
		return 0
	}
	return ClampLevel(st.FanLevel) * PurifierRPMStep
}

// ApplyPurifierDesired 纯函数：desired 里的 power_on / fan_level 覆盖状态，返回是否有变化。
// 容错：bool 也接受 "true"/"1"/1；fan_level 接受数字或数字字符串；非法值忽略。
func ApplyPurifierDesired(st PurifierState, desired map[string]any) (PurifierState, bool) {
	changed := false
	if v, ok := desired["power_on"]; ok {
		if b, ok := looseBool(v); ok && b != st.PowerOn {
			st.PowerOn = b
			changed = true
		}
	}
	if v, ok := desired["fan_level"]; ok {
		if n, ok := looseInt(v); ok && n >= PurifierMinLevel && n <= PurifierMaxLevel && n != st.FanLevel {
			st.FanLevel = n
			changed = true
		}
	}
	if changed {
		st.TriggerSource = "cloud"
	}
	return st, changed
}

func looseBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case float64:
		return x != 0, true
	case string:
		switch x {
		case "true", "1":
			return true, true
		case "false", "0":
			return false, true
		}
	}
	return false, false
}

func looseInt(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case int:
		return x, true
	case string:
		n, err := strconv.Atoi(x)
		return n, err == nil
	}
	return 0, false
}

// BuildPurifierTelemetry 纯函数：净化器遥测帧。work_state 2 表示运转、0 待机；fan_rpm 进 TDengine 列，
// power_on / fan_level / pressure_diff / runtime_h / trigger_source 作为未建模标量透传进影子 reported。
func BuildPurifierTelemetry(seq, ts int64, st PurifierState, jitter float64) Telemetry {
	on := st.PowerOn
	lv := ClampLevel(st.FanLevel)
	pressure := 40 + float64(st.RuntimeS)/36000 + jitter*2 // 随累计运行缓慢上升（滤芯堵塞）
	runtimeH := float64(st.RuntimeS) / 3600
	t := Telemetry{Seq: seq, Ts: ts, WorkState: 0, TempCavity: 24 + jitter, FanRPM: PurifierRPM(st),
		PowerOn: &on, FanLevel: &lv, PressureDiff: &pressure, RuntimeH: &runtimeH}
	if on {
		t.WorkState = 2
	}
	if st.TriggerSource != "" {
		t.TriggerSource = st.TriggerSource
	}
	return t
}

// Advance 纯函数：一个遥测周期过去，开机则累计运行秒。
func (st PurifierState) Advance(period time.Duration) PurifierState {
	if st.PowerOn {
		st.RuntimeS += int64(period / time.Second)
	}
	return st
}

// PairAccessories 通过 accessory-svc 把 n 台配件配到主机（需双方设备行已存在，即设备已完成一次 bootstrap）。
// 返回成功配对数；单台失败只记错，继续下一台。
func PairAccessories(ctx context.Context, baseURL, host, prefix string, n int) (int, []error) {
	cli := &http.Client{Timeout: 5 * time.Second}
	ok := 0
	var errs []error
	for i := 1; i <= n; i++ {
		acc := SNFor(prefix, i)
		body, _ := json.Marshal(map[string]any{"host_sn": host, "acc_sn": acc, "acc_type": "purifier"})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/v1/pairings", bytes.NewReader(body))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := cli.Do(req)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", acc, err))
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			errs = append(errs, fmt.Errorf("%s: status %d", acc, resp.StatusCode))
			continue
		}
		ok++
	}
	return ok, errs
}
