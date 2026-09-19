package reco

import "fmt"

// 动态校正（技术方案 §9.1）。与 param-svc 的在线版本同源：批处理只用它统计设备分布并做异常检测（INC-4-20），
// 在线返回给客户端的系数由 param-svc 负责。硬上下限是代码常量，不可配置。

const (
	KMin = 0.8
	KMax = 1.25
)

// CorrectionCfg 对应 iot_global.param_correction_cfg 一行。
type CorrectionCfg struct {
	A, B, C    float64
	RatedHours float64
}

// DefaultCorrectionCfg 是方案给的保守初始值。
func DefaultCorrectionCfg() CorrectionCfg {
	return CorrectionCfg{A: 0.15, B: 0.05, C: 0.05, RatedHours: 10000}
}

// Coefs 是校正输出：系数与可显示的原因。
type Coefs struct {
	KPower  float64
	KSpeed  float64
	Reasons []string
}

// Correction 纯函数：
//
//	k_power = clamp(1 + a × (1 − health/100) + b × laser_hours/rated_hours, 0.8, 1.25)
//	k_speed = clamp(1 − c × (1 − health/100), 0.8, 1.25)
//
// health / laserHours 任一为 nil（输入缺失）→ 返回 1 且 Reasons 说明「输入缺失，未校正」，绝不猜测。
func Correction(health, laserHours *float64, cfg CorrectionCfg) Coefs {
	if health == nil || laserHours == nil || cfg.RatedHours <= 0 {
		return Coefs{KPower: 1, KSpeed: 1, Reasons: []string{"inputs_missing_no_correction"}}
	}
	h := clamp(*health, 0, 100)
	wear := 1 - h/100
	kp := 1 + cfg.A*wear + cfg.B*(*laserHours/cfg.RatedHours)
	ks := 1 - cfg.C*wear
	out := Coefs{KPower: clamp(kp, KMin, KMax), KSpeed: clamp(ks, KMin, KMax)}
	if wear > 0 {
		out.Reasons = append(out.Reasons, fmt.Sprintf("health=%.0f", h))
	}
	if *laserHours > 0 {
		out.Reasons = append(out.Reasons, fmt.Sprintf("laser_hours=%.0f", *laserHours))
	}
	if out.KPower >= KMax {
		out.Reasons = append(out.Reasons, "power_at_cap_consider_replacing_module")
	}
	return out
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// CapShareAlarm 纯函数（INC-4-20 检测信号）：落在上限 KMax 的设备占比 > threshold 即告警。
func CapShareAlarm(ks []float64, threshold float64) (share float64, alarm bool) {
	if len(ks) == 0 {
		return 0, false
	}
	n := 0
	for _, k := range ks {
		if k >= KMax {
			n++
		}
	}
	share = float64(n) / float64(len(ks))
	return share, share > threshold
}
