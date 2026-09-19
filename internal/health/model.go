// Package health 实现 BL3 耗材与材料：激光模块健康度小时级批计算、换模块识别、到期预测、
// 提醒判定与冷却、SKU 映射与下单归因、材料码防伪（docs/tech-design-bl3-consumables-materials.md §4 到 §9）。
//
// 三条不可协商约束：输入缺失不写 0 且有批级熔断；提醒走非安全通道与 alarm-svc 无共享路径；假码或可疑码不带出参数。
package health

import (
	"math"
	"sort"
	"time"
)

// Part 取值：consumable_health.part。
const (
	PartModule = "module"
	PartLens   = "lens"
)

// Cfg 是 health_model_cfg 的一行（§4.1 / §4.2）。
type Cfg struct {
	Rated           float64 // rated_weighted_hours
	WLow            float64
	WMid            float64
	WHigh           float64
	OvertempPenalty float64
	Version         int
}

// DefaultCfg 是 LM40 的种子配置（与 sql/bl3.sql 一致）。
func DefaultCfg() Cfg {
	return Cfg{Rated: 10000, WLow: 1.0, WMid: 1.3, WHigh: 1.8, OvertempPenalty: 0.5, Version: 1}
}

// 常量（§4.2 / §6 / §7）。
const (
	MaxPenalty          = 10.0
	MaxBucketHours      = 1.0  // Δhours > 1 h/桶视为上报异常，截断
	HoursResetRatio     = 0.05 // laser_hours 回落到 < 5% 上次值
	HoursResetBuckets   = 2    // 且持续 ≥ 2 个小时桶
	UserMarkWindow      = 24 * time.Hour
	ModuleHoursRatio    = 0.5 // 设备上报 module_hours < 50% 云端累计视为更换
	MinPredictDays      = 7.0
	EOLHealth           = 20.0 // 预测到「建议更换」档
	DefaultCooldown     = 7 * 24 * time.Hour
	DefaultMissingFuse  = 0.2
	SuspiciousThreshold = 5
	AttributionWindow   = 30 * 24 * time.Hour
)

// Levels 是提醒档位（跨档触发，取最低）。
var Levels = []int{80, 50, 20}

// HoursFlag 标记 WeightedHours 对异常输入的处理。
type HoursFlag string

const (
	FlagOK      HoursFlag = ""
	FlagRegress HoursFlag = "regress" // Δ<0：返回 0（未判定为更换时该桶跳过）
	FlagClipped HoursFlag = "clipped" // Δ>1 h：截断为 1
)

// WeightedHours 纯函数（§4.1）：weighted = Δ × (share_low·w_low + share_mid·w_mid + share_high·w_high)。
// share 三者之和不为 1 时按比例归一；三者全 0 视为未知 → 权重 1.0（调用方应先用 ShareFromAvgPower 兜底）。
func WeightedHours(deltaHours, shareLow, shareMid, shareHigh float64, cfg Cfg) (float64, HoursFlag) {
	flag := FlagOK
	switch {
	case deltaHours < 0 || math.IsNaN(deltaHours):
		return 0, FlagRegress
	case deltaHours > MaxBucketHours:
		deltaHours, flag = MaxBucketHours, FlagClipped
	}
	sum := shareLow + shareMid + shareHigh
	var w float64
	if sum <= 0 {
		w = 1.0
	} else {
		w = (shareLow*cfg.WLow + shareMid*cfg.WMid + shareHigh*cfg.WHigh) / sum
	}
	return deltaHours * w, flag
}

// ShareFromAvgPower 纯函数：老流没有 share_* 时，用小时平均功率落到的单档 = 1.0 兜底。
func ShareFromAvgPower(avgPower float64) (lo, mid, hi float64) {
	switch {
	case avgPower <= 50:
		return 1, 0, 0
	case avgPower <= 80:
		return 0, 1, 0
	default:
		return 0, 0, 1
	}
}

// Health 纯函数（§4.2）：base = 100 × (1 − used/rated)；penalty = min(p × overtemp, 10)；clamp 0..100。
// rated <= 0 时返回 (0, 0)，调用方必须先用 ComputeHealth 做缺失判定，不得把这个 0 写库。
func Health(usedWeighted, rated float64, overtempCount int, cfg Cfg) (health, penalty float64) {
	if rated <= 0 {
		return 0, 0
	}
	if overtempCount < 0 {
		overtempCount = 0
	}
	penalty = math.Min(cfg.OvertempPenalty*float64(overtempCount), MaxPenalty)
	base := 100 * (1 - usedWeighted/rated)
	health = clamp(base-penalty, 0, 100)
	return health, penalty
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

// Inputs 是一次健康度计算的全部输入；HasUsed=false 或 Rated<=0 表示关键输入缺失。
type Inputs struct {
	UsedWeighted  float64
	HasUsed       bool
	OvertempCount int
	Cfg           Cfg
	Rate30d       float64 // 近 30 天日均加权时长；<=0 不预测
	Now           time.Time
}

// ComputeHealth 纯函数：关键输入缺失 → ok=false，**不返回 0 分**（INC-3-02 根治的第一层）。
func ComputeHealth(in Inputs) (score float64, eol *time.Time, ok bool) {
	if !in.HasUsed || in.Cfg.Rated <= 0 {
		return 0, nil, false
	}
	score, _ = Health(in.UsedWeighted, in.Cfg.Rated, in.OvertempCount, in.Cfg)
	return score, PredictEOL(score, in.Cfg.Rated, in.Rate30d, in.Now), true
}

// Monotonic 纯函数（§4.2）：health 单调不增；next > prev 且非更换 → 保留 prev（kept=true）。
// prev < 0 表示首次计算，直接采用 next。
func Monotonic(prev, next float64, swapped bool) (value float64, kept bool) {
	if swapped || prev < 0 {
		return next, false
	}
	if next > prev {
		return prev, true
	}
	return next, false
}

// 换模块识别原因（module_swap.reason）。
const (
	SwapModelChange = "model_change"
	SwapHoursReset  = "hours_reset"
	SwapUserMarked  = "user_marked"
	SwapModuleHours = "module_hours"
)

// DetectSwap 纯函数（§6.1）。
//   - prevModel != curModel（两者非空）→ model_change
//   - hoursHistory[0] 是上次记录的 laser_hours，其后为新桶；≥2 个连续新桶 < 5% × 上次值 → hours_reset
//   - moduleHours 非空且 < 50% × 上次值（上次值 > 10）→ module_hours
//   - 用户 24 h 内标记且有单桶回落（弱信号）→ user_marked；仅标记无信号 → 不判定
func DetectSwap(prevModel, curModel string, hoursHistory []float64, userMarkedAt *time.Time, moduleHours *float64, now time.Time) (bool, string) {
	if prevModel != "" && curModel != "" && prevModel != curModel {
		return true, SwapModelChange
	}
	var last float64
	if len(hoursHistory) > 0 {
		last = hoursHistory[0]
	}
	if moduleHours != nil && last > 10 && *moduleHours < ModuleHoursRatio*last {
		return true, SwapModuleHours
	}
	drops := 0
	if last > 0 {
		for _, h := range hoursHistory[1:] {
			if h < HoursResetRatio*last {
				drops++
				if drops >= HoursResetBuckets {
					return true, SwapHoursReset
				}
			} else {
				drops = 0
			}
		}
	}
	if userMarkedAt != nil && now.Sub(*userMarkedAt) >= 0 && now.Sub(*userMarkedAt) <= UserMarkWindow && last > 0 {
		for _, h := range hoursHistory[1:] {
			if h < HoursResetRatio*last {
				return true, SwapUserMarked
			}
		}
	}
	return false, ""
}

// PredictEOL 纯函数（§6.2）：remaining = (health − 20)/100 × rated；eol = now + remaining/rate30d 天。
// rate30d <= 0 或 health <= 20 → nil（已到期或无法预测）。
func PredictEOL(health, rated, rate30d float64, now time.Time) *time.Time {
	if rate30d <= 0 || rated <= 0 || health <= EOLHealth {
		return nil
	}
	remaining := (health - EOLHealth) / 100 * rated
	days := remaining / rate30d
	if math.IsInf(days, 0) || math.IsNaN(days) || days > 365*20 {
		return nil
	}
	t := now.Add(time.Duration(days * 24 * float64(time.Hour)))
	return &t
}

// Rate30d 纯函数：近 30 天加权时长之和 / 天数；天数 < 7 不预测（返回 0）。
func Rate30d(sumWeighted float64, days float64) float64 {
	if days < MinPredictDays || sumWeighted <= 0 {
		return 0
	}
	return sumWeighted / days
}

// 提醒判定（§7.1）。
type ReminderAction string

const (
	ActionSend       ReminderAction = "send"
	ActionSuppressed ReminderAction = "suppressed"
	ActionDeferred   ReminderAction = "deferred"
	ActionNone       ReminderAction = "none"
)

const (
	ReasonCooldown = "cooldown"
	ReasonOptout   = "optout"
)

// ReminderInput 是判定输入。
type ReminderInput struct {
	HealthPrev float64 // 首次计算传 101（大于任何档）
	HealthNow  float64
	LevelsSent []int // 本模块生命周期已发档位
	LastSentAt *time.Time
	WorkState  int // 1 预热 / 2 作业 → deferred
	Optout     bool
	Now        time.Time
	Cooldown   time.Duration // <=0 用 DefaultCooldown
}

// ReminderDecision 是判定输出；Level 为触发档（多档跨越取最低），0 表示无。
type ReminderDecision struct {
	Action ReminderAction
	Level  int
	Reason string
}

// DecideReminder 纯函数：跨档 level ∈ {80,50,20} 且 prev > level ≥ now 且未发过；关闭 → suppressed(optout)；
// 冷却内 → suppressed(cooldown)（level 不进 levels_sent，下次仍可发）；作业中 → deferred；否则 send。
func DecideReminder(in ReminderInput) ReminderDecision {
	sent := map[int]bool{}
	for _, l := range in.LevelsSent {
		sent[l] = true
	}
	level := 0
	ls := append([]int(nil), Levels...)
	sort.Sort(sort.Reverse(sort.IntSlice(ls))) // 80,50,20
	for _, l := range ls {
		if in.HealthPrev > float64(l) && float64(l) >= in.HealthNow && !sent[l] {
			level = l // 继续循环取最低
		}
	}
	if level == 0 {
		return ReminderDecision{Action: ActionNone}
	}
	if in.Optout {
		return ReminderDecision{Action: ActionSuppressed, Level: level, Reason: ReasonOptout}
	}
	cd := in.Cooldown
	if cd <= 0 {
		cd = DefaultCooldown
	}
	if in.LastSentAt != nil && in.Now.Sub(*in.LastSentAt) < cd {
		return ReminderDecision{Action: ActionSuppressed, Level: level, Reason: ReasonCooldown}
	}
	if in.WorkState == 1 || in.WorkState == 2 {
		return ReminderDecision{Action: ActionDeferred, Level: level}
	}
	return ReminderDecision{Action: ActionSend, Level: level}
}

// ShouldFuseBatch 纯函数（§5.1 第 3 步）：skipped/total > ratio → 整轮不写。total<=0 不熔断。
func ShouldFuseBatch(skipped, total int, ratio float64) bool {
	if total <= 0 || skipped <= 0 {
		return false
	}
	return float64(skipped)/float64(total) > ratio
}

// SuspiciousByScans 纯函数（§9.2）：30 天内 distinct user > threshold → suspicious。
func SuspiciousByScans(distinctUsers, threshold int) bool {
	if threshold <= 0 {
		threshold = SuspiciousThreshold
	}
	return distinctUsers > threshold
}

// AttributionWindowOK 纯函数（§8.2）：paid_at 在 sent_at 之后且 ≤ window。
func AttributionWindowOK(sentAt, paidAt time.Time, window time.Duration) bool {
	if window <= 0 {
		window = AttributionWindow
	}
	d := paidAt.Sub(sentAt)
	return d >= 0 && d <= window
}

// StaleAfter 判定：updated_at 距 now 超过 after 为 stale。
func IsStale(updatedAt, now time.Time, after time.Duration) bool {
	return now.Sub(updatedAt) > after
}
