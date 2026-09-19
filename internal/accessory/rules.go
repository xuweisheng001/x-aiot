// Package accessory 是 BL2 配件与安全生态：配对、云端联动规则引擎、待关闭定时器、配件告警关联主机、滤芯寿命批。
//
// 两条不可协商的约束（技术方案 §6）：
//  1. 本地兜底在固件；云端只做增强与留痕。规则引擎 bug 不可能导致作业中无排烟，因此下发失败不 Nak（重放没有意义）。
//  2. 关闭比开启谨慎：云端「关」只是建议（固件仲裁），且受全局开关 IOT_ACC_ALLOW_OFF 与过期事件丢弃约束。
package accessory

import (
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// Pairing 是 accessory_pairing 的一行。
type Pairing struct {
	ID             int64      `json:"id"`
	HostSN         string     `json:"host_sn"`
	AccSN          string     `json:"acc_sn"`
	AccType        string     `json:"acc_type"`
	LinkageEnabled bool       `json:"linkage_enabled"`
	OffDelayS      int        `json:"off_delay_s"`
	PairedAt       time.Time  `json:"paired_at"`
	UnpairedAt     *time.Time `json:"unpaired_at,omitempty"`
}

// Trigger 是规则表的「触发」列。
type Trigger string

const (
	TrigJobStart   Trigger = "JOB_START"
	TrigJobEnd     Trigger = "JOB_END"     // JOB_DONE / JOB_FAIL
	TrigWorkStart  Trigger = "WORK_START"  // 遥测 work_state → 2（兜住无 JOB 元数据的老固件）
	TrigWorkEnd    Trigger = "WORK_END"    // 遥测 work_state → 0
	TrigHostSafety Trigger = "HOST_SAFETY" // 主机安全码 → 排烟优先
	TrigFire       Trigger = "FIRE_SUPPRESSED"
	TrigSmoke      Trigger = "SMOKE_HIGH"
	TrigOffTimer   Trigger = "OFF_TIMER" // 定时器到期
	TrigReconcile  Trigger = "RECONCILE" // 对账兜底
	TrigPair       Trigger = "PAIR"      // 配对时下发 paired_sn
)

// Event 是规则引擎的归一化输入（主机事件 / 主机遥测 / 配件事件）。
type Event struct {
	SN          string
	IsTelemetry bool
	Code        string // event 的 code
	MaterialID  string // JOB_START 携带
	WorkState   int    // telemetry 的 work_state
	Seq         int64
	Ts          int64 // 设备时间 ms
	RecvTs      int64 // 桥接接收 ms
}

// DesiredState 是净化器的目标状态（幂等比较用）。
type DesiredState struct {
	PowerOn  bool `json:"power_on"`
	FanLevel int  `json:"fan_level"`
}

// ActionType 是规则输出的动作类型。
type ActionType string

const (
	ActSetDesired  ActionType = "set_desired"  // desired {power_on, fan_level}
	ActScheduleOff ActionType = "schedule_off" // off_delay 后建议关闭
	ActCancelOff   ActionType = "cancel_off"
	ActStopHost    ActionType = "stop_host"   // 主机 POST /cmd stop（灭火第二道）
	ActSafetyVent  ActionType = "safety_vent" // 标记 10 min 安全排烟窗口
)

// Action 是一条待执行动作。
type Action struct {
	Type    ActionType    `json:"type"`
	AccSN   string        `json:"acc_sn,omitempty"`
	HostSN  string        `json:"host_sn,omitempty"`
	Desired DesiredState  `json:"desired,omitempty"`
	Delay   time.Duration `json:"delay,omitempty"`
	Trigger Trigger       `json:"trigger"`
}

// 规则常量（技术方案 §5.2 / §5.3、预推演 §9）。
const (
	DefaultFanLevel          = 2
	MaxFanLevel              = 4
	SafetyVentWindow         = 10 * time.Minute
	JobStartDebounce         = 10 * time.Second // work_state→2 在 JOB_START 后 10 s 内不重复联动
	MaxEventAge              = 60 * time.Second // 过期事件不联动（INC-2-01）
	DefaultOffDelay          = 180 * time.Second
	MinOffDelay, MaxOffDelay = 60 * time.Second, 600 * time.Second
	WorkStateUnknown         = -1
)

// EngineState 是 Decide 需要的上下文快照；全部由引擎在调用前填好，Decide 本身无副作用。
type EngineState struct {
	Now            time.Time
	AllowOff       bool // IOT_ACC_ALLOW_OFF 全局开关（INC-2-03）
	StopHostOnFire bool // IOT_ACC_STOP_HOST_ON_FIRE

	PrevWorkState   map[string]int          // host → 上一次已知 work_state；缺失视为未知，不联动
	LastJobStart    map[string]time.Time    // host → 最近一次已处理 JOB_START
	SafetyVentUntil map[string]time.Time    // acc → 安全排烟窗口截止
	Current         map[string]DesiredState // acc → 最近已下发/已回报的目标状态（幂等）
	HostWorkState   map[string]int          // host → 影子 work_state（灭火停机的第二证据）；缺失视为未知

	Rules        map[string]int // material_id → fan_level
	DefaultLevel int
}

// LevelFor 纯函数：材料 → 档位；无映射用 def；def 非法时用 DefaultFanLevel。
func LevelFor(rules map[string]int, materialID string, def int) int {
	if def < 1 || def > MaxFanLevel {
		def = DefaultFanLevel
	}
	if lv, ok := rules[materialID]; ok && lv >= 1 && lv <= MaxFanLevel {
		return lv
	}
	return def
}

// IsStale 纯函数：桥接接收时间距 now 超过 max 的事件不联动（现在下发的建议对应的是早已结束的状态）。
func IsStale(recvTs int64, now time.Time, max time.Duration) bool {
	if recvTs <= 0 {
		return false // 无接收时间信息时不因此丢弃
	}
	age := now.Sub(time.UnixMilli(recvTs))
	return age > max
}

// ShouldCancelOff 纯函数：在关闭建议排定之后又来了作业开始，则取消关闭。
func ShouldCancelOff(lastStart, scheduledAt time.Time) bool {
	return !lastStart.IsZero() && !lastStart.Before(scheduledAt)
}

// HostStopEvidence 纯函数：灭火事件停主机的「双证据」——配件报 FIRE_SUPPRESSED 且主机不处于空闲。
// 主机状态未知（-1）时按需要停机处理：停一台可能并未作业的机器代价远小于漏停。
func HostStopEvidence(code string, hostWorkState int) bool {
	return code == string(TrigFire) && hostWorkState != 0
}

// IsHostSafetyCode：主机安全码（SafetyCodes 去掉配件码）。
func IsHostSafetyCode(code string) bool {
	return envelope.SafetyCodes[code] && !envelope.AccessorySafetyCodes[code]
}

// ClampOffDelay 把配对里的 off_delay_s 限到 [60,600] 秒。
func ClampOffDelay(sec int) time.Duration {
	d := time.Duration(sec) * time.Second
	if d < MinOffDelay {
		return MinOffDelay
	}
	if d > MaxOffDelay {
		return MaxOffDelay
	}
	return d
}

// Decide 是规则表的代码形态（技术方案 §5.2）。
//
//   - pairings：该事件涉及的主机的全部有效配对（配件事件时引擎先由 acc 找到主机再取其全部配对）。
//   - 返回按执行顺序排列的动作；空切片表示不联动。
//
// 规则引擎只输出「目标状态」，不理解 MQTT / HTTP；幂等（目标状态未变不下发）也在这里判定。
func Decide(evt Event, pairings []Pairing, st EngineState) []Action {
	if len(pairings) == 0 {
		return nil
	}
	if IsStale(evt.RecvTs, st.Now, MaxEventAge) {
		return nil
	}
	host := pairings[0].HostSN
	self := findAcc(pairings, evt.SN) // 事件来自配件时非 nil

	switch {
	// ---- 配件安全事件 ----
	case self != nil && evt.Code == string(TrigFire):
		var out []Action
		if st.StopHostOnFire && HostStopEvidence(evt.Code, hostState(st, host)) {
			out = append(out, Action{Type: ActStopHost, HostSN: host, AccSN: self.AccSN, Trigger: TrigFire})
		}
		for _, p := range pairings {
			if p.AccSN == self.AccSN {
				continue
			}
			out = append(out, ventActions(p, st, TrigFire)...)
		}
		return out
	case self != nil && evt.Code == string(TrigSmoke):
		return ventActions(*self, st, TrigSmoke)
	case self != nil:
		return nil // 配件的其它事件 / 遥测不驱动联动

	// ---- 主机安全事件：排烟优先，不看 linkage_enabled ----
	case !evt.IsTelemetry && IsHostSafetyCode(evt.Code):
		var out []Action
		for _, p := range pairings {
			out = append(out, ventActions(p, st, TrigHostSafety)...)
		}
		return out

	// ---- 主机作业开始 ----
	case !evt.IsTelemetry && evt.Code == "JOB_START":
		return startActions(pairings, st, LevelFor(st.Rules, evt.MaterialID, st.DefaultLevel), TrigJobStart)
	case evt.IsTelemetry:
		prev, known := st.PrevWorkState[host]
		if !known || prev == evt.WorkState {
			return nil // 去抖：只处理 work_state 变化（INC-2-05）
		}
		switch evt.WorkState {
		case 2:
			if ls, ok := st.LastJobStart[host]; ok && st.Now.Sub(ls) < JobStartDebounce {
				return nil // JOB_START 已联动
			}
			return startActions(pairings, st, LevelFor(st.Rules, "", st.DefaultLevel), TrigWorkStart)
		case 0:
			return endActions(pairings, st, TrigWorkEnd)
		}
		return nil

	// ---- 主机作业结束 ----
	case !evt.IsTelemetry && (evt.Code == "JOB_DONE" || evt.Code == "JOB_FAIL"):
		return endActions(pairings, st, TrigJobEnd)
	}
	return nil
}

func findAcc(pairings []Pairing, sn string) *Pairing {
	for i := range pairings {
		if pairings[i].AccSN == sn {
			return &pairings[i]
		}
	}
	return nil
}

func hostState(st EngineState, host string) int {
	if ws, ok := st.HostWorkState[host]; ok {
		return ws
	}
	return WorkStateUnknown
}

// startActions：取消待关闭 + 开机到目标档位；安全排烟窗口内档位强制 4。
func startActions(pairings []Pairing, st EngineState, level int, trig Trigger) []Action {
	var out []Action
	for _, p := range pairings {
		if !p.LinkageEnabled {
			continue
		}
		lv := level
		if until, ok := st.SafetyVentUntil[p.AccSN]; ok && st.Now.Before(until) {
			lv = MaxFanLevel
		}
		out = append(out, Action{Type: ActCancelOff, AccSN: p.AccSN, HostSN: p.HostSN, Trigger: trig})
		want := DesiredState{PowerOn: true, FanLevel: lv}
		if cur, ok := st.Current[p.AccSN]; ok && cur == want {
			continue // 幂等：目标状态未变不下发
		}
		out = append(out, Action{Type: ActSetDesired, AccSN: p.AccSN, HostSN: p.HostSN, Desired: want, Trigger: trig})
	}
	return out
}

// endActions：排定 off_delay 后的关闭建议；安全排烟窗口未结束则延到窗口结束；AllowOff=false 时不排定。
func endActions(pairings []Pairing, st EngineState, trig Trigger) []Action {
	if !st.AllowOff {
		return nil
	}
	var out []Action
	for _, p := range pairings {
		if !p.LinkageEnabled {
			continue
		}
		delay := ClampOffDelay(p.OffDelayS)
		if until, ok := st.SafetyVentUntil[p.AccSN]; ok && until.Sub(st.Now) > delay {
			delay = until.Sub(st.Now)
		}
		out = append(out, Action{Type: ActScheduleOff, AccSN: p.AccSN, HostSN: p.HostSN, Delay: delay, Trigger: trig})
	}
	return out
}

// ventActions：安全排烟——取消待关闭、最大档开机、标记 10 min 窗口；不看 linkage_enabled。
func ventActions(p Pairing, st EngineState, trig Trigger) []Action {
	out := []Action{
		{Type: ActCancelOff, AccSN: p.AccSN, HostSN: p.HostSN, Trigger: trig},
		{Type: ActSafetyVent, AccSN: p.AccSN, HostSN: p.HostSN, Delay: SafetyVentWindow, Trigger: trig},
	}
	want := DesiredState{PowerOn: true, FanLevel: MaxFanLevel}
	if cur, ok := st.Current[p.AccSN]; !ok || cur != want {
		out = append(out, Action{Type: ActSetDesired, AccSN: p.AccSN, HostSN: p.HostSN, Desired: want, Trigger: trig})
	}
	return out
}

// HostView / AccView 是对账用的影子视图。
type HostView struct {
	WorkState int // -1 未知
}

type AccView struct {
	Known         bool
	PowerOn       bool
	FanLevel      int
	TriggerSource string
}

// ReconcileDecide 纯函数（预推演 INC-2-03 / INC-2-04 的对账兜底）：
//   - 主机作业中而联动开启的净化器已回报关机 → 立即开机（记 off_while_working）；
//   - 主机空闲、净化器由云端开着且没有待关闭定时器 → 排定关闭（记 long_on）。
func ReconcileDecide(p Pairing, host HostView, acc AccView, hasTimer bool, st EngineState) (actions []Action, findings []string) {
	if !p.LinkageEnabled || !acc.Known {
		return nil, nil
	}
	switch {
	case host.WorkState == 2 && !acc.PowerOn:
		findings = append(findings, "off_while_working")
		actions = append(actions, Action{Type: ActSetDesired, AccSN: p.AccSN, HostSN: p.HostSN,
			Desired: DesiredState{PowerOn: true, FanLevel: LevelFor(st.Rules, "", st.DefaultLevel)}, Trigger: TrigReconcile})
	case host.WorkState == 0 && acc.PowerOn && acc.TriggerSource == "cloud" && !hasTimer && st.AllowOff:
		findings = append(findings, "long_on")
		actions = append(actions, Action{Type: ActScheduleOff, AccSN: p.AccSN, HostSN: p.HostSN, Delay: ClampOffDelay(p.OffDelayS), Trigger: TrigReconcile})
	}
	return actions, findings
}

// OwnersConsistent 纯函数：配对双方的 owner 集合有交集才允许配对；任一方无绑定信息（如模拟器）时无法校验，放行由 BFF 兜底。
func OwnersConsistent(hostOwners, accOwners []int64) bool {
	if len(hostOwners) == 0 || len(accOwners) == 0 {
		return true
	}
	set := map[int64]bool{}
	for _, u := range hostOwners {
		set[u] = true
	}
	for _, u := range accOwners {
		if set[u] {
			return true
		}
	}
	return false
}
