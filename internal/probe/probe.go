// Package probe 是安全事件合成探针（docs/incident-premortem-bl1.md INC-10 根治项之一）。
//
// 为什么需要它：火焰事件漏告警是全系统最不能发生的事故，而它的链路有七跳
// （设备 → EMQX → bridge → JetStream → alarm-svc → PG → 推送），任何一跳静默失效
// 都表现为「什么都没发生」。对账任务能发现「事件有、告警无」，但对账本身依赖 TDengine
// 与 PG 都正常；探针则从最外侧主动打一发真实事件，量出端到端时间，是最后一道检测。
//
// 三条设计约束：
//  1. 走真实链路而不是调内部函数，否则探针只能证明代码没坏，不能证明系统没坏。
//  2. 探针必须清理自己造的告警，否则运营的告警列表里每 10 分钟多一条 FLAME_DETECTED，
//     真告警会被淹没——探针制造告警疲劳就本末倒置了。
//  3. 探针间隔必须大于 alarm-svc 的 squelch 窗口，否则第二发被聚合吞掉，
//     探针会把「正常聚合」误报成「链路故障」。
package probe

import (
	"fmt"
	"time"
)

// Status 是一轮探测的判定结果。
type Status string

const (
	StatusOK      Status = "ok"      // 端到端在 SLO 内
	StatusSlow    Status = "slow"    // 到达了但超过 SLO
	StatusMissing Status = "missing" // 超时未到达：S1
	StatusError   Status = "error"   // 探针自身出错（发不出去 / 查不了库），不代表链路坏
)

// DefaultSLO 与技术方案 §2 的安全事件端到端 P99 < 3 s 同口径。
const DefaultSLO = 3 * time.Second

// Verdict 纯函数：把一次探测的观测结果翻译成判定。
//
//   - found=false → missing（不看 latency）
//   - latency <= slo → ok
//   - 否则 slow
//
// slo <= 0 时用 DefaultSLO。latency 为负（时钟回拨或设备时间超前）按 0 处理：
// 探针的价值是发现「太慢」，不该因为时钟问题误报成 ok 之外的状态。
func Verdict(found bool, latency, slo time.Duration) (Status, string) {
	if slo <= 0 {
		slo = DefaultSLO
	}
	if !found {
		return StatusMissing, fmt.Sprintf("no alarm notified within %s", slo)
	}
	if latency < 0 {
		latency = 0
	}
	if latency <= slo {
		return StatusOK, ""
	}
	return StatusSlow, fmt.Sprintf("notified in %s, over SLO %s", latency.Truncate(time.Millisecond), slo)
}

// IntervalSane 纯函数：探针间隔必须大于 squelch 窗口，否则连续两发被聚合成一条告警，
// 第二发查不到新告警会被误判为 missing。返回 ok=false 时调用方应拒绝启动或至少 WARN。
func IntervalSane(interval, squelch time.Duration) (bool, string) {
	if interval <= 0 {
		return false, "interval must be positive"
	}
	if squelch <= 0 {
		return true, ""
	}
	if interval <= squelch {
		return false, fmt.Sprintf("interval %s must exceed alarm squelch window %s, "+
			"otherwise the second probe is aggregated away and looks like a missing alarm", interval, squelch)
	}
	return true, ""
}

// Result 是一轮探测的完整记录。
type Result struct {
	Status    Status        `json:"status"`
	Reason    string        `json:"reason,omitempty"`
	SN        string        `json:"sn"`
	Code      string        `json:"code"`
	AlarmID   int64         `json:"alarm_id,omitempty"`
	EventTs   time.Time     `json:"event_ts"`
	NotifiedA *time.Time    `json:"notified_at,omitempty"`
	Latency   time.Duration `json:"latency_ms"`
	CleanedUp bool          `json:"cleaned_up"`
}

// Fatal 报告这轮结果是否要当 S1 处理（漏告警）。slow 是 S2，error 是探针自身问题。
func (r Result) Fatal() bool { return r.Status == StatusMissing }
