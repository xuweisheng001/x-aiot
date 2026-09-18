package ota

// 任务状态 = 设备上报 phase（pending/notified 由云端写入）。
const (
	TaskPending     = "pending"
	TaskNotified    = "notified"
	TaskDownloading = "downloading"
	TaskVerifying   = "verifying"
	TaskSuccess     = "success"
	TaskFailed      = "failed"
	TaskRolledBack  = "rolled_back"
)

const (
	BatchRunning   = "running"
	BatchPaused    = "paused"
	BatchFused     = "fused"
	BatchCompleted = "completed"
)

var terminal = map[string]bool{TaskSuccess: true, TaskFailed: true, TaskRolledBack: true}
var validPhase = map[string]bool{
	TaskNotified: true, TaskDownloading: true, TaskVerifying: true,
	TaskSuccess: true, TaskFailed: true, TaskRolledBack: true,
}

// IsTerminal 报告 phase 是否为终态（终态之间不再迁移，重复终态上报不二次计数）。
func IsTerminal(phase string) bool { return terminal[phase] }

// ValidPhase 报告设备上报的 phase 是否在契约集合内。
func ValidPhase(phase string) bool { return validPhase[phase] }

// TerminalStates 供 SQL `status NOT IN (...)` 使用。
func TerminalStates() []string { return []string{TaskSuccess, TaskFailed, TaskRolledBack} }
