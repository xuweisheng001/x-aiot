// Package job 实现 BL4 加工记录与反哺链路（技术方案 §8）：
// 独立 durable consumer 消费 JOB_* 事件 → opt-in fail-closed → 字段格式白名单 → job_record；
// 结果标记接口（job_record 未到达时暂存）；撤回 opt-in 后以 desired 为触发删除。
package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

// JOB 事件码（物模型 v1.1 任务事件）。
const (
	CodeStart = "JOB_START"
	CodeDone  = "JOB_DONE"
	CodeFail  = "JOB_FAIL"
	CodePause = "JOB_PAUSE"
)

// 加工结局，对应 job_record.outcome（ck_job_outcome）。
const (
	OutcomeDone  = "done"
	OutcomeFail  = "fail"
	OutcomePause = "pause"
)

// 结果标记，对应 job_feedback.rating（ck_feedback_rating）。
var Ratings = map[string]bool{"good": true, "burnt": true, "uncut": true}

// IsJobCode 报告事件码是否属于任务事件。
func IsJobCode(code string) bool {
	switch code {
	case CodeStart, CodeDone, CodeFail, CodePause:
		return true
	}
	return false
}

// OutcomeFor 把终态事件码映射为 outcome；JOB_START 返回空串。
func OutcomeFor(code string) string {
	switch code {
	case CodeDone:
		return OutcomeDone
	case CodeFail:
		return OutcomeFail
	case CodePause:
		return OutcomePause
	}
	return ""
}

// JobFields 是 JOB_* 事件携带的四个元数据字段（PRD FR-413 / 物模型 v1.1）。
type JobFields struct {
	JobID          string `json:"job_id"`
	MaterialID     string `json:"material_id"`
	ParamProfileID string `json:"param_profile_id"`
	ParamsHash     string `json:"params_hash"`
	// MaterialCodeID 是 BL3 材料码（31 字符 base32，P1 可选）；job_record 暂无对应列，只做白名单校验不落库。
	MaterialCodeID string `json:"material_code_id,omitempty"`
}

// EventPayload 是 event 载荷中本包关心的部分；其余字段忽略。
type EventPayload struct {
	Seq  int64  `json:"seq"`
	Ts   int64  `json:"ts"`
	Code string `json:"code"`
	JobFields
}

var (
	uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	idRe   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)
	hashRe = regexp.MustCompile(`^[0-9a-f]{64}$`)
	// 材料码：base32 字母表 A-Z2-7，固定 31 字符（19 字节）
	matCodeRe = regexp.MustCompile(`^[A-Z2-7]{31}$`)
)

// ErrBadFields 表示字段格式不在白名单内（INC-4-10：防止文件名 / 参数原文混入事件）。
var ErrBadFields = errors.New("job fields out of whitelist")

// ValidateJobFields 纯函数：job_id 必须是 36 位 UUID；material_id / param_profile_id 为空或
// ^[A-Za-z0-9_-]{1,32}$；params_hash 为空或 64 位小写 hex；material_code_id 为空或 31 位 base32（A-Z2-7）。错误信息只含字段名与长度，不含原文。
func ValidateJobFields(f JobFields) error {
	if !uuidRe.MatchString(f.JobID) {
		return fmt.Errorf("%w: job_id len=%d", ErrBadFields, len(f.JobID))
	}
	if f.MaterialID != "" && !idRe.MatchString(f.MaterialID) {
		return fmt.Errorf("%w: material_id len=%d", ErrBadFields, len(f.MaterialID))
	}
	if f.ParamProfileID != "" && !idRe.MatchString(f.ParamProfileID) {
		return fmt.Errorf("%w: param_profile_id len=%d", ErrBadFields, len(f.ParamProfileID))
	}
	if f.ParamsHash != "" && !hashRe.MatchString(f.ParamsHash) {
		return fmt.Errorf("%w: params_hash len=%d", ErrBadFields, len(f.ParamsHash))
	}
	if f.MaterialCodeID != "" && !matCodeRe.MatchString(f.MaterialCodeID) {
		return fmt.Errorf("%w: material_code_id len=%d", ErrBadFields, len(f.MaterialCodeID))
	}
	return nil
}

// ParseEvent 解析 event 载荷；code 为空视为坏载荷。
func ParseEvent(raw json.RawMessage) (*EventPayload, error) {
	var p EventPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("event payload: %w", err)
	}
	if p.Code == "" {
		return nil, errors.New("event payload: missing code")
	}
	return &p, nil
}
