package simulator

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// 物模型 v1.1 任务事件（技术方案 BL4 §6.3 / §11.3）：
// 设备只在 reported.job_feedback_optin 为 true 时携带 job 四字段，这是「图纸不上云、未同意不上报」的第一道闸（INC-4-10 / INC-4-18）。

// Materials 是模拟器轮转使用的官方材料 id（sql/bl4.sql 预置）。
var Materials = []string{"BASSWOOD_3MM", "ACRYLIC_3MM", "LEATHER_1MM"}

// DefaultJobParams 是模拟器每次加工使用的固定参数档内容。
var DefaultJobParams = map[string]any{"power": 60, "speed": 100, "passes": 1}

// DefaultParamProfileID 是模拟器上报的参数档 id。
const DefaultParamProfileID = "official-1"

// JobFields 是事件载荷里的四个任务字段；全部 omitempty，零值即「不出现」。
type JobFields struct {
	JobID          string `json:"job_id,omitempty"`
	MaterialID     string `json:"material_id,omitempty"`
	ParamProfileID string `json:"param_profile_id,omitempty"`
	ParamsHash     string `json:"params_hash,omitempty"`
}

// JobEvent = Event + 任务字段 + 完成用时。
type JobEvent struct {
	Event
	JobFields
	DurationS int64 `json:"duration_s,omitempty"`
}

// ParamsHash = sha256(键排序的紧凑 JSON) 小写 hex。json.Marshal 对 map 已按键排序且无空白。
func ParamsHash(params map[string]any) string {
	b, _ := json.Marshal(params)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// BuildJobFields 纯函数：optin=false → 四字段全空；true → 原样携带并计算 hash。
func BuildJobFields(optin bool, jobID, materialID, profileID string, params map[string]any) JobFields {
	if !optin {
		return JobFields{}
	}
	return JobFields{JobID: jobID, MaterialID: materialID, ParamProfileID: profileID, ParamsHash: ParamsHash(params)}
}

// MaterialFor 按任务序号轮转材料。
func MaterialFor(jobIdx int64) string {
	if jobIdx < 0 {
		jobIdx = -jobIdx
	}
	return Materials[int(jobIdx%int64(len(Materials)))]
}

// NewJobID 生成 RFC 4122 v4 UUID（设备侧 job_id）。
func NewJobID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	var dst [36]byte
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], b[10:16])
	return string(dst[:])
}

// JobTransition 纯函数：由前后 work_state 判断任务边界。
// 非作业(≠2) → 作业(2) 为 start；作业 → 非作业 为 done。
func JobTransition(prevState, state int) (start, done bool) {
	working := state == 2
	wasWorking := prevState == 2
	return working && !wasWorking, wasWorking && !working
}

// OptinFromDesired 从 desired 载荷取 job_feedback_optin；不存在或非 bool 返回 ok=false。
func OptinFromDesired(desired map[string]any) (v, ok bool) {
	raw, exists := desired["job_feedback_optin"]
	if !exists {
		return false, false
	}
	b, isBool := raw.(bool)
	return b, isBool
}
