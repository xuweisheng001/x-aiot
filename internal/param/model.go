// Package param 实现 BL4 参数库服务：版本化参数档、双人审批发布、差异校验、灰度、增量同步、回滚、
// 用户自定义参数乐观锁与校正系数只读接口（docs/tech-design-bl4-software-content.md §7 / §9.1）。
//
// 本文件只有纯函数与数据结构，全部表驱动单测；SQL 在 store.go。
package param

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Profile 是一条参数档（iot_global.param_profile 的镜像）。
type Profile struct {
	ID             int64           `json:"id"`
	ProductKey     string          `json:"product_key"`
	ModuleModel    string          `json:"module_model"`
	MaterialID     string          `json:"material_id"`
	Params         json.RawMessage `json:"params"`
	Source         string          `json:"source"`
	VersionAdded   int64           `json:"version_added"`
	VersionRemoved *int64          `json:"version_removed,omitempty"`
	SampleCount    int             `json:"sample_count"`
	Confidence     *float64        `json:"confidence,omitempty"`
	ApprovedBy     string          `json:"approved_by,omitempty"`
}

// Release 是一次发布（iot_global.param_release）。
type Release struct {
	ProductKey     string `json:"product_key"`
	Version        int64  `json:"version"`
	Note           string `json:"note,omitempty"`
	SnapshotURL    string `json:"snapshot_url"`
	RolloutPct     int    `json:"rollout_pct"`
	CreatedBy      string `json:"created_by"`
	ApprovedBy     string `json:"approved_by"`
	ReleasedAt     string `json:"released_at"`
	RolledBackFrom *int64 `json:"rolled_back_from,omitempty"`
}

const (
	SourceOfficial    = "official"
	SourceRecommended = "recommended"

	// DiffPctThreshold / DiffMaxCount：同 (module, material, source) 的 power 或 speed 变化 > 30% 的条目超过 10 条即拒绝发布（INC-4-13）。
	DiffPctThreshold = 0.30
	DiffMaxCount     = 10

	// ForceFullBehind：客户端落后超过 20 个版本改走全量快照。
	ForceFullBehind = 20

	// KMin / KMax：校正系数硬上下限，代码常量不可配置（INC-4-20）。
	KMin = 0.8
	KMax = 1.25
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
	ErrDenied   = errors.New("denied")
	ErrBadParam = errors.New("bad param")
)

// IsEffective 报告 p 在版本 v 下是否有效。
func (p Profile) IsEffective(v int64) bool {
	return p.VersionAdded <= v && (p.VersionRemoved == nil || *p.VersionRemoved > v)
}

// EffectiveAt 返回版本 v 下的有效集合（保持输入顺序）。
func EffectiveAt(profiles []Profile, v int64) []Profile {
	out := make([]Profile, 0, len(profiles))
	for _, p := range profiles {
		if p.IsEffective(v) {
			out = append(out, p)
		}
	}
	return out
}

// Delta 计算从 since 同步到 to 的增量：removed 是 since 有效、to 无效的 id；added 是 since 无效、to 有效的档。
// 同一档在 (since, to] 内先加后删（或反之）对客户端不可见，两边都不出现。removed 与 added 由调用方保证先删后加的应用顺序。
func Delta(profiles []Profile, since, to int64) (removed []int64, added []Profile) {
	removed = []int64{}
	added = []Profile{}
	for _, p := range profiles {
		was, now := p.IsEffective(since), p.IsEffective(to)
		switch {
		case was && !now:
			removed = append(removed, p.ID)
		case !was && now:
			added = append(added, p)
		}
	}
	sort.Slice(removed, func(i, j int) bool { return removed[i] < removed[j] })
	return removed, added
}

// Violation 是一条差异校验违规。
type Violation struct {
	ModuleModel string  `json:"module_model"`
	MaterialID  string  `json:"material_id"`
	Source      string  `json:"source"`
	Field       string  `json:"field"`
	Old         float64 `json:"old"`
	New         float64 `json:"new"`
	ChangePct   float64 `json:"change_pct"`
}

// ParamNum 从 params JSON 取数值字段（数字或数字字符串）。
func ParamNum(params json.RawMessage, key string) (float64, bool) {
	var m map[string]any
	if err := json.Unmarshal(params, &m); err != nil {
		return 0, false
	}
	switch v := m[key].(type) {
	case float64:
		return v, true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	}
	return 0, false
}

func diffKey(p Profile) string { return p.ModuleModel + "|" + p.MaterialID + "|" + p.Source }

// DiffValidate 比较新旧有效集合中同 (module_model, material_id, source) 档的 power 与 speed，
// 相对变化 > pctThreshold 记一条 Violation。返回全部违规；调用方按 len(violations) > maxCount 判定拒绝。
// maxCount 只是签名对齐文档，判定留给调用方（便于 force 时仍能返回违规明细）。
func DiffValidate(old, new []Profile, pctThreshold float64, maxCount int) []Violation {
	_ = maxCount
	oldByKey := map[string]Profile{}
	for _, p := range old {
		oldByKey[diffKey(p)] = p
	}
	var out []Violation
	for _, n := range new {
		o, ok := oldByKey[diffKey(n)]
		if !ok {
			continue
		}
		for _, f := range []string{"power", "speed"} {
			ov, ook := ParamNum(o.Params, f)
			nv, nok := ParamNum(n.Params, f)
			if !ook || !nok || ov == 0 {
				continue
			}
			pct := math.Abs(nv-ov) / math.Abs(ov)
			if pct > pctThreshold {
				out = append(out, Violation{ModuleModel: n.ModuleModel, MaterialID: n.MaterialID, Source: n.Source,
					Field: f, Old: ov, New: nv, ChangePct: math.Round(pct*10000) / 10000})
			}
		}
	}
	return out
}

// DiffRejected 报告违规数是否超过阈值。
func DiffRejected(v []Violation, maxCount int) bool { return len(v) > maxCount }

// RollbackPlan 计算把 current（当前有效集合）恢复成 target（目标版本有效集合）所需的动作：
// toRemove = current 有而 target 没有的 id；toReadd = target 有而 current 没有的档（复制为新档，由调用方设 version_added）。
func RollbackPlan(current, target []Profile) (toRemove []int64, toReadd []Profile) {
	toRemove = []int64{}
	toReadd = []Profile{}
	cur := map[int64]bool{}
	for _, p := range current {
		cur[p.ID] = true
	}
	tgt := map[int64]bool{}
	for _, p := range target {
		tgt[p.ID] = true
	}
	for _, p := range current {
		if !tgt[p.ID] {
			toRemove = append(toRemove, p.ID)
		}
	}
	for _, p := range target {
		if !cur[p.ID] {
			c := p
			c.ID = 0
			c.VersionRemoved = nil
			toReadd = append(toReadd, c)
		}
	}
	sort.Slice(toRemove, func(i, j int) bool { return toRemove[i] < toRemove[j] })
	return toRemove, toReadd
}

// Bucket 返回 md5(id) 低 32 位 / 2^32 ∈ [0,1)，对同一 id 稳定。与 internal/ota.Bucket 同源算法，
// 有意复制而不 import，避免参数库依赖 OTA 包。
func Bucket(id string) float64 {
	sum := md5.Sum([]byte(id))
	low := binary.BigEndian.Uint32(sum[12:16])
	return float64(low) / 4294967296.0
}

// InRollout 报告 bucket 是否落入 pct% 灰度。pct>=100 对所有人可见；没有 bucket（hasBucket=false）时只对 100 可见。
func InRollout(bucket float64, hasBucket bool, pct int) bool {
	if pct >= 100 {
		return true
	}
	if !hasBucket || pct <= 0 {
		return false
	}
	return bucket < float64(pct)/100
}

// VisibleVersion 在按 version 降序的 releases 中返回该 bucket 可见的最高版本。
func VisibleVersion(releases []Release, bucket float64, hasBucket bool) (Release, bool) {
	sorted := make([]Release, len(releases))
	copy(sorted, releases)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Version > sorted[j].Version })
	for _, r := range sorted {
		if InRollout(bucket, hasBucket, r.RolloutPct) {
			return r, true
		}
	}
	return Release{}, false
}

// CanonicalHash 对 params 做规范化（对象键排序）后 sha256，hex 小写；解析失败返回空串。
// 与 XCS 侧 params_hash 的规范化规则一致（docs/tech-design-bl4-software-content.md §6.3）。
func CanonicalHash(params json.RawMessage) string {
	var v any
	if err := json.Unmarshal(params, &v); err != nil {
		return ""
	}
	b, err := json.Marshal(v) // encoding/json 对 map 键排序
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Cfg 是校正系数配置（iot_global.param_correction_cfg）。
type Cfg struct {
	A, B, C    float64
	RatedHours float64
}

// DefaultCfg 是 P1 初始保守值（PRD 与方案 §9.1）。
var DefaultCfg = Cfg{A: 0.15, B: 0.05, C: 0.05, RatedHours: 10000}

// Reason 解释一个校正来源。
type Reason struct {
	Type   string  `json:"type"`
	Value  float64 `json:"value,omitempty"`
	Effect string  `json:"effect"`
}

// Result 是校正结果。
type Result struct {
	KPower  float64  `json:"k_power"`
	KSpeed  float64  `json:"k_speed"`
	Reasons []Reason `json:"reasons"`
}

func clamp(x, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, x)) }

// Correction 计算校正系数：
//
//	k_power = clamp(1 + a·(1 - health/100) + b·hours/rated, KMin, KMax)
//	k_speed = clamp(1 - c·(1 - health/100), KMin, KMax)
//
// 任一输入缺失（healthKnown/hoursKnown 为 false）→ k=1，reason "input_missing"（INC-4-20：缺失不计算）。
func Correction(cfg Cfg, laserHours, health float64, healthKnown, hoursKnown bool) Result {
	if !healthKnown || !hoursKnown {
		return Result{KPower: 1, KSpeed: 1, Reasons: []Reason{{Type: "input_missing", Effect: "校正暂停，使用官方参数"}}}
	}
	if cfg.RatedHours <= 0 {
		cfg.RatedHours = DefaultCfg.RatedHours
	}
	health = clamp(health, 0, 100)
	wear := 1 - health/100
	kp := clamp(1+cfg.A*wear+cfg.B*laserHours/cfg.RatedHours, KMin, KMax)
	ks := clamp(1-cfg.C*wear, KMin, KMax)
	res := Result{KPower: round4(kp), KSpeed: round4(ks), Reasons: []Reason{}}
	if wear > 0 {
		res.Reasons = append(res.Reasons, Reason{Type: "health", Value: health, Effect: pctEffect(cfg.A * wear)})
	}
	if laserHours > 0 {
		res.Reasons = append(res.Reasons, Reason{Type: "laser_hours", Value: laserHours, Effect: pctEffect(cfg.B * laserHours / cfg.RatedHours)})
	}
	if kp >= KMax {
		res.Reasons = append(res.Reasons, Reason{Type: "limit", Effect: "已达上限，建议更换模块"})
	}
	return res
}

func round4(x float64) float64 { return math.Round(x*10000) / 10000 }

func pctEffect(delta float64) string {
	return fmt.Sprintf("%+.1f%%", delta*100)
}

// ValidModule / ValidMaterial：与 DDL 列宽一致的简单格式校验。
func ValidIdent(s string) bool {
	if len(s) == 0 || len(s) > 32 {
		return false
	}
	for _, c := range s {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}
