// Package fleet 是 BL5「教育与 B 端」的组织层：租户与授权、设备归属、多机看板、时段锁定对账、班级任务队列与调度器、耗材集采汇总。
// 纪律（docs/spec.md §8）：判定逻辑全部是纯函数并表驱动单测；所有租户 SQL 以 org_id 为首个条件（技术方案 §4.4 三道锁）。
package fleet

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Role 是组织成员角色（org_member.role）。
type Role string

const (
	RoleAdmin   Role = "org_admin"
	RoleTeacher Role = "teacher"
	RoleStudent Role = "student"
)

// ValidRole 报告角色是否合法。
func ValidRole(r Role) bool { return r == RoleAdmin || r == RoleTeacher || r == RoleStudent }

// Action 是角色矩阵里的动作（技术方案 §4.2）。
type Action string

const (
	ActView          Action = "view"           // 看板 / 设备状态 / 队列列表
	ActManageMembers Action = "manage_members" // 成员与归属管理、建站点
	ActAttachDevice  Action = "attach_device"
	ActEditSchedule  Action = "edit_schedule"
	ActUnlock        Action = "unlock"
	ActControl       Action = "control" // pause / stop
	ActManageQueue   Action = "manage_queue"
	ActApprove       Action = "approve"
	ActSubmit        Action = "submit"
	ActViewConsumabl Action = "view_consumables"
)

// matrix 是角色矩阵（§4.2）；不在表里的 (role, action) 一律拒绝。
var matrix = map[Role]map[Action]bool{
	RoleAdmin: {ActView: true, ActManageMembers: true, ActAttachDevice: true, ActEditSchedule: true, ActUnlock: true,
		ActControl: true, ActManageQueue: true, ActApprove: true, ActSubmit: true, ActViewConsumabl: true},
	RoleTeacher: {ActView: true, ActEditSchedule: true, ActUnlock: true, ActControl: true, ActManageQueue: true,
		ActApprove: true, ActSubmit: true, ActViewConsumabl: true},
	RoleStudent: {ActView: true, ActSubmit: true},
}

// Authorize 纯函数：角色是否允许动作。未知角色 / 动作 → false（fail-closed）。
func Authorize(role Role, action Action) bool { return matrix[role][action] }

// Decision 是归属冲突裁决结果。
type Decision struct {
	Allow  bool   // 是否允许归属到组织
	Audit  bool   // 是否需要写留痕（个人绑定被组织覆盖）
	Reason string // 留痕 / 拒绝原因
}

// ResolveOwnership 纯函数：设备已有个人绑定 / 已归属组织时的裁决（§4.2「冲突以组织为准并留痕」）。
//   - 已归属别的组织 → 拒绝（一设备一组织，DDL sn 主键护栏）；
//   - 有个人绑定 → 允许，组织为准，写留痕通知个人用户；
//   - 都没有 → 允许，无需留痕。
func ResolveOwnership(personalBound, orgAttached bool) Decision {
	switch {
	case orgAttached:
		return Decision{Allow: false, Reason: "device already attached to another org"}
	case personalBound:
		return Decision{Allow: true, Audit: true, Reason: "personal binding overridden by org"}
	default:
		return Decision{Allow: true}
	}
}

// ---- 任务队列状态机（queue_item.status，取值受 ck_item_status 约束） ----
//
// 任务书里的 queued ≡ DDL 的 approved（教师审批通过即入队，调度器从 approved 取队首）；expired ≡ DDL 的 skipped。

const (
	StSubmitted    = "submitted"
	StApproved     = "approved" // = queued
	StRejected     = "rejected"
	StCanceled     = "canceled"
	StDispatched   = "dispatched"
	StRunning      = "running"
	StDone         = "done"
	StFailed       = "failed"
	StSkipped      = "skipped" // = expired
	StNeedsTeacher = "needs_teacher"
)

// RoleSystem 是调度器 / 对账等内部动作的执行者。
const RoleSystem Role = "system"

// ErrTransition 是非法状态迁移。
var ErrTransition = errors.New("illegal transition")

// transitions: from → to → 允许执行的角色集合。
var transitions = map[string]map[string][]Role{
	StSubmitted:    {StApproved: {RoleAdmin, RoleTeacher}, StRejected: {RoleAdmin, RoleTeacher}, StCanceled: {RoleStudent, RoleTeacher, RoleAdmin}, StSkipped: {RoleSystem}},
	StApproved:     {StDispatched: {RoleSystem}, StCanceled: {RoleStudent, RoleTeacher, RoleAdmin}, StSkipped: {RoleSystem}, StNeedsTeacher: {RoleSystem}},
	StDispatched:   {StRunning: {RoleSystem}, StDone: {RoleSystem}, StFailed: {RoleSystem}, StSkipped: {RoleSystem}, StApproved: {RoleSystem}},
	StRunning:      {StDone: {RoleSystem}, StFailed: {RoleSystem}},
	StNeedsTeacher: {StApproved: {RoleAdmin, RoleTeacher}, StRejected: {RoleAdmin, RoleTeacher}},
}

// Transition 纯函数：from→to 是否允许且 role 是否有权；违规返回 ErrTransition（包装了细节）。
func Transition(from, to string, role Role) error {
	roles, ok := transitions[from][to]
	if !ok {
		return fmt.Errorf("%w: %s -> %s", ErrTransition, from, to)
	}
	for _, r := range roles {
		if r == role {
			return nil
		}
	}
	return fmt.Errorf("%w: %s -> %s not allowed for role %s", ErrTransition, from, to, role)
}

// ---- 影子摘要与看板聚合 ----

// OnlineWindow 复用 deviceapi 的 90 s 在线判定。
const OnlineWindow = 90 * time.Second

// ShadowSummary 是一台设备的影子摘要（从 Redis reported 解析）。
type ShadowSummary struct {
	SN        string `json:"sn"`
	Present   bool   `json:"present"` // 影子是否存在
	WorkState int    `json:"work_state"`
	Locked    bool   `json:"locked"`
	UpdatedAt int64  `json:"updated_at"` // ms
	OTAPhase  string `json:"ota_phase,omitempty"`
}

// Summarize 把 reported Hash 解析成摘要。
func Summarize(sn string, reported map[string]string) ShadowSummary {
	s := ShadowSummary{SN: sn, Present: len(reported) > 0}
	s.WorkState = WorkStateOf(reported)
	s.Locked = LockedOf(reported)
	s.UpdatedAt, _ = strconv.ParseInt(reported["updated_at"], 10, 64)
	s.OTAPhase = reported["ota_phase"]
	return s
}

// WorkStateOf 解析 reported.work_state（pipeline 以数字写入，HSET 后为 "0"/"2" 这类字符串）；缺失或非法 → -1（未知）。
func WorkStateOf(reported map[string]string) int {
	v, ok := reported["work_state"]
	if !ok {
		return -1
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return -1
	}
	return int(f)
}

// LockedOf 解析 reported 的锁态：lock_state 非 0 或 lock 为真都算 locked（设备端 §17.4 上报 lock_state）。
func LockedOf(reported map[string]string) bool {
	if v, ok := reported["lock_state"]; ok {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err == nil && f != 0 {
			return true
		}
	}
	switch strings.ToLower(strings.TrimSpace(reported["lock"])) {
	case "true", "1":
		return true
	}
	return false
}

// Online 报告影子在 now 之前 OnlineWindow 内更新过（无时间信息按离线——看板宁可少算在线）。
func (s ShadowSummary) Online(now time.Time) bool {
	if s.UpdatedAt <= 0 {
		return false
	}
	return now.Sub(time.UnixMilli(s.UpdatedAt)) <= OnlineWindow
}

// Idle 报告设备空闲且未锁（调度器候选条件之一）。
func (s ShadowSummary) Idle() bool { return s.WorkState == 0 && !s.Locked }

// Counts 是看板计数。
type Counts struct {
	Total      int `json:"total"`
	Online     int `json:"online"`
	Working    int `json:"working"`
	Locked     int `json:"locked"`
	Alarm      int `json:"alarm"`
	PendingOTA int `json:"pending_ota"`
}

// Aggregate 纯函数：按影子摘要聚合看板计数；alarmSNs / otaSNs 是有未关闭告警 / 未完成 OTA 任务的 SN 集合（来自 PG）。
func Aggregate(shadows []ShadowSummary, alarmSNs, otaSNs map[string]bool, now time.Time) Counts {
	var c Counts
	c.Total = len(shadows)
	for _, s := range shadows {
		if s.Online(now) {
			c.Online++
			if s.WorkState > 0 {
				c.Working++
			}
		}
		if s.Locked {
			c.Locked++
		}
		if alarmSNs[s.SN] {
			c.Alarm++
		}
		if otaSNs[s.SN] {
			c.PendingOTA++
		}
	}
	return c
}

// ---- 锁对账 ----

// LockPatch 是对账要下发的 desired 补丁；nil 表示无需下发。
type LockPatch map[string]any

// ReconcileLock 纯函数：根据「此刻应有 lock」、当前 desired.lock、临时解锁到期时间决定是否补发。
//   - 临时解锁未到期 → 不动（教师解锁优先于课表，INC-5-08）；
//   - 到期 → 补发应有值并清 lock_expires_at（action=expire）；
//   - desired.lock 已等于应有值 → 不动（避免下发风暴，INC-5-12）；
//   - 否则补发。
func ReconcileLock(shouldLock bool, curLock *bool, expiresAt *time.Time, now time.Time) (patch LockPatch, expired bool) {
	if expiresAt != nil {
		if now.Before(*expiresAt) {
			return nil, false
		}
		return LockPatch{"lock": shouldLock, "lock_expires_at": nil}, true
	}
	if curLock != nil && *curLock == shouldLock {
		return nil, false
	}
	return LockPatch{"lock": shouldLock}, false
}

// ---- 耗材汇总 ----

// HealthRow 是 consumable_health 一行。
type HealthRow struct {
	SN             string     `json:"sn"`
	Part           string     `json:"part"`
	Health         float64    `json:"health"`
	PredictedEOLAt *time.Time `json:"predicted_eol_at,omitempty"`
}

// PartBucket 是某部件的健康度分布。
type PartBucket struct {
	Part     string `json:"part"`
	Total    int    `json:"total"`
	Good     int    `json:"good"`     // ≥ 60
	Warn     int    `json:"warn"`     // [20, 60)
	Critical int    `json:"critical"` // < 20
}

// LowHealthThreshold 是进入采购清单的阈值。
const LowHealthThreshold = 20

// SummarizeConsumables 纯函数：按 part 分布 + health<20 清单（清单按 health 升序、sn 次序稳定）。
func SummarizeConsumables(rows []HealthRow) (buckets []PartBucket, low []HealthRow) {
	idx := map[string]int{}
	for _, r := range rows {
		i, ok := idx[r.Part]
		if !ok {
			i = len(buckets)
			idx[r.Part] = i
			buckets = append(buckets, PartBucket{Part: r.Part})
		}
		b := &buckets[i]
		b.Total++
		switch {
		case r.Health < LowHealthThreshold:
			b.Critical++
			low = append(low, r)
		case r.Health < 60:
			b.Warn++
		default:
			b.Good++
		}
	}
	// 清单排序：health 升序，再 sn
	for i := 1; i < len(low); i++ {
		for j := i; j > 0 && less(low[j], low[j-1]); j-- {
			low[j], low[j-1] = low[j-1], low[j]
		}
	}
	return buckets, low
}

func less(a, b HealthRow) bool {
	if a.Health != b.Health {
		return a.Health < b.Health
	}
	if a.SN != b.SN {
		return a.SN < b.SN
	}
	return a.Part < b.Part
}

// ConsumablesCSV 纯函数：低健康清单导出（表头 sn,part,health,predicted_eol_at）。
func ConsumablesCSV(low []HealthRow) string {
	var sb strings.Builder
	sb.WriteString("sn,part,health,predicted_eol_at\n")
	for _, r := range low {
		eol := ""
		if r.PredictedEOLAt != nil {
			eol = r.PredictedEOLAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(&sb, "%s,%s,%.2f,%s\n", csvField(r.SN), csvField(r.Part), r.Health, eol)
	}
	return sb.String()
}

func csvField(s string) string {
	if strings.ContainsAny(s, ",\"\n") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

// ---- 实体 ----

type Org struct {
	OrgID            int64     `json:"org_id"`
	Name             string    `json:"name"`
	Type             string    `json:"type"`
	Region           string    `json:"region"`
	UnlockMaxMinutes int       `json:"unlock_max_minutes"`
	CreatedAt        time.Time `json:"created_at"`
}

type Site struct {
	SiteID    int64     `json:"site_id"`
	OrgID     int64     `json:"org_id"`
	Name      string    `json:"name"`
	TZ        string    `json:"tz"`
	CreatedAt time.Time `json:"created_at"`
}

type Member struct {
	OrgID    int64     `json:"org_id"`
	UserID   int64     `json:"user_id"`
	Role     Role      `json:"role"`
	JoinedAt time.Time `json:"joined_at"`
}

type DeviceOrg struct {
	SN         string    `json:"sn"`
	OrgID      int64     `json:"org_id"`
	SiteID     int64     `json:"site_id"`
	AssignedBy int64     `json:"assigned_by"`
	AssignedAt time.Time `json:"assigned_at"`
}

type Queue struct {
	QueueID   int64    `json:"queue_id"`
	OrgID     int64    `json:"org_id"`
	SiteID    int64    `json:"site_id"`
	Name      string   `json:"name"`
	DeviceSNs []string `json:"device_sns"`
}

type Item struct {
	ItemID          string     `json:"item_id"`
	OrgID           int64      `json:"org_id"`
	QueueID         int64      `json:"queue_id"`
	Submitter       int64      `json:"submitter_user_id"`
	FileSHA256      string     `json:"file_sha256"`
	FileURL         string     `json:"file_url"`
	MaterialID      string     `json:"material_id,omitempty"`
	ParamProfileID  string     `json:"param_profile_id,omitempty"`
	EstMinutes      int        `json:"est_minutes"`
	Status          string     `json:"status"`
	ApprovedBy      string     `json:"approved_by,omitempty"`
	RejectedReason  string     `json:"rejected_reason,omitempty"`
	AssignedSN      string     `json:"assigned_sn,omitempty"`
	JobID           string     `json:"job_id,omitempty"`
	DispatchedCmdID string     `json:"dispatched_cmd_id,omitempty"`
	Retry           int        `json:"retry"`
	SubmittedAt     time.Time  `json:"submitted_at"`
	ApprovedAt      *time.Time `json:"approved_at,omitempty"`
	DispatchedAt    *time.Time `json:"dispatched_at,omitempty"`
}

// LockAudit 是 lock_audit 一行。
type LockAudit struct {
	OrgID  int64
	SN     string
	Action string // lock | unlock | temp_unlock | expire | denied_personal | schedule_change
	Actor  string
	Source string
	Reason string
}
