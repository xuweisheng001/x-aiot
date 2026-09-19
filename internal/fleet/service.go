package fleet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/schedule"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
)

// Shadows 是 Redis 影子读取抽象（测试注入 fake，避免依赖真实 Redis）。
type Shadows interface {
	Read(ctx context.Context, sn string) (map[string]string, error)
}

// RedisShadow 是 Shadows 的 Redis 实现（internal/pkg/shadow）。
type RedisShadow struct{ RDB *redis.Client }

func (r *RedisShadow) Read(ctx context.Context, sn string) (map[string]string, error) {
	if r == nil || r.RDB == nil {
		return map[string]string{}, nil
	}
	return shadow.Read(ctx, r.RDB, sn)
}

// Options 是 fleet-svc 的运行参数（全部可由环境变量覆盖，见 cmd/fleet-svc）。
type Options struct {
	ReconcileInterval time.Duration // 课表对账周期
	DispatchInterval  time.Duration // 调度器轮询周期
	ExpireInterval    time.Duration // 过期清理周期
	UnlockTTL         time.Duration // 临时解锁默认时长
	ApprovedTTL       time.Duration // approved 无人取走多久算过期
	DispatchedTTL     time.Duration // dispatched 无进展多久算过期
	MaxRetry          int           // 下发失败重试上限，超过 → needs_teacher
	ShadowBatch       int           // 批量读影子分片大小（≤ 200）
	MaxDevices        int           // 看板一次最多取多少台
}

func DefaultOptions() Options {
	return Options{
		ReconcileInterval: time.Minute,
		DispatchInterval:  5 * time.Second,
		ExpireInterval:    time.Minute,
		UnlockTTL:         2 * time.Hour,
		ApprovedTTL:       2 * time.Hour,
		DispatchedTTL:     10 * time.Minute,
		MaxRetry:          3,
		ShadowBatch:       200,
		MaxDevices:        5000,
	}
}

// Service 编排纯函数（model.go）与 Store。
type Service struct {
	Store Store
	Sh    Shadows
	M     *Metrics
	Now   func() time.Time
	Opt   Options
	// OTA 是 ota-svc 客户端（组织批量 OTA，见 ota.go）；nil 时该接口返回 500 而不是静默降级。
	OTA OTAClient
}

func NewService(st Store, sh Shadows, m *Metrics, opt Options) *Service {
	if m == nil {
		m = NewMetrics()
	}
	if sh == nil {
		sh = &RedisShadow{}
	}
	def := DefaultOptions()
	if opt.ShadowBatch <= 0 || opt.ShadowBatch > 200 {
		opt.ShadowBatch = def.ShadowBatch
	}
	if opt.MaxDevices <= 0 {
		opt.MaxDevices = def.MaxDevices
	}
	if opt.MaxRetry <= 0 {
		opt.MaxRetry = def.MaxRetry
	}
	if opt.UnlockTTL <= 0 {
		opt.UnlockTTL = def.UnlockTTL
	}
	return &Service{Store: st, Sh: sh, M: m, Now: time.Now, Opt: opt}
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// ---- 组织 / 站点 / 成员 ----

var nameRe = regexp.MustCompile(`^[^\x00-\x1f]{1,128}$`)

func (s *Service) CreateOrg(ctx context.Context, name, typ, region string, creator int64) (*Org, error) {
	name = strings.TrimSpace(name)
	if !nameRe.MatchString(name) {
		return nil, fmt.Errorf("%w: name required", ErrBadParam)
	}
	if typ != "school" && typ != "studio" {
		return nil, fmt.Errorf("%w: type must be school|studio", ErrBadParam)
	}
	if creator <= 0 {
		return nil, fmt.Errorf("%w: X-User-Id required", ErrBadParam)
	}
	if region == "" {
		region = "US"
	}
	return s.Store.CreateOrg(ctx, name, typ, region, creator)
}

func (s *Service) CreateSite(ctx context.Context, orgID int64, name, tz string) (*Site, error) {
	name = strings.TrimSpace(name)
	if !nameRe.MatchString(name) {
		return nil, fmt.Errorf("%w: name required", ErrBadParam)
	}
	if _, err := schedule.Location(tz); err != nil || strings.TrimSpace(tz) == "" {
		return nil, fmt.Errorf("%w: valid tz required", ErrBadParam)
	}
	return s.Store.CreateSite(ctx, orgID, name, tz)
}

func (s *Service) AddMember(ctx context.Context, orgID, userID int64, role Role, invitedBy int64) error {
	if userID <= 0 {
		return fmt.Errorf("%w: user_id required", ErrBadParam)
	}
	if !ValidRole(role) {
		return fmt.Errorf("%w: bad role", ErrBadParam)
	}
	return s.Store.AddMember(ctx, orgID, userID, role, invitedBy)
}

// ---- 设备归属 ----

var snRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// Attach 归属设备：site 必须属本组织；用 ResolveOwnership 裁决个人绑定 / 他组织占用，两种结果都写 lock_audit 留痕。
func (s *Service) Attach(ctx context.Context, orgID, actor int64, sn string, siteID int64) error {
	if !snRe.MatchString(sn) {
		return fmt.Errorf("%w: bad sn", ErrBadParam)
	}
	if _, err := s.Store.GetSite(ctx, orgID, siteID); err != nil {
		return err // 站点不属本组织 → ErrNotFound（不泄露存在性）
	}
	personal, err := s.Store.PersonalBound(ctx, sn)
	if err != nil {
		return err
	}
	orgAttached := false
	switch cur, err := s.Store.DeviceOrgOf(ctx, sn); {
	case err == nil:
		orgAttached = cur.OrgID != orgID // 同组织换站点不算冲突
	case errors.Is(err, ErrNotFound):
	default:
		return err
	}
	d := ResolveOwnership(personal, orgAttached)
	if !d.Allow {
		s.audit(ctx, LockAudit{OrgID: orgID, SN: sn, Action: "denied_personal", Actor: actorOf(actor), Reason: d.Reason})
		return fmt.Errorf("%w: %s", ErrConflict, d.Reason)
	}
	if err := s.Store.AttachDevice(ctx, DeviceOrg{SN: sn, OrgID: orgID, SiteID: siteID, AssignedBy: actor}); err != nil {
		return err
	}
	if d.Audit {
		s.audit(ctx, LockAudit{OrgID: orgID, SN: sn, Action: "denied_personal", Actor: actorOf(actor), Reason: d.Reason})
	}
	return nil
}

func actorOf(userID int64) string { return fmt.Sprintf("user:%d", userID) }

// audit 写锁留痕；失败只记日志（留痕不阻断主流程，但必须可观测）。
func (s *Service) audit(ctx context.Context, a LockAudit) {
	if err := s.Store.InsertLockAudit(ctx, a); err != nil {
		slog.Warn("lock audit", "org", a.OrgID, "sn", a.SN, "action", a.Action, "err", err)
	}
}

// ---- 看板 ----

// SummaryResp 是 GET /fleet/summary 的响应体。
type SummaryResp struct {
	OrgID   int64           `json:"org_id"`
	AsOf    time.Time       `json:"as_of"`
	Counts  Counts          `json:"counts"`
	Devices []ShadowSummary `json:"devices"`
}

// Summary 看板：ListDevices → 分批读影子 → Summarize → 告警 / OTA 集合 → Aggregate。
func (s *Service) Summary(ctx context.Context, orgID int64) (*SummaryResp, error) {
	devs, _, err := s.Store.ListDevices(ctx, orgID, 0, s.Opt.MaxDevices)
	if err != nil {
		return nil, err
	}
	sns := make([]string, 0, len(devs))
	for _, d := range devs {
		sns = append(sns, d.SN)
	}
	shadows, err := s.readShadows(ctx, sns)
	if err != nil {
		return nil, err
	}
	alarmSNs, err := s.Store.OpenAlarmSNs(ctx, sns)
	if err != nil {
		return nil, err
	}
	otaSNs, err := s.Store.PendingOTASNs(ctx, sns)
	if err != nil {
		return nil, err
	}
	now := s.now()
	return &SummaryResp{OrgID: orgID, AsOf: now, Counts: Aggregate(shadows, alarmSNs, otaSNs, now), Devices: shadows}, nil
}

// readShadows 按 ShadowBatch（≤ 200）分批读 Redis 影子。
func (s *Service) readShadows(ctx context.Context, sns []string) ([]ShadowSummary, error) {
	out := make([]ShadowSummary, 0, len(sns))
	for i := 0; i < len(sns); i += s.Opt.ShadowBatch {
		end := min(i+s.Opt.ShadowBatch, len(sns))
		for _, sn := range sns[i:end] {
			rep, err := s.Sh.Read(ctx, sn)
			if err != nil {
				return nil, err
			}
			out = append(out, Summarize(sn, rep))
		}
	}
	return out, nil
}

// ---- 课表与锁 ----

// PutPolicy 校验并落库课表，写 schedule_change 留痕。
func (s *Service) PutPolicy(ctx context.Context, orgID, siteID int64, p schedule.Policy, by int64) (int64, error) {
	if strings.TrimSpace(p.TZ) == "" {
		return 0, fmt.Errorf("%w: tz required", ErrBadParam)
	}
	if err := schedule.Validate(p); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrBadParam, err)
	}
	if _, err := s.Store.GetSite(ctx, orgID, siteID); err != nil {
		return 0, err
	}
	version, err := s.Store.UpsertPolicy(ctx, orgID, siteID, p, by)
	if err != nil {
		return 0, err
	}
	s.audit(ctx, LockAudit{OrgID: orgID, Action: "schedule_change", Actor: actorOf(by), Reason: fmt.Sprintf("site %d version %d", siteID, version)})
	return version, nil
}

// Unlock 临时解锁：patch lock=false + lock_expires_at=now+ttl，写 temp_unlock 留痕。
// 时长上限取 org.unlock_max_minutes（DDL CHECK 1..1440）。
func (s *Service) Unlock(ctx context.Context, orgID, actor int64, sn string, minutes int) (time.Time, error) {
	if _, err := s.Store.GetDevice(ctx, orgID, sn); err != nil {
		return time.Time{}, err // 设备不属本组织 → 404
	}
	ttl := s.Opt.UnlockTTL
	if minutes > 0 {
		ttl = time.Duration(minutes) * time.Minute
	}
	if org, err := s.Store.GetOrg(ctx, orgID); err == nil && org.UnlockMaxMinutes > 0 {
		if maxTTL := time.Duration(org.UnlockMaxMinutes) * time.Minute; ttl > maxTTL {
			ttl = maxTTL
		}
	}
	expires := s.now().Add(ttl).UTC()
	patch, _ := json.Marshal(map[string]any{"lock": false, "lock_expires_at": expires.Format(time.RFC3339)})
	if _, err := s.Store.MergeDesired(ctx, sn, patch); err != nil {
		return time.Time{}, err
	}
	s.audit(ctx, LockAudit{OrgID: orgID, SN: sn, Action: "temp_unlock", Actor: actorOf(actor),
		Reason: fmt.Sprintf("until %s", expires.Format(time.RFC3339))})
	s.M.Inc(MUnlocks)
	return expires, nil
}

// ReconcileOnce 课表对账一轮：AllPolicies → SiteSNs → ShouldLock → GetDesiredLock → ReconcileLock → MergeDesired + 留痕。
func (s *Service) ReconcileOnce(ctx context.Context) (applied int, err error) {
	s.M.Inc(MReconcileRuns)
	policies, err := s.Store.AllPolicies(ctx)
	if err != nil {
		return 0, err
	}
	now := s.now()
	for _, pr := range policies {
		should := schedule.ShouldLock(pr.Policy, now)
		sns, err := s.Store.SiteSNs(ctx, pr.OrgID, pr.SiteID)
		if err != nil {
			slog.Warn("reconcile site sns", "org", pr.OrgID, "site", pr.SiteID, "err", err)
			continue
		}
		for _, sn := range sns {
			dl, err := s.Store.GetDesiredLock(ctx, sn)
			if err != nil {
				slog.Warn("reconcile desired", "sn", sn, "err", err)
				continue
			}
			patch, expired := ReconcileLock(should, dl.Lock, dl.ExpiresAt, now)
			if patch == nil {
				continue
			}
			raw, _ := json.Marshal(patch)
			if _, err := s.Store.MergeDesired(ctx, sn, raw); err != nil {
				slog.Error("reconcile merge desired", "sn", sn, "err", err)
				continue
			}
			action := "unlock"
			if should {
				action = "lock"
			}
			if expired {
				action = "expire"
			}
			s.audit(ctx, LockAudit{OrgID: pr.OrgID, SN: sn, Action: action, Actor: "system", Source: "fleet", Reason: "schedule reconcile"})
			s.M.Inc(MLocksApplied)
			applied++
		}
	}
	return applied, nil
}

// RunReconcile 按 ReconcileInterval 跑对账，直到 ctx 取消。
func (s *Service) RunReconcile(ctx context.Context) {
	t := time.NewTicker(s.Opt.ReconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := s.ReconcileOnce(ctx); err != nil {
				slog.Error("reconcile", "err", err)
			} else if n > 0 {
				slog.Info("reconcile applied", "patches", n)
			}
		}
	}
}

// ---- 队列 ----

var sha256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)

// SubmitReq 是提交任务的请求体。
type SubmitReq struct {
	FileSHA256     string `json:"file_sha256"`
	FileURL        string `json:"file_url"`
	MaterialID     string `json:"material_id"`
	ParamProfileID string `json:"param_profile_id"`
	EstMinutes     int    `json:"est_minutes"`
}

// Submit 学生提交任务（status=submitted，等教师审批）。
func (s *Service) Submit(ctx context.Context, orgID, queueID, user int64, req SubmitReq) (*Item, error) {
	if !sha256Re.MatchString(strings.ToLower(strings.TrimSpace(req.FileSHA256))) {
		return nil, fmt.Errorf("%w: file_sha256 must be 64 hex", ErrBadParam)
	}
	if _, err := s.Store.GetQueue(ctx, orgID, queueID); err != nil {
		return nil, err
	}
	if req.EstMinutes <= 0 {
		req.EstMinutes = 10
	}
	it := Item{ItemID: NewID(), OrgID: orgID, QueueID: queueID, Submitter: user,
		FileSHA256: strings.ToLower(strings.TrimSpace(req.FileSHA256)), FileURL: req.FileURL,
		MaterialID: req.MaterialID, ParamProfileID: req.ParamProfileID, EstMinutes: req.EstMinutes,
		Status: StSubmitted, JobID: "", SubmittedAt: s.now()}
	if err := s.Store.InsertItem(ctx, it); err != nil {
		return nil, err
	}
	it.JobID = it.ItemID
	return &it, nil
}

// SetItemStatus 状态迁移：一律先 model.Transition 校验（角色 + 迁移合法性），再条件 UPDATE。
func (s *Service) SetItemStatus(ctx context.Context, orgID int64, role Role, itemID, to string, by int64, reason string) (*Item, error) {
	it, err := s.Store.GetItem(ctx, orgID, itemID)
	if err != nil {
		return nil, err
	}
	if role == RoleStudent && it.Submitter != by {
		return nil, fmt.Errorf("%w: students may only act on their own items", ErrDenied)
	}
	if err := Transition(it.Status, to, role); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDenied, err)
	}
	if err := s.Store.SetStatus(ctx, orgID, itemID, it.Status, to, actorOf(by), reason); err != nil {
		return nil, err
	}
	it.Status = to
	return it, nil
}

// ---- 耗材 ----

// ConsumablesResp 是耗材集采汇总。
type ConsumablesResp struct {
	Buckets []PartBucket `json:"buckets"`
	Low     []HealthRow  `json:"low"`
}

func (s *Service) Consumables(ctx context.Context, orgID int64) (*ConsumablesResp, error) {
	rows, err := s.Store.ConsumableHealth(ctx, orgID)
	if err != nil {
		return nil, err
	}
	buckets, low := SummarizeConsumables(rows)
	if buckets == nil {
		buckets = []PartBucket{}
	}
	if low == nil {
		low = []HealthRow{}
	}
	return &ConsumablesResp{Buckets: buckets, Low: low}, nil
}

// NewID 生成 36 位 UUID v4 字符串（queue_item.item_id 是 CHAR(36)）。
func NewID() string {
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
