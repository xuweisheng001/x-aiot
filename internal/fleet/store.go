package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtool/xtool-aiot/internal/pkg/schedule"
)

// 业务错误：handler 按此映射 HTTP 状态。
var (
	ErrNotFound  = errors.New("not found")  // 404：资源不存在或不属于该组织（租户隔离一律 404，不泄露存在性）
	ErrNotMember = errors.New("not member") // 404：非成员
	ErrConflict  = errors.New("conflict")   // 409
	ErrBadParam  = errors.New("bad param")  // 400
	ErrDenied    = errors.New("denied")     // 403
)

// PolicyRow 是 schedule_policy 一行。
type PolicyRow struct {
	OrgID   int64
	SiteID  int64
	Version int64
	Policy  schedule.Policy
}

// DesiredLock 是 shadow_desired 中与锁相关的字段。
type DesiredLock struct {
	Lock      *bool
	ExpiresAt *time.Time
}

// Store 是 fleet 依赖的 PG 操作集合。所有租户方法第一个参数就是 orgID，SQL 以 org_id 为首个条件（§4.4 第一道锁）。
type Store interface {
	// 组织与成员
	CreateOrg(ctx context.Context, name, typ, region string, creator int64) (*Org, error)
	GetOrg(ctx context.Context, orgID int64) (*Org, error)
	GetRole(ctx context.Context, orgID, userID int64) (Role, error)
	AddMember(ctx context.Context, orgID, userID int64, role Role, invitedBy int64) error
	RemoveMember(ctx context.Context, orgID, userID int64) error
	ListMembers(ctx context.Context, orgID int64) ([]Member, error)
	CreateSite(ctx context.Context, orgID int64, name, tz string) (*Site, error)
	GetSite(ctx context.Context, orgID, siteID int64) (*Site, error)
	ListSites(ctx context.Context, orgID int64) ([]Site, error)
	// 归属
	PersonalBound(ctx context.Context, sn string) (bool, error)
	DeviceOrgOf(ctx context.Context, sn string) (*DeviceOrg, error) // 不限租户：归属冲突裁决用；找不到 → ErrNotFound
	AttachDevice(ctx context.Context, d DeviceOrg) error
	DetachDevice(ctx context.Context, orgID int64, sn string) error
	GetDevice(ctx context.Context, orgID int64, sn string) (*DeviceOrg, error)
	ListDevices(ctx context.Context, orgID int64, offset, limit int) ([]DeviceOrg, int, error)
	SiteSNs(ctx context.Context, orgID, siteID int64) ([]string, error)
	// 课表与锁
	UpsertPolicy(ctx context.Context, orgID, siteID int64, p schedule.Policy, by int64) (int64, error)
	GetPolicy(ctx context.Context, orgID, siteID int64) (*PolicyRow, error)
	AllPolicies(ctx context.Context) ([]PolicyRow, error) // 对账用，跨租户只读
	GetDesiredLock(ctx context.Context, sn string) (DesiredLock, error)
	MergeDesired(ctx context.Context, sn string, patch json.RawMessage) (int64, error) // 与 deviceapi PATCH /desired 同 SQL
	InsertLockAudit(ctx context.Context, a LockAudit) error
	ListLockAudit(ctx context.Context, orgID int64, sn string, since time.Time, limit int) ([]map[string]any, error)
	// 队列
	CreateQueue(ctx context.Context, orgID, siteID int64, name string, sns []string, by int64) (*Queue, error)
	GetQueue(ctx context.Context, orgID, queueID int64) (*Queue, error)
	ListQueues(ctx context.Context, orgID int64) ([]Queue, error)
	AllQueues(ctx context.Context) ([]Queue, error) // 调度器用
	InsertItem(ctx context.Context, it Item) error
	GetItem(ctx context.Context, orgID int64, itemID string) (*Item, error)
	ListItems(ctx context.Context, orgID, queueID int64, status string) ([]Item, error)
	// 状态迁移：带前置状态的条件 UPDATE，affected=0 → ErrConflict
	SetStatus(ctx context.Context, orgID int64, itemID, from, to, by, reason string) error
	// 调度器
	HeadApproved(ctx context.Context, queueID int64) (*Item, error)
	ActiveSNs(ctx context.Context, queueID int64) (map[string]bool, error)
	ClaimDispatch(ctx context.Context, itemID, sn, token string) (bool, error)
	ConfirmDispatch(ctx context.Context, itemID, token, cmdID string) error
	RevertDispatch(ctx context.Context, itemID, token string, maxRetry int) error
	ExpireItems(ctx context.Context, approvedTTL, dispatchedTTL time.Duration, now time.Time) (int, error)
	// 看板 / 耗材
	OpenAlarmSNs(ctx context.Context, sns []string) (map[string]bool, error)
	PendingOTASNs(ctx context.Context, sns []string) (map[string]bool, error)
	ConsumableHealth(ctx context.Context, orgID int64) ([]HealthRow, error)
}

// PGStore 是 Store 的 pgx 实现。
type PGStore struct{ DB *pgxpool.Pool }

func NewPGStore(db *pgxpool.Pool) *PGStore { return &PGStore{DB: db} }

// ---- 组织与成员 ----

func (s *PGStore) CreateOrg(ctx context.Context, name, typ, region string, creator int64) (*Org, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	o := &Org{Name: name, Type: typ, Region: region}
	err = tx.QueryRow(ctx, `INSERT INTO iot_global.org(name,type,region) VALUES($1,$2,$3) RETURNING org_id, unlock_max_minutes, created_at`,
		name, typ, region).Scan(&o.OrgID, &o.UnlockMaxMinutes, &o.CreatedAt)
	if err != nil {
		if sqlState(err) == "23514" {
			return nil, fmt.Errorf("%w: %v", ErrBadParam, err)
		}
		return nil, fmt.Errorf("create org: %w", err)
	}
	if creator > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO iot_shard.org_member(org_id,user_id,role,invited_by) VALUES($1,$2,'org_admin',$2)`, o.OrgID, creator); err != nil {
			return nil, fmt.Errorf("create org admin: %w", err)
		}
	}
	return o, tx.Commit(ctx)
}

func (s *PGStore) GetOrg(ctx context.Context, orgID int64) (*Org, error) {
	o := &Org{}
	err := s.DB.QueryRow(ctx, `SELECT org_id,name,type,region,unlock_max_minutes,created_at FROM iot_global.org WHERE org_id=$1`, orgID).
		Scan(&o.OrgID, &o.Name, &o.Type, &o.Region, &o.UnlockMaxMinutes, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return o, err
}

func (s *PGStore) GetRole(ctx context.Context, orgID, userID int64) (Role, error) {
	var r string
	err := s.DB.QueryRow(ctx, `SELECT role FROM iot_shard.org_member WHERE org_id=$1 AND user_id=$2 AND removed_at IS NULL`, orgID, userID).Scan(&r)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotMember
	}
	if err != nil {
		return "", fmt.Errorf("get role: %w", err)
	}
	return Role(r), nil
}

func (s *PGStore) AddMember(ctx context.Context, orgID, userID int64, role Role, invitedBy int64) error {
	// 已是成员则改角色（uk_member_active 部分唯一）
	ct, err := s.DB.Exec(ctx, `UPDATE iot_shard.org_member SET role=$3 WHERE org_id=$1 AND user_id=$2 AND removed_at IS NULL`, orgID, userID, string(role))
	if err != nil {
		return fmt.Errorf("add member: %w", err)
	}
	if ct.RowsAffected() == 1 {
		return nil
	}
	_, err = s.DB.Exec(ctx, `INSERT INTO iot_shard.org_member(org_id,user_id,role,invited_by) VALUES($1,$2,$3,$4)`, orgID, userID, string(role), invitedBy)
	if err != nil {
		if sqlState(err) == "23505" {
			return ErrConflict
		}
		return fmt.Errorf("add member: %w", err)
	}
	return nil
}

func (s *PGStore) RemoveMember(ctx context.Context, orgID, userID int64) error {
	ct, err := s.DB.Exec(ctx, `UPDATE iot_shard.org_member SET removed_at=now() WHERE org_id=$1 AND user_id=$2 AND removed_at IS NULL`, orgID, userID)
	if err != nil {
		return fmt.Errorf("remove member: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PGStore) ListMembers(ctx context.Context, orgID int64) ([]Member, error) {
	rows, err := s.DB.Query(ctx, `SELECT org_id,user_id,role,joined_at FROM iot_shard.org_member WHERE org_id=$1 AND removed_at IS NULL ORDER BY joined_at`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Member
	for rows.Next() {
		var m Member
		var role string
		if err := rows.Scan(&m.OrgID, &m.UserID, &role, &m.JoinedAt); err != nil {
			return nil, err
		}
		m.Role = Role(role)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *PGStore) CreateSite(ctx context.Context, orgID int64, name, tz string) (*Site, error) {
	st := &Site{OrgID: orgID, Name: name, TZ: tz}
	err := s.DB.QueryRow(ctx, `INSERT INTO iot_global.site(org_id,name,tz) VALUES($1,$2,$3) RETURNING site_id, created_at`, orgID, name, tz).Scan(&st.SiteID, &st.CreatedAt)
	if err != nil {
		if sqlState(err) == "23514" {
			return nil, fmt.Errorf("%w: %v", ErrBadParam, err)
		}
		return nil, fmt.Errorf("create site: %w", err)
	}
	return st, nil
}

func (s *PGStore) GetSite(ctx context.Context, orgID, siteID int64) (*Site, error) {
	st := &Site{}
	err := s.DB.QueryRow(ctx, `SELECT site_id,org_id,name,tz,created_at FROM iot_global.site WHERE org_id=$1 AND site_id=$2`, orgID, siteID).
		Scan(&st.SiteID, &st.OrgID, &st.Name, &st.TZ, &st.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return st, err
}

func (s *PGStore) ListSites(ctx context.Context, orgID int64) ([]Site, error) {
	rows, err := s.DB.Query(ctx, `SELECT site_id,org_id,name,tz,created_at FROM iot_global.site WHERE org_id=$1 ORDER BY site_id`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Site
	for rows.Next() {
		var st Site
		if err := rows.Scan(&st.SiteID, &st.OrgID, &st.Name, &st.TZ, &st.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// ---- 归属 ----

func (s *PGStore) PersonalBound(ctx context.Context, sn string) (bool, error) {
	var n int
	err := s.DB.QueryRow(ctx, `SELECT count(*) FROM iot_shard.device_binding WHERE sn=$1 AND unbound_at IS NULL`, sn).Scan(&n)
	return n > 0, err
}

func (s *PGStore) DeviceOrgOf(ctx context.Context, sn string) (*DeviceOrg, error) {
	d := &DeviceOrg{}
	var by *int64
	err := s.DB.QueryRow(ctx, `SELECT sn,org_id,site_id,assigned_by,assigned_at FROM iot_shard.device_org WHERE sn=$1`, sn).
		Scan(&d.SN, &d.OrgID, &d.SiteID, &by, &d.AssignedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if by != nil {
		d.AssignedBy = *by
	}
	return d, err
}

func (s *PGStore) AttachDevice(ctx context.Context, d DeviceOrg) error {
	// 同组织内允许换站点；别的组织持有 → 主键冲突 → ErrConflict（一设备一组织）
	ct, err := s.DB.Exec(ctx, `
INSERT INTO iot_shard.device_org(sn,org_id,site_id,assigned_by) VALUES($1,$2,$3,$4)
ON CONFLICT (sn) DO UPDATE SET site_id=EXCLUDED.site_id, assigned_by=EXCLUDED.assigned_by, assigned_at=now()
 WHERE device_org.org_id = EXCLUDED.org_id`, d.SN, d.OrgID, d.SiteID, d.AssignedBy)
	if err != nil {
		return fmt.Errorf("attach device: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

func (s *PGStore) DetachDevice(ctx context.Context, orgID int64, sn string) error {
	ct, err := s.DB.Exec(ctx, `DELETE FROM iot_shard.device_org WHERE org_id=$1 AND sn=$2`, orgID, sn)
	if err != nil {
		return fmt.Errorf("detach device: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PGStore) GetDevice(ctx context.Context, orgID int64, sn string) (*DeviceOrg, error) {
	d := &DeviceOrg{}
	var by *int64
	err := s.DB.QueryRow(ctx, `SELECT sn,org_id,site_id,assigned_by,assigned_at FROM iot_shard.device_org WHERE org_id=$1 AND sn=$2`, orgID, sn).
		Scan(&d.SN, &d.OrgID, &d.SiteID, &by, &d.AssignedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if by != nil {
		d.AssignedBy = *by
	}
	return d, err
}

func (s *PGStore) ListDevices(ctx context.Context, orgID int64, offset, limit int) ([]DeviceOrg, int, error) {
	var total int
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM iot_shard.device_org WHERE org_id=$1`, orgID).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.DB.Query(ctx, `SELECT sn,org_id,site_id,COALESCE(assigned_by,0),assigned_at FROM iot_shard.device_org WHERE org_id=$1 ORDER BY sn OFFSET $2 LIMIT $3`, orgID, offset, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []DeviceOrg
	for rows.Next() {
		var d DeviceOrg
		if err := rows.Scan(&d.SN, &d.OrgID, &d.SiteID, &d.AssignedBy, &d.AssignedAt); err != nil {
			return nil, 0, err
		}
		out = append(out, d)
	}
	return out, total, rows.Err()
}

func (s *PGStore) SiteSNs(ctx context.Context, orgID, siteID int64) ([]string, error) {
	rows, err := s.DB.Query(ctx, `SELECT sn FROM iot_shard.device_org WHERE org_id=$1 AND site_id=$2 ORDER BY sn`, orgID, siteID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var sn string
		if err := rows.Scan(&sn); err != nil {
			return nil, err
		}
		out = append(out, sn)
	}
	return out, rows.Err()
}

// ---- 课表与锁 ----

func (s *PGStore) UpsertPolicy(ctx context.Context, orgID, siteID int64, p schedule.Policy, by int64) (int64, error) {
	weekly, _ := json.Marshal(p.Weekly)
	if p.Weekly == nil {
		weekly = []byte(`{}`)
	}
	overrides, _ := json.Marshal(p.Overrides)
	if p.Overrides == nil {
		overrides = []byte(`[]`)
	}
	var version int64
	err := s.DB.QueryRow(ctx, `
INSERT INTO iot_shard.schedule_policy(org_id,site_id,tz,weekly,overrides,updated_by) VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6)
ON CONFLICT (site_id) DO UPDATE SET tz=EXCLUDED.tz, weekly=EXCLUDED.weekly, overrides=EXCLUDED.overrides,
   version=schedule_policy.version+1, updated_by=EXCLUDED.updated_by, updated_at=now()
 WHERE schedule_policy.org_id = EXCLUDED.org_id
RETURNING version`, orgID, siteID, p.TZ, string(weekly), string(overrides), by).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound // site 属别的组织
	}
	if err != nil {
		return 0, fmt.Errorf("upsert policy: %w", err)
	}
	return version, nil
}

func scanPolicy(row pgx.Row) (*PolicyRow, error) {
	var pr PolicyRow
	var weekly, overrides []byte
	if err := row.Scan(&pr.OrgID, &pr.SiteID, &pr.Version, &pr.Policy.TZ, &weekly, &overrides); err != nil {
		return nil, err
	}
	_ = json.Unmarshal(weekly, &pr.Policy.Weekly)
	_ = json.Unmarshal(overrides, &pr.Policy.Overrides)
	pr.Policy.Version = pr.Version
	return &pr, nil
}

func (s *PGStore) GetPolicy(ctx context.Context, orgID, siteID int64) (*PolicyRow, error) {
	pr, err := scanPolicy(s.DB.QueryRow(ctx, `SELECT org_id,site_id,version,tz,weekly,overrides FROM iot_shard.schedule_policy WHERE org_id=$1 AND site_id=$2`, orgID, siteID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return pr, err
}

func (s *PGStore) AllPolicies(ctx context.Context) ([]PolicyRow, error) {
	rows, err := s.DB.Query(ctx, `SELECT org_id,site_id,version,tz,weekly,overrides FROM iot_shard.schedule_policy ORDER BY org_id, site_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PolicyRow
	for rows.Next() {
		pr, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *pr)
	}
	return out, rows.Err()
}

func (s *PGStore) GetDesiredLock(ctx context.Context, sn string) (DesiredLock, error) {
	var raw []byte
	err := s.DB.QueryRow(ctx, `SELECT desired FROM iot_shard.shadow_desired WHERE sn=$1`, sn).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return DesiredLock{}, nil
	}
	if err != nil {
		return DesiredLock{}, fmt.Errorf("get desired: %w", err)
	}
	return ParseDesiredLock(raw), nil
}

// ParseDesiredLock 纯函数：从 desired JSON 取 lock 与 lock_expires_at（RFC3339 或 unix ms）。
func ParseDesiredLock(raw []byte) DesiredLock {
	var m struct {
		Lock      *bool           `json:"lock"`
		ExpiresAt json.RawMessage `json:"lock_expires_at"`
	}
	var d DesiredLock
	if json.Unmarshal(raw, &m) != nil {
		return d
	}
	d.Lock = m.Lock
	if len(m.ExpiresAt) > 0 && string(m.ExpiresAt) != "null" {
		var str string
		var ms int64
		if json.Unmarshal(m.ExpiresAt, &str) == nil {
			if t, err := time.Parse(time.RFC3339, str); err == nil {
				d.ExpiresAt = &t
			}
		} else if json.Unmarshal(m.ExpiresAt, &ms) == nil && ms > 0 {
			t := time.UnixMilli(ms)
			d.ExpiresAt = &t
		}
	}
	return d
}

// MergeDesired 与 deviceapi PGStore.MergeDesired 同一条 SQL（合并 JSONB、version+1）。
func (s *PGStore) MergeDesired(ctx context.Context, sn string, patch json.RawMessage) (int64, error) {
	var version int64
	err := s.DB.QueryRow(ctx, `
INSERT INTO iot_shard.shadow_desired (sn, desired, version, updated_at) VALUES ($1, $2::jsonb, 1, now())
ON CONFLICT (sn) DO UPDATE
   SET desired = shadow_desired.desired || EXCLUDED.desired,
       version = shadow_desired.version + 1,
       updated_at = now()
RETURNING version`, sn, string(patch)).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("merge desired: %w", err)
	}
	return version, nil
}

func (s *PGStore) InsertLockAudit(ctx context.Context, a LockAudit) error {
	if a.Source == "" {
		a.Source = "fleet"
	}
	_, err := s.DB.Exec(ctx, `INSERT INTO iot_shard.lock_audit(org_id,sn,action,actor,source,reason) VALUES($1,NULLIF($2,''),$3,$4,$5,NULLIF($6,''))`,
		a.OrgID, a.SN, a.Action, a.Actor, a.Source, a.Reason)
	if err != nil {
		return fmt.Errorf("lock audit: %w", err)
	}
	return nil
}

func (s *PGStore) ListLockAudit(ctx context.Context, orgID int64, sn string, since time.Time, limit int) ([]map[string]any, error) {
	rows, err := s.DB.Query(ctx, `SELECT id,sn,action,actor,source,reason,created_at FROM iot_shard.lock_audit
 WHERE org_id=$1 AND ($2='' OR sn=$2) AND created_at >= $3 ORDER BY created_at DESC LIMIT $4`, orgID, sn, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id int64
		var snv, reason *string
		var action, actor, source string
		var at time.Time
		if err := rows.Scan(&id, &snv, &action, &actor, &source, &reason, &at); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "sn": snv, "action": action, "actor": actor, "source": source, "reason": reason, "created_at": at})
	}
	return out, rows.Err()
}

// ---- 队列 ----

func (s *PGStore) CreateQueue(ctx context.Context, orgID, siteID int64, name string, sns []string, by int64) (*Queue, error) {
	q := &Queue{OrgID: orgID, SiteID: siteID, Name: name, DeviceSNs: sns}
	if sns == nil {
		q.DeviceSNs = []string{}
	}
	err := s.DB.QueryRow(ctx, `INSERT INTO iot_shard.job_queue(org_id,site_id,name,device_sns,created_by) VALUES($1,$2,$3,$4,$5) RETURNING queue_id`,
		orgID, siteID, name, q.DeviceSNs, by).Scan(&q.QueueID)
	if err != nil {
		return nil, fmt.Errorf("create queue: %w", err)
	}
	return q, nil
}

const queueCols = `queue_id,org_id,site_id,name,device_sns`

func scanQueue(row pgx.Row) (*Queue, error) {
	var q Queue
	if err := row.Scan(&q.QueueID, &q.OrgID, &q.SiteID, &q.Name, &q.DeviceSNs); err != nil {
		return nil, err
	}
	if q.DeviceSNs == nil {
		q.DeviceSNs = []string{}
	}
	return &q, nil
}

func (s *PGStore) GetQueue(ctx context.Context, orgID, queueID int64) (*Queue, error) {
	q, err := scanQueue(s.DB.QueryRow(ctx, `SELECT `+queueCols+` FROM iot_shard.job_queue WHERE org_id=$1 AND queue_id=$2`, orgID, queueID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return q, err
}

func (s *PGStore) queues(ctx context.Context, sql string, args ...any) ([]Queue, error) {
	rows, err := s.DB.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Queue{}
	for rows.Next() {
		q, err := scanQueue(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *q)
	}
	return out, rows.Err()
}

func (s *PGStore) ListQueues(ctx context.Context, orgID int64) ([]Queue, error) {
	return s.queues(ctx, `SELECT `+queueCols+` FROM iot_shard.job_queue WHERE org_id=$1 ORDER BY queue_id`, orgID)
}

func (s *PGStore) AllQueues(ctx context.Context) ([]Queue, error) {
	return s.queues(ctx, `SELECT `+queueCols+` FROM iot_shard.job_queue ORDER BY queue_id`)
}

func (s *PGStore) InsertItem(ctx context.Context, it Item) error {
	_, err := s.DB.Exec(ctx, `
INSERT INTO iot_shard.queue_item(item_id,org_id,queue_id,submitter_user_id,file_sha256,file_url,material_id,param_profile_id,est_minutes,status,job_id)
VALUES($1,$2,$3,$4,$5,NULLIF($6,''),NULLIF($7,''),NULLIF($8,''),$9,$10,$1)`,
		it.ItemID, it.OrgID, it.QueueID, it.Submitter, it.FileSHA256, it.FileURL, it.MaterialID, it.ParamProfileID, it.EstMinutes, it.Status)
	if err != nil {
		if sqlState(err) == "23514" || sqlState(err) == "22001" {
			return fmt.Errorf("%w: %v", ErrBadParam, err)
		}
		return fmt.Errorf("insert item: %w", err)
	}
	return nil
}

const itemCols = `item_id,org_id,queue_id,submitter_user_id,file_sha256,COALESCE(file_url,''),COALESCE(material_id,''),COALESCE(param_profile_id,''),est_minutes,status,
 COALESCE(approved_by,''),COALESCE(rejected_reason,''),COALESCE(assigned_sn,''),COALESCE(job_id,''),COALESCE(dispatched_cmd_id,''),retry,submitted_at,approved_at,dispatched_at`

func scanItem(row pgx.Row) (*Item, error) {
	var it Item
	if err := row.Scan(&it.ItemID, &it.OrgID, &it.QueueID, &it.Submitter, &it.FileSHA256, &it.FileURL, &it.MaterialID, &it.ParamProfileID, &it.EstMinutes, &it.Status,
		&it.ApprovedBy, &it.RejectedReason, &it.AssignedSN, &it.JobID, &it.DispatchedCmdID, &it.Retry, &it.SubmittedAt, &it.ApprovedAt, &it.DispatchedAt); err != nil {
		return nil, err
	}
	return &it, nil
}

func (s *PGStore) GetItem(ctx context.Context, orgID int64, itemID string) (*Item, error) {
	it, err := scanItem(s.DB.QueryRow(ctx, `SELECT `+itemCols+` FROM iot_shard.queue_item WHERE org_id=$1 AND item_id=$2`, orgID, itemID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return it, err
}

func (s *PGStore) ListItems(ctx context.Context, orgID, queueID int64, status string) ([]Item, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+itemCols+` FROM iot_shard.queue_item WHERE org_id=$1 AND queue_id=$2 AND ($3='' OR status=$3) ORDER BY submitted_at LIMIT 500`, orgID, queueID, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Item{}
	for rows.Next() {
		it, err := scanItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *it)
	}
	return out, rows.Err()
}

// SetStatus 带前置状态的条件 UPDATE；approved 时记 approved_by/approved_at，rejected 记原因。
func (s *PGStore) SetStatus(ctx context.Context, orgID int64, itemID, from, to, by, reason string) error {
	ct, err := s.DB.Exec(ctx, `
UPDATE iot_shard.queue_item SET status=$4::text,
   approved_by = CASE WHEN $4::text='approved' THEN $5 ELSE approved_by END,
   approved_at = CASE WHEN $4::text='approved' THEN now() ELSE approved_at END,
   rejected_reason = CASE WHEN $4::text='rejected' THEN NULLIF($6,'') ELSE rejected_reason END,
   finished_at = CASE WHEN $4::text IN ('done','failed','canceled','rejected','skipped') THEN now() ELSE finished_at END
 WHERE org_id=$1 AND item_id=$2 AND status=$3`, orgID, itemID, from, to, by, reason)
	if err != nil {
		return fmt.Errorf("set status: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

// ---- 调度器 ----

func (s *PGStore) HeadApproved(ctx context.Context, queueID int64) (*Item, error) {
	it, err := scanItem(s.DB.QueryRow(ctx, `SELECT `+itemCols+` FROM iot_shard.queue_item WHERE queue_id=$1 AND status='approved' ORDER BY approved_at, submitted_at LIMIT 1`, queueID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return it, err
}

func (s *PGStore) ActiveSNs(ctx context.Context, queueID int64) (map[string]bool, error) {
	rows, err := s.DB.Query(ctx, `SELECT assigned_sn FROM iot_shard.queue_item WHERE assigned_sn IS NOT NULL AND status IN ('assigned','dispatched','running')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var sn string
		if err := rows.Scan(&sn); err != nil {
			return nil, err
		}
		out[sn] = true
	}
	return out, rows.Err()
}

// ClaimDispatch 抢占：只有 status='approved' 的行才会被置为 dispatched（affected=1 才真正下发，INC-5-13）。
// uk_item_device_active 撞上（同设备已有进行中任务）视为未抢到。
func (s *PGStore) ClaimDispatch(ctx context.Context, itemID, sn, token string) (bool, error) {
	ct, err := s.DB.Exec(ctx, `
UPDATE iot_shard.queue_item SET status='dispatched', assigned_sn=$2, dispatched_cmd_id=$3, assigned_at=now(), dispatched_at=now()
 WHERE item_id=$1 AND status='approved'`, itemID, sn, token)
	if err != nil {
		if sqlState(err) == "23505" {
			return false, nil
		}
		return false, fmt.Errorf("claim dispatch: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

// ConfirmDispatch 把抢占令牌换成 deviceapi 返回的真实 cmd_id。
func (s *PGStore) ConfirmDispatch(ctx context.Context, itemID, token, cmdID string) error {
	_, err := s.DB.Exec(ctx, `UPDATE iot_shard.queue_item SET dispatched_cmd_id=$3 WHERE item_id=$1 AND dispatched_cmd_id=$2 AND status='dispatched'`, itemID, token, cmdID)
	if err != nil {
		return fmt.Errorf("confirm dispatch: %w", err)
	}
	return nil
}

// RevertDispatch 下发失败回 approved 并 retry+1；retry ≥ maxRetry → needs_teacher。
func (s *PGStore) RevertDispatch(ctx context.Context, itemID, token string, maxRetry int) error {
	_, err := s.DB.Exec(ctx, `
UPDATE iot_shard.queue_item SET retry=retry+1, assigned_sn=NULL, dispatched_cmd_id=NULL, assigned_at=NULL, dispatched_at=NULL,
   status = CASE WHEN retry+1 >= $3 THEN 'needs_teacher' ELSE 'approved' END
 WHERE item_id=$1 AND dispatched_cmd_id=$2 AND status='dispatched'`, itemID, token, maxRetry)
	if err != nil {
		return fmt.Errorf("revert dispatch: %w", err)
	}
	return nil
}

// ExpireItems：approved 超 approvedTTL、dispatched 超 dispatchedTTL 无进展 → skipped（任务书的 expired）。
func (s *PGStore) ExpireItems(ctx context.Context, approvedTTL, dispatchedTTL time.Duration, now time.Time) (int, error) {
	ct, err := s.DB.Exec(ctx, `
UPDATE iot_shard.queue_item SET status='skipped', skip_count=skip_count+1, finished_at=$1, rejected_reason='expired'
 WHERE (status='approved' AND COALESCE(approved_at, submitted_at) < $2)
    OR (status='dispatched' AND dispatched_at < $3)`, now, now.Add(-approvedTTL), now.Add(-dispatchedTTL))
	if err != nil {
		return 0, fmt.Errorf("expire items: %w", err)
	}
	return int(ct.RowsAffected()), nil
}

// ---- 看板 / 耗材 ----

func (s *PGStore) snSet(ctx context.Context, sql string, sns []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(sns) == 0 {
		return out, nil
	}
	rows, err := s.DB.Query(ctx, sql, sns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var sn string
		if err := rows.Scan(&sn); err != nil {
			return nil, err
		}
		out[sn] = true
	}
	return out, rows.Err()
}

func (s *PGStore) OpenAlarmSNs(ctx context.Context, sns []string) (map[string]bool, error) {
	return s.snSet(ctx, `SELECT DISTINCT sn FROM iot_shard.alarm WHERE sn = ANY($1) AND status IN ('open','notified')`, sns)
}

func (s *PGStore) PendingOTASNs(ctx context.Context, sns []string) (map[string]bool, error) {
	return s.snSet(ctx, `SELECT DISTINCT sn FROM iot_shard.ota_device_task WHERE sn = ANY($1) AND status IN ('pending','notified','downloading','verifying')`, sns)
}

func (s *PGStore) ConsumableHealth(ctx context.Context, orgID int64) ([]HealthRow, error) {
	rows, err := s.DB.Query(ctx, `
SELECT h.sn, h.part, h.health, h.predicted_eol_at FROM iot_shard.device_org d JOIN iot_shard.consumable_health h ON h.sn = d.sn
 WHERE d.org_id=$1 ORDER BY h.part, h.sn`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []HealthRow{}
	for rows.Next() {
		var r HealthRow
		if err := rows.Scan(&r.SN, &r.Part, &r.Health, &r.PredictedEOLAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func sqlState(err error) string {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState()
	}
	return ""
}
