package support

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store 抽象全部 PG 访问，便于 handler / service 单测注入 fake。
// 只读 BL1 / BL4 的表（device、device_binding、alarm、cmd_audit、ota_*、job_record），写 BL6 自己的表（sql/bl6.sql）。
type Store interface {
	// 设备与归属
	DeviceInfo(ctx context.Context, sn string) (DeviceInfo, error)
	IsOwner(ctx context.Context, sn string, userID int64) (bool, error)
	FWBySN(ctx context.Context, sns []string) (map[string]string, error)
	OnlineCount(ctx context.Context, productKey, fwVersion string, since time.Time) (int, error)

	// 诊断包数据源（PG 部分）
	RecentAlarms(ctx context.Context, sn string, since time.Time) ([]AlarmItem, error)
	RecentAudit(ctx context.Context, sn string, since time.Time, limit int) ([]AuditItem, error)
	CurrentOTA(ctx context.Context, sn string) (*OTAItem, error)
	RecentJobs(ctx context.Context, sn string, limit int) ([]JobItem, error)
	AuditResult(ctx context.Context, cmdID string) (string, error)

	// 诊断包
	InsertBundle(ctx context.Context, b Bundle) error
	GetBundle(ctx context.Context, id string) (Bundle, error)
	ListBundles(ctx context.Context, sn string, limit int) ([]BundleMeta, error)

	// 授权
	InsertGrant(ctx context.Context, g Grant) error
	GetGrant(ctx context.Context, id string) (Grant, error)
	// TransitionGrant 带前置状态的 UPDATE；affected=0 → ok=false（非法迁移）。
	TransitionGrant(ctx context.Context, id, to string, now time.Time) (bool, error)

	// 指令 → 工单
	InsertCmdTicket(ctx context.Context, m CmdTicket) error
	ListCmdTickets(ctx context.Context, ticketID string) ([]CmdTicket, error)

	// Agent 留痕
	InsertAgentCall(ctx context.Context, c AgentCall) error

	// 错误码字典
	DictPublish(ctx context.Context, e DictEntry) (version int64, err error)
	DictGet(ctx context.Context, code string) (DictEntry, error)
	DictLookup(ctx context.Context, codes []string) (map[string]DictEntry, error)

	// 批次缺陷
	DefectThreshold(ctx context.Context, productKey, code string) (threshold int, cooldown time.Duration, err error)
	LastDefectAlert(ctx context.Context, productKey, fwVersion, code string) (*time.Time, error)
	UpsertDefectAlert(ctx context.Context, a DefectAlert) (created bool, err error)
	ListDefectAlerts(ctx context.Context, since time.Time, limit int) ([]DefectAlert, error)

	// 保修
	WarrantyStats(ctx context.Context, sn string, now time.Time) (WarrantyStats, error)
	Baseline(ctx context.Context, productKey, metric string) (p90 float64, ok bool, err error)
	InsertWarranty(ctx context.Context, c WarrantyCase) error
}

// PGStore 是 Store 的 PostgreSQL 实现。
type PGStore struct{ DB *pgxpool.Pool }

var _ Store = (*PGStore)(nil)

// translate 把 PG 错误翻译为包内错误：唯一冲突 → ErrConflict；CHECK 拒绝 → ErrDenied（审批约束）或 ErrBadParam。
func translate(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return fmt.Errorf("%w: %s", ErrConflict, pgErr.ConstraintName)
		case "23514":
			if strings.Contains(pgErr.ConstraintName, "approved") {
				return fmt.Errorf("%w: %s", ErrDenied, pgErr.ConstraintName)
			}
			return fmt.Errorf("%w: %s", ErrBadParam, pgErr.ConstraintName)
		case "23503":
			return fmt.Errorf("%w: %s", ErrBadParam, pgErr.ConstraintName)
		}
	}
	return err
}

// ---------- 设备与归属 ----------

func (s *PGStore) DeviceInfo(ctx context.Context, sn string) (DeviceInfo, error) {
	var d DeviceInfo
	var fw *string
	err := s.DB.QueryRow(ctx, `SELECT product_key, fw_version, status, activated_at, last_online_at FROM iot_shard.device WHERE sn=$1`, sn).
		Scan(&d.ProductKey, &fw, &d.Status, &d.ActivatedAt, &d.LastOnlineAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, fmt.Errorf("%w: device %s", ErrNotFound, sn)
	}
	if fw != nil {
		d.FWVersion = *fw
	}
	return d, err
}

func (s *PGStore) IsOwner(ctx context.Context, sn string, userID int64) (bool, error) {
	var n int
	err := s.DB.QueryRow(ctx, `SELECT count(*) FROM iot_shard.device_binding WHERE sn=$1 AND user_id=$2 AND role='owner' AND unbound_at IS NULL`, sn, userID).Scan(&n)
	return n > 0, err
}

func (s *PGStore) FWBySN(ctx context.Context, sns []string) (map[string]string, error) {
	out := map[string]string{}
	if len(sns) == 0 {
		return out, nil
	}
	rows, err := s.DB.Query(ctx, `SELECT sn, COALESCE(fw_version,'') FROM iot_shard.device WHERE sn = ANY($1)`, sns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var sn, fw string
		if err := rows.Scan(&sn, &fw); err != nil {
			return nil, err
		}
		out[sn] = fw
	}
	return out, rows.Err()
}

func (s *PGStore) OnlineCount(ctx context.Context, productKey, fwVersion string, since time.Time) (int, error) {
	var n int
	q := `SELECT count(*) FROM iot_shard.device WHERE product_key=$1 AND last_online_at >= $3 AND `
	if fwVersion == UnknownFW {
		q += `fw_version IS NULL AND $2 = $2`
	} else {
		q += `fw_version = $2`
	}
	err := s.DB.QueryRow(ctx, q, productKey, fwVersion, since).Scan(&n)
	return n, err
}

// ---------- 诊断包数据源 ----------

func (s *PGStore) RecentAlarms(ctx context.Context, sn string, since time.Time) ([]AlarmItem, error) {
	rows, err := s.DB.Query(ctx, `SELECT id, code, level, status, event_ts FROM iot_shard.alarm WHERE sn=$1 AND event_ts >= $2 ORDER BY event_ts DESC LIMIT 50`, sn, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AlarmItem{}
	for rows.Next() {
		var a AlarmItem
		if err := rows.Scan(&a.ID, &a.Code, &a.Level, &a.Status, &a.EventTs); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *PGStore) RecentAudit(ctx context.Context, sn string, since time.Time, limit int) ([]AuditItem, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.DB.Query(ctx, `SELECT cmd_id, action, source, operator, created_at, result FROM iot_shard.cmd_audit WHERE sn=$1 AND created_at >= $2 ORDER BY created_at DESC LIMIT $3`, sn, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AuditItem{}
	for rows.Next() {
		var a AuditItem
		if err := rows.Scan(&a.CmdID, &a.Action, &a.Source, &a.Operator, &a.CreatedAt, &a.Result); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *PGStore) CurrentOTA(ctx context.Context, sn string) (*OTAItem, error) {
	var o OTAItem
	err := s.DB.QueryRow(ctx, `
SELECT t.batch_id, t.status, COALESCE(f.version,''), t.updated_at
  FROM iot_shard.ota_device_task t
  JOIN iot_global.ota_batch b ON b.id = t.batch_id
  JOIN iot_global.firmware f ON f.id = b.firmware_id
 WHERE t.sn=$1 ORDER BY t.updated_at DESC LIMIT 1`, sn).Scan(&o.BatchID, &o.Status, &o.FwTo, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (s *PGStore) RecentJobs(ctx context.Context, sn string, limit int) ([]JobItem, error) {
	if limit <= 0 {
		limit = 5
	}
	rows, err := s.DB.Query(ctx, `SELECT job_id, COALESCE(material_id,''), COALESCE(param_profile_id,''), COALESCE(outcome,''), started_at FROM iot_shard.job_record WHERE sn=$1 ORDER BY started_at DESC LIMIT $2`, sn, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []JobItem{}
	for rows.Next() {
		var j JobItem
		if err := rows.Scan(&j.JobID, &j.MaterialID, &j.ParamProfileID, &j.Outcome, &j.StartedAt); err != nil {
			return nil, err
		}
		j.JobID = strings.TrimSpace(j.JobID)
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *PGStore) AuditResult(ctx context.Context, cmdID string) (string, error) {
	var res string
	err := s.DB.QueryRow(ctx, `SELECT result FROM iot_shard.cmd_audit WHERE cmd_id=$1 ORDER BY created_at DESC LIMIT 1`, cmdID).Scan(&res)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return res, err
}

// ---------- 诊断包 ----------

func (s *PGStore) InsertBundle(ctx context.Context, b Bundle) error {
	content, err := json.Marshal(b.Content)
	if err != nil {
		return err
	}
	sources, _ := json.Marshal(b.Sources)
	_, err = s.DB.Exec(ctx, `INSERT INTO iot_shard.diagnostic_bundle(bundle_id, sn, trigger, ticket_id, content, sources, created_at, expires_at) VALUES($1,$2,$3,NULLIF($4,''),$5,$6,$7,$8)`,
		b.BundleID, b.SN, b.Trigger, b.TicketID, content, sources, b.CreatedAt, b.ExpiresAt)
	return translate(err)
}

func (s *PGStore) GetBundle(ctx context.Context, id string) (Bundle, error) {
	var b Bundle
	var content, sources []byte
	var ticket *string
	err := s.DB.QueryRow(ctx, `SELECT bundle_id, sn, trigger, ticket_id, content, sources, created_at, expires_at FROM iot_shard.diagnostic_bundle WHERE bundle_id=$1`, id).
		Scan(&b.BundleID, &b.SN, &b.Trigger, &ticket, &content, &sources, &b.CreatedAt, &b.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, fmt.Errorf("%w: bundle %s", ErrNotFound, id)
	}
	if err != nil {
		return b, err
	}
	b.BundleID = strings.TrimSpace(b.BundleID)
	if ticket != nil {
		b.TicketID = *ticket
	}
	if err := json.Unmarshal(content, &b.Content); err != nil {
		return b, err
	}
	_ = json.Unmarshal(sources, &b.Sources)
	return b, nil
}

func (s *PGStore) ListBundles(ctx context.Context, sn string, limit int) ([]BundleMeta, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.DB.Query(ctx, `SELECT bundle_id, trigger, COALESCE(ticket_id,''), created_at, expires_at, COALESCE((content->>'degraded')::boolean, false) FROM iot_shard.diagnostic_bundle WHERE sn=$1 ORDER BY created_at DESC LIMIT $2`, sn, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []BundleMeta{}
	for rows.Next() {
		var m BundleMeta
		if err := rows.Scan(&m.BundleID, &m.Trigger, &m.TicketID, &m.CreatedAt, &m.ExpiresAt, &m.Degraded); err != nil {
			return nil, err
		}
		m.BundleID = strings.TrimSpace(m.BundleID)
		out = append(out, m)
	}
	return out, rows.Err()
}

// ---------- 授权 ----------

const grantCols = `grant_id, sn, ticket_id, operator, actions, status, requested_at, granted_at, expires_at, revoked_at, denied_at`

func scanGrant(row pgx.Row) (Grant, error) {
	var g Grant
	err := row.Scan(&g.GrantID, &g.SN, &g.TicketID, &g.Operator, &g.Actions, &g.Status, &g.RequestedAt, &g.GrantedAt, &g.ExpiresAt, &g.RevokedAt, &g.DeniedAt)
	g.GrantID = strings.TrimSpace(g.GrantID)
	return g, err
}

func (s *PGStore) InsertGrant(ctx context.Context, g Grant) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO iot_shard.support_grant(grant_id, sn, ticket_id, operator, actions, status, requested_at) VALUES($1,$2,$3,$4,$5,$6,$7)`,
		g.GrantID, g.SN, g.TicketID, g.Operator, g.Actions, g.Status, g.RequestedAt)
	return translate(err)
}

func (s *PGStore) GetGrant(ctx context.Context, id string) (Grant, error) {
	g, err := scanGrant(s.DB.QueryRow(ctx, `SELECT `+grantCols+` FROM iot_shard.support_grant WHERE grant_id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return g, fmt.Errorf("%w: grant %s", ErrNotFound, id)
	}
	return g, err
}

// TransitionGrant：目标状态决定要写的时间列；WHERE status IN (可迁往 to 的前置状态) 即数据库守卫。
func (s *PGStore) TransitionGrant(ctx context.Context, id, to string, now time.Time) (bool, error) {
	var set string
	switch to {
	// $3 同时用于赋值与时间运算时 PG 推断不出唯一类型（42P08 inconsistent types deduced），必须显式 ::timestamptz。
	case GrantGranted:
		set = `status=$2, granted_at=$3::timestamptz, expires_at=$3::timestamptz + make_interval(secs => $4::double precision)`
	case GrantDenied:
		set = `status=$2, denied_at=$3::timestamptz`
	case GrantRevoked:
		set = `status=$2, revoked_at=$3::timestamptz`
	default:
		return false, fmt.Errorf("%w: unknown target status %q", ErrBadParam, to)
	}
	sources := GrantSources(to)
	if len(sources) == 0 {
		return false, nil
	}
	args := []any{id, to, now}
	if to == GrantGranted {
		args = append(args, GrantTTL.Seconds())
	}
	args = append(args, sources)
	tag, err := s.DB.Exec(ctx, `UPDATE iot_shard.support_grant SET `+set+` WHERE grant_id=$1 AND status = ANY($`+fmt.Sprint(len(args))+`)`, args...)
	if err != nil {
		return false, translate(err)
	}
	return tag.RowsAffected() == 1, nil
}

// ---------- 指令 → 工单 ----------

func (s *PGStore) InsertCmdTicket(ctx context.Context, m CmdTicket) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO iot_shard.cmd_ticket_map(cmd_id, ticket_id, grant_id, sn, source, created_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (cmd_id) DO NOTHING`,
		m.CmdID, m.TicketID, m.GrantID, m.SN, m.Source, m.CreatedAt)
	return translate(err)
}

func (s *PGStore) ListCmdTickets(ctx context.Context, ticketID string) ([]CmdTicket, error) {
	rows, err := s.DB.Query(ctx, `SELECT cmd_id, ticket_id, grant_id, sn, source, created_at FROM iot_shard.cmd_ticket_map WHERE ticket_id=$1 ORDER BY created_at DESC LIMIT 200`, ticketID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []CmdTicket{}
	for rows.Next() {
		var m CmdTicket
		if err := rows.Scan(&m.CmdID, &m.TicketID, &m.GrantID, &m.SN, &m.Source, &m.CreatedAt); err != nil {
			return nil, err
		}
		m.GrantID = strings.TrimSpace(m.GrantID)
		out = append(out, m)
	}
	return out, rows.Err()
}

// ---------- Agent 留痕 ----------

func (s *PGStore) InsertAgentCall(ctx context.Context, c AgentCall) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO iot_shard.agent_call_log(agent_id, ticket_id, sn, endpoint, grant_id, created_at) VALUES($1,NULLIF($2,''),$3,$4,NULLIF($5,''),$6)`,
		c.AgentID, c.TicketID, c.SN, c.Endpoint, c.GrantID, c.CreatedAt)
	return err
}

// ---------- 错误码字典 ----------

const dictCols = `code, version, product_keys, severity, cause, steps, need_service, defect_threshold, created_by, COALESCE(approved_by,''), released_at`

func scanDict(row pgx.Row) (DictEntry, error) {
	var e DictEntry
	err := row.Scan(&e.Code, &e.Version, &e.ProductKeys, &e.Severity, &e.Cause, &e.Steps, &e.NeedService, &e.DefectThreshold, &e.CreatedBy, &e.ApprovedBy, &e.ReleasedAt)
	return e, err
}

func (s *PGStore) DictPublish(ctx context.Context, e DictEntry) (int64, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var v int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(version),0)+1 FROM iot_global.error_code_dict WHERE code=$1`, e.Code).Scan(&v); err != nil {
		return 0, err
	}
	if e.ProductKeys == nil {
		e.ProductKeys = []string{}
	}
	_, err = tx.Exec(ctx, `INSERT INTO iot_global.error_code_dict(code, version, product_keys, severity, cause, steps, need_service, defect_threshold, created_by, approved_by, released_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, e.Code, v, e.ProductKeys, e.Severity, e.Cause, e.Steps, e.NeedService, e.DefectThreshold, e.CreatedBy, e.ApprovedBy, e.ReleasedAt)
	if err != nil {
		return 0, translate(err)
	}
	return v, tx.Commit(ctx)
}

func (s *PGStore) DictGet(ctx context.Context, code string) (DictEntry, error) {
	e, err := scanDict(s.DB.QueryRow(ctx, `SELECT `+dictCols+` FROM iot_global.error_code_dict WHERE code=$1 ORDER BY version DESC LIMIT 1`, code))
	if errors.Is(err, pgx.ErrNoRows) {
		return e, fmt.Errorf("%w: code %s", ErrNotFound, code)
	}
	return e, err
}

func (s *PGStore) DictLookup(ctx context.Context, codes []string) (map[string]DictEntry, error) {
	out := map[string]DictEntry{}
	if len(codes) == 0 {
		return out, nil
	}
	rows, err := s.DB.Query(ctx, `SELECT DISTINCT ON (code) `+dictCols+` FROM iot_global.error_code_dict WHERE code = ANY($1) ORDER BY code, version DESC`, codes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		e, err := scanDict(rows)
		if err != nil {
			return nil, err
		}
		out[e.Code] = e
	}
	return out, rows.Err()
}

// ---------- 批次缺陷 ----------

// DefectThreshold：字典 defect_threshold → 机型 defect_threshold 表 → 代码默认 20 / 24h。
func (s *PGStore) DefectThreshold(ctx context.Context, productKey, code string) (int, time.Duration, error) {
	threshold, cooldown := DefaultDefectThreshold, DefaultDefectCooldown
	var minDevices, cooldownHours *int
	err := s.DB.QueryRow(ctx, `SELECT min_devices, cooldown_hours FROM iot_global.defect_threshold WHERE product_key=$1`, productKey).Scan(&minDevices, &cooldownHours)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, err
	}
	if minDevices != nil && *minDevices > 0 {
		threshold = *minDevices
	}
	if cooldownHours != nil && *cooldownHours > 0 {
		cooldown = time.Duration(*cooldownHours) * time.Hour
	}
	var dictThreshold *int
	err = s.DB.QueryRow(ctx, `SELECT defect_threshold FROM iot_global.error_code_dict WHERE code=$1 ORDER BY version DESC LIMIT 1`, code).Scan(&dictThreshold)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, err
	}
	if dictThreshold != nil && *dictThreshold > 0 {
		threshold = *dictThreshold
	}
	return threshold, cooldown, nil
}

func (s *PGStore) LastDefectAlert(ctx context.Context, productKey, fwVersion, code string) (*time.Time, error) {
	var t *time.Time
	err := s.DB.QueryRow(ctx, `SELECT max(created_at) FROM iot_global.defect_alert WHERE product_key=$1 AND fw_version=$2 AND error_code=$3`, productKey, fwVersion, code).Scan(&t)
	return t, err
}

// UpsertDefectAlert：同 (product_key, fw_version, error_code, window_start) 只建一次；已存在则更新计数并返回 created=false。
func (s *PGStore) UpsertDefectAlert(ctx context.Context, a DefectAlert) (bool, error) {
	var created bool
	err := s.DB.QueryRow(ctx, `
INSERT INTO iot_global.defect_alert(product_key, fw_version, error_code, device_count, event_count, online_count, first_seen, window_start, cooldown_until, created_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (product_key, fw_version, error_code, window_start) DO UPDATE
   SET device_count = EXCLUDED.device_count, event_count = EXCLUDED.event_count, online_count = EXCLUDED.online_count
RETURNING (xmax = 0)`, a.ProductKey, a.FWVersion, a.ErrorCode, a.DeviceCount, a.EventCount, a.OnlineCount, a.FirstSeen, a.WindowStart, a.CooldownUntil, a.CreatedAt).Scan(&created)
	return created, translate(err)
}

func (s *PGStore) ListDefectAlerts(ctx context.Context, since time.Time, limit int) ([]DefectAlert, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.DB.Query(ctx, `SELECT id, product_key, fw_version, error_code, device_count, event_count, online_count, first_seen, window_start, COALESCE(ticket_id,''), notified_at, cooldown_until, created_at
		FROM iot_global.defect_alert WHERE created_at >= $1 ORDER BY created_at DESC LIMIT $2`, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []DefectAlert{}
	for rows.Next() {
		var a DefectAlert
		if err := rows.Scan(&a.ID, &a.ProductKey, &a.FWVersion, &a.ErrorCode, &a.DeviceCount, &a.EventCount, &a.OnlineCount, &a.FirstSeen, &a.WindowStart, &a.TicketID, &a.NotifiedAt, &a.CooldownUntil, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---------- 保修 ----------

// WarrantyStats 只从 PG 汇总原始数据；laser_hours 由 service 从影子补充。
func (s *PGStore) WarrantyStats(ctx context.Context, sn string, now time.Time) (WarrantyStats, error) {
	st := WarrantyStats{SafetyEvents30d: map[string]int{}, ErrorCodesTop: []CodeCount{}}
	var fw *string
	err := s.DB.QueryRow(ctx, `SELECT product_key, fw_version, activated_at FROM iot_shard.device WHERE sn=$1`, sn).Scan(&st.productKey, &fw, &st.ActivatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, fmt.Errorf("%w: device %s", ErrNotFound, sn)
	}
	if err != nil {
		return st, err
	}
	if fw != nil {
		st.FWVersion = *fw
	}
	// 安全事件 30 天：alarm 表按 code 计数
	rows, err := s.DB.Query(ctx, `SELECT code, count(*) FROM iot_shard.alarm WHERE sn=$1 AND event_ts >= $2 GROUP BY code`, sn, now.Add(-30*24*time.Hour))
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var code string
		var n int
		if err := rows.Scan(&code, &n); err != nil {
			rows.Close()
			return st, err
		}
		st.SafetyEvents30d[code] = n
	}
	rows.Close()
	// OTA 终态分布
	rows, err = s.DB.Query(ctx, `SELECT status, count(*) FROM iot_shard.ota_device_task WHERE sn=$1 GROUP BY status`, sn)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return st, err
		}
		switch status {
		case "success":
			st.OTASuccess = n
		case "failed":
			st.OTAFailed = n
		case "rolled_back":
			st.OTARolledBack = n
		}
	}
	rows.Close()
	// 加工记录：非官方参数占比（只有 opt-in 用户才有记录）
	err = s.DB.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE param_profile_id IS NULL OR param_profile_id NOT LIKE 'official%') FROM iot_shard.job_record WHERE sn=$1`, sn).
		Scan(&st.Jobs, &st.JobsNonOfficial)
	if err != nil {
		return st, err
	}
	st.JobsOptInPresent = st.Jobs > 0
	return st, nil
}

func (s *PGStore) Baseline(ctx context.Context, productKey, metric string) (float64, bool, error) {
	var p90 *float64
	err := s.DB.QueryRow(ctx, `SELECT p90::float8 FROM iot_global.warranty_baseline WHERE product_key=$1 AND metric=$2`, productKey, metric).Scan(&p90)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && p90 == nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return *p90, true, nil
}

func (s *PGStore) InsertWarranty(ctx context.Context, c WarrantyCase) error {
	summary, _ := json.Marshal(c.Summary)
	signals, _ := json.Marshal(c.Signals)
	_, err := s.DB.Exec(ctx, `INSERT INTO iot_shard.warranty_case(case_id, sn, ticket_id, summary, signals, generated_at) VALUES($1,$2,NULLIF($3,''),$4,$5,$6)`,
		c.CaseID, c.SN, c.TicketID, summary, signals, c.GeneratedAt)
	return translate(err)
}
