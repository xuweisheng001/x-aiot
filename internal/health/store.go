package health

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// 业务错误（handler 翻译为 HTTP 状态）。
var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
	ErrBadParam = errors.New("bad param")
	ErrDenied   = errors.New("denied")
)

// HealthRow 是 iot_shard.consumable_health 的一行（used_hours 存加权时长）。
type HealthRow struct {
	SN             string     `json:"sn"`
	Part           string     `json:"part"`
	Health         float64    `json:"health"`
	UsedHours      float64    `json:"used_hours"`
	PredictedEOLAt *time.Time `json:"predicted_eol_at"`
	NotifiedAt     *time.Time `json:"notified_at,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// Explain 是 iot_shard.health_explain 的一行（「为什么是 62 分」）。
type Explain struct {
	SN            string     `json:"-"`
	Part          string     `json:"-"`
	ModelVersion  int        `json:"model_version"`
	UsedWeighted  float64    `json:"used_weighted"`
	Rated         float64    `json:"rated"`
	ShareLow      float64    `json:"share_low"`
	ShareMid      float64    `json:"share_mid"`
	ShareHigh     float64    `json:"share_high"`
	OvertempCount int        `json:"overtemp_count"`
	Penalty       float64    `json:"penalty"`
	LastBucketTs  *time.Time `json:"last_bucket_ts"`
	LastHours     *float64   `json:"last_hours"`
	ModuleModel   string     `json:"module_model"`
	LevelsSent    []int      `json:"levels_sent"`
	ComputedAt    time.Time  `json:"computed_at"`
}

// Swap 是一次换模块记录（iot_shard.module_swap）。
type Swap struct {
	FromModel, ToModel, Reason string
	PredictedEOLBefore         *time.Time
}

// HealthUpdate 是一台设备一轮计算的写入单元：consumable_health + health_explain（+ module_swap），单事务。
type HealthUpdate struct {
	Row     HealthRow
	Explain Explain
	Swap    *Swap
}

// Reminder 是 iot_shard.health_reminder 的一行；ID 即 reminder_id。
type Reminder struct {
	ID               int64      `json:"id"`
	SN               string     `json:"sn"`
	Part             string     `json:"part"`
	Level            string     `json:"level"`
	HealthAt         float64    `json:"health_at"`
	SkuID            string     `json:"sku_id,omitempty"`
	SentAt           *time.Time `json:"sent_at"`
	SuppressedReason string     `json:"suppressed_reason,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

// SKU 是 iot_global.sku_mapping 的一行。
type SKU struct {
	ID          int64  `json:"id"`
	Part        string `json:"part"`
	ProductKey  string `json:"product_key"`
	ModuleModel string `json:"module_model"`
	SkuID       string `json:"sku_id"`
	Title       string `json:"title"`
	CompatNote  string `json:"compat_note,omitempty"`
	Sellable    bool   `json:"sellable"`
}

// MaterialCode 是 iot_global.material_code 的一行。
type MaterialCode struct {
	CodeID     string
	MaterialID string
	Batch      string
	Seq        int64
	Status     string
}

// MaterialInfo 是 material_index ⋈ material 的结果。
type MaterialInfo struct {
	MaterialID  string
	ThicknessMM *float64
}

// Bucket 是一个 (sn, 小时) 桶：与 telemetry_1h 流同口径（last laser_hours、avg_power、share_low/mid/high）。
// 指针为 nil 表示该列缺失（NULL）。
type Bucket struct {
	SN, ProductKey                string
	Ts                            time.Time
	LaserHours                    *float64
	AvgPower                      *float64
	ShareLow, ShareMid, ShareHigh *float64
}

// Reported 是影子 reported 中健康度关心的字段。
type Reported struct {
	Found       bool
	ModuleModel string
	WorkState   int
	Optout      bool // consumable_notify_optout
}

// Store 抽象 PG，便于 handler / service 单测注入 fake。
type Store interface {
	HealthRow(ctx context.Context, sn, part string) (*HealthRow, error) // 无行返回 nil, nil
	Explain(ctx context.Context, sn, part string) (*Explain, error)     // 无行返回 nil, nil
	Cfg(ctx context.Context, productKey, moduleModel string) (Cfg, bool, error)
	WriteHealth(ctx context.Context, up HealthUpdate) error
	LastSentAt(ctx context.Context, sn, part string) (*time.Time, error)
	InsertReminder(ctx context.Context, r *Reminder) error // 回填 ID / CreatedAt
	Reminders(ctx context.Context, sn string) ([]Reminder, error)
	Reminder(ctx context.Context, id int64) (*Reminder, error) // 无行 → ErrNotFound
	SKUs(ctx context.Context, productKey, part string) ([]SKU, error)
	Click(ctx context.Context, reminderID, userID int64, skuID string, now time.Time) error
	Attribute(ctx context.Context, reminderID int64, orderNo, skuID string, now time.Time) error
	MaterialCode(ctx context.Context, codeID string) (*MaterialCode, error) // 无行 → ErrNotFound
	MaterialByIdx(ctx context.Context, idx uint32) (*MaterialInfo, error)   // 无行 → ErrNotFound
	InsertMaterialCode(ctx context.Context, mc MaterialCode) error
	// ConsumeScan 在事务内：锁码行 → 状态 / 次数 / distinct user 判定 → 记 material_code_scan；ok=false 时 reason 为 unknown|revoked|suspicious|replay。
	ConsumeScan(ctx context.Context, codeID string, userID *int64, sn string, maxScans int, now time.Time) (ok bool, reason string, err error)
}

// Source 抽象 TDengine 输入。
type Source interface {
	Buckets(ctx context.Context, since time.Time) ([]Bucket, error)
	OvertempCounts(ctx context.Context, since time.Time) (map[string]int, error)
}

// ShadowReader 抽象影子读取。
type ShadowReader interface {
	Reported(ctx context.Context, sn string) (Reported, error)
}

// Notifier 抽象通知发布（JetStream IOT_NOTIFY）。
type Notifier interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// ===================== PG =====================

// PGStore 是 Store 的 PostgreSQL 实现。
type PGStore struct{ DB *pgxpool.Pool }

func (s *PGStore) HealthRow(ctx context.Context, sn, part string) (*HealthRow, error) {
	var r HealthRow
	err := s.DB.QueryRow(ctx, `SELECT sn, part, health::float8, COALESCE(used_hours,0)::float8, predicted_eol_at, notified_at, updated_at
		FROM iot_shard.consumable_health WHERE sn=$1 AND part=$2`, sn, part).
		Scan(&r.SN, &r.Part, &r.Health, &r.UsedHours, &r.PredictedEOLAt, &r.NotifiedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("health row: %w", err)
	}
	return &r, nil
}

func (s *PGStore) Explain(ctx context.Context, sn, part string) (*Explain, error) {
	var e Explain
	var levels string
	err := s.DB.QueryRow(ctx, `SELECT sn, part, model_version, used_weighted::float8, rated::float8,
		COALESCE(share_low,0)::float8, COALESCE(share_mid,0)::float8, COALESCE(share_high,0)::float8, overtemp_count, penalty::float8,
		last_bucket_ts, last_hours::float8, COALESCE(module_model,''), levels_sent, computed_at
		FROM iot_shard.health_explain WHERE sn=$1 AND part=$2`, sn, part).
		Scan(&e.SN, &e.Part, &e.ModelVersion, &e.UsedWeighted, &e.Rated, &e.ShareLow, &e.ShareMid, &e.ShareHigh, &e.OvertempCount, &e.Penalty,
			&e.LastBucketTs, &e.LastHours, &e.ModuleModel, &levels, &e.ComputedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("explain: %w", err)
	}
	e.LevelsSent = ParseLevels(levels)
	return &e, nil
}

func (s *PGStore) Cfg(ctx context.Context, productKey, moduleModel string) (Cfg, bool, error) {
	var c Cfg
	err := s.DB.QueryRow(ctx, `SELECT rated_weighted_hours::float8, w_low::float8, w_mid::float8, w_high::float8, overtemp_penalty::float8, version
		FROM iot_global.health_model_cfg WHERE product_key=$1 AND module_model=$2 ORDER BY version DESC LIMIT 1`, productKey, moduleModel).
		Scan(&c.Rated, &c.WLow, &c.WMid, &c.WHigh, &c.OvertempPenalty, &c.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return Cfg{}, false, nil
	}
	if err != nil {
		return Cfg{}, false, fmt.Errorf("cfg: %w", err)
	}
	return c, true, nil
}

// WriteHealth 单事务 UPSERT consumable_health + health_explain，换模块时追加 module_swap。
func (s *PGStore) WriteHealth(ctx context.Context, up HealthUpdate) error {
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	r, e := up.Row, up.Explain
	if _, err := tx.Exec(ctx, `INSERT INTO iot_shard.consumable_health(sn, part, health, used_hours, predicted_eol_at, updated_at)
		VALUES($1,$2,$3,$4,$5,$6)
		ON CONFLICT (sn, part) DO UPDATE SET health=EXCLUDED.health, used_hours=EXCLUDED.used_hours,
		  predicted_eol_at=EXCLUDED.predicted_eol_at, updated_at=EXCLUDED.updated_at`,
		r.SN, r.Part, r.Health, r.UsedHours, r.PredictedEOLAt, r.UpdatedAt); err != nil {
		return fmt.Errorf("upsert consumable_health: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO iot_shard.health_explain(sn, part, model_version, used_weighted, rated, share_low, share_mid, share_high,
		  overtemp_count, penalty, last_bucket_ts, last_hours, module_model, levels_sent, computed_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT (sn, part) DO UPDATE SET model_version=EXCLUDED.model_version, used_weighted=EXCLUDED.used_weighted, rated=EXCLUDED.rated,
		  share_low=EXCLUDED.share_low, share_mid=EXCLUDED.share_mid, share_high=EXCLUDED.share_high, overtemp_count=EXCLUDED.overtemp_count,
		  penalty=EXCLUDED.penalty, last_bucket_ts=EXCLUDED.last_bucket_ts, last_hours=EXCLUDED.last_hours, module_model=EXCLUDED.module_model,
		  levels_sent=EXCLUDED.levels_sent, computed_at=EXCLUDED.computed_at`,
		r.SN, r.Part, e.ModelVersion, e.UsedWeighted, e.Rated, e.ShareLow, e.ShareMid, e.ShareHigh, e.OvertempCount, e.Penalty,
		e.LastBucketTs, e.LastHours, e.ModuleModel, FormatLevels(e.LevelsSent), e.ComputedAt); err != nil {
		return fmt.Errorf("upsert health_explain: %w", err)
	}
	if up.Swap != nil {
		if _, err := tx.Exec(ctx, `INSERT INTO iot_shard.module_swap(sn, detected_at, from_model, to_model, reason, predicted_eol_before) VALUES($1,$2,$3,$4,$5,$6)`,
			r.SN, r.UpdatedAt, up.Swap.FromModel, up.Swap.ToModel, up.Swap.Reason, up.Swap.PredictedEOLBefore); err != nil {
			return fmt.Errorf("insert module_swap: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// LastSentAt 用 max(sent_at)（INC-3-09：不用最后一行，因为 suppressed 行 sent_at 为 NULL）。
func (s *PGStore) LastSentAt(ctx context.Context, sn, part string) (*time.Time, error) {
	var t *time.Time
	if err := s.DB.QueryRow(ctx, `SELECT max(sent_at) FROM iot_shard.health_reminder WHERE sn=$1 AND part=$2`, sn, part).Scan(&t); err != nil {
		return nil, fmt.Errorf("last sent: %w", err)
	}
	return t, nil
}

func (s *PGStore) InsertReminder(ctx context.Context, r *Reminder) error {
	var sku, reason *string
	if r.SkuID != "" {
		sku = &r.SkuID
	}
	if r.SuppressedReason != "" {
		reason = &r.SuppressedReason
	}
	err := s.DB.QueryRow(ctx, `INSERT INTO iot_shard.health_reminder(sn, part, level, health_at, sku_id, sent_at, suppressed_reason)
		VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id, created_at`, r.SN, r.Part, r.Level, r.HealthAt, sku, r.SentAt, reason).Scan(&r.ID, &r.CreatedAt)
	if err != nil {
		return translatePG(err)
	}
	return nil
}

const reminderCols = `id, sn, part, level, COALESCE(health_at,0)::float8, COALESCE(sku_id,''), sent_at, COALESCE(suppressed_reason,''), created_at`

func scanReminder(row pgx.Row) (Reminder, error) {
	var r Reminder
	err := row.Scan(&r.ID, &r.SN, &r.Part, &r.Level, &r.HealthAt, &r.SkuID, &r.SentAt, &r.SuppressedReason, &r.CreatedAt)
	return r, err
}

func (s *PGStore) Reminders(ctx context.Context, sn string) ([]Reminder, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+reminderCols+` FROM iot_shard.health_reminder WHERE sn=$1 ORDER BY created_at DESC, id DESC LIMIT 200`, sn)
	if err != nil {
		return nil, fmt.Errorf("reminders: %w", err)
	}
	defer rows.Close()
	var out []Reminder
	for rows.Next() {
		r, err := scanReminder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PGStore) Reminder(ctx context.Context, id int64) (*Reminder, error) {
	r, err := scanReminder(s.DB.QueryRow(ctx, `SELECT `+reminderCols+` FROM iot_shard.health_reminder WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: reminder %d", ErrNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("reminder: %w", err)
	}
	return &r, nil
}

func (s *PGStore) SKUs(ctx context.Context, productKey, part string) ([]SKU, error) {
	rows, err := s.DB.Query(ctx, `SELECT id, part, product_key, module_model, sku_id, title, COALESCE(compat_note,''), sellable
		FROM iot_global.sku_mapping WHERE product_key=$1 AND ($2='' OR part=$2) ORDER BY part, module_model`, productKey, part)
	if err != nil {
		return nil, fmt.Errorf("skus: %w", err)
	}
	defer rows.Close()
	var out []SKU
	for rows.Next() {
		var k SKU
		if err := rows.Scan(&k.ID, &k.Part, &k.ProductKey, &k.ModuleModel, &k.SkuID, &k.Title, &k.CompatNote, &k.Sellable); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// Click 幂等：首个点击的 user_id / clicked_at 保留。reminder 不存在由 FK 拒绝 → ErrNotFound。
func (s *PGStore) Click(ctx context.Context, reminderID, userID int64, skuID string, now time.Time) error {
	var sku *string
	if skuID != "" {
		sku = &skuID
	}
	_, err := s.DB.Exec(ctx, `INSERT INTO iot_shard.reminder_attribution(reminder_id, user_id, sku_id, clicked_at) VALUES($1,$2,$3,$4)
		ON CONFLICT (reminder_id) DO UPDATE SET user_id=COALESCE(reminder_attribution.user_id, EXCLUDED.user_id),
		  clicked_at=COALESCE(reminder_attribution.clicked_at, EXCLUDED.clicked_at)`, reminderID, userID, sku, now)
	return translatePG(err)
}

// Attribute 幂等：同 reminder 只归因一次（同 order_no 重放成功，不同 order_no → ErrConflict）；order_id UNIQUE 冲突 → ErrConflict。
func (s *PGStore) Attribute(ctx context.Context, reminderID int64, orderNo, skuID string, now time.Time) error {
	var sku *string
	if skuID != "" {
		sku = &skuID
	}
	tag, err := s.DB.Exec(ctx, `INSERT INTO iot_shard.reminder_attribution(reminder_id, order_id, sku_id, paid_at, attributed_at) VALUES($1,$2,$3,$4,$4)
		ON CONFLICT (reminder_id) DO UPDATE SET order_id=EXCLUDED.order_id, sku_id=COALESCE(reminder_attribution.sku_id, EXCLUDED.sku_id),
		  paid_at=COALESCE(reminder_attribution.paid_at, EXCLUDED.paid_at), attributed_at=COALESCE(reminder_attribution.attributed_at, EXCLUDED.attributed_at)
		WHERE reminder_attribution.order_id IS NULL OR reminder_attribution.order_id = EXCLUDED.order_id`, reminderID, orderNo, sku, now)
	if err != nil {
		return translatePG(err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: reminder %d already attributed to another order", ErrConflict, reminderID)
	}
	return nil
}

func (s *PGStore) MaterialCode(ctx context.Context, codeID string) (*MaterialCode, error) {
	var mc MaterialCode
	err := s.DB.QueryRow(ctx, `SELECT code_id, material_id, batch, seq, status FROM iot_global.material_code WHERE code_id=$1`, codeID).
		Scan(&mc.CodeID, &mc.MaterialID, &mc.Batch, &mc.Seq, &mc.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("material code: %w", err)
	}
	mc.CodeID = strings.TrimSpace(mc.CodeID)
	return &mc, nil
}

func (s *PGStore) MaterialByIdx(ctx context.Context, idx uint32) (*MaterialInfo, error) {
	var mi MaterialInfo
	err := s.DB.QueryRow(ctx, `SELECT m.material_id, m.thickness_mm::float8 FROM iot_global.material_index i JOIN iot_global.material m USING (material_id) WHERE i.material_idx=$1`, int64(idx)).
		Scan(&mi.MaterialID, &mi.ThicknessMM)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("material idx: %w", err)
	}
	return &mi, nil
}

func (s *PGStore) InsertMaterialCode(ctx context.Context, mc MaterialCode) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO iot_global.material_code(code_id, material_id, batch, seq) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
		mc.CodeID, mc.MaterialID, mc.Batch, mc.Seq)
	return translatePG(err)
}

func (s *PGStore) ConsumeScan(ctx context.Context, codeID string, userID *int64, sn string, maxScans int, now time.Time) (bool, string, error) {
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	record := func(verified bool) error {
		_, err := tx.Exec(ctx, `INSERT INTO iot_shard.material_code_scan(code_id, user_id, sn, verified, scanned_at) VALUES($1,$2,$3,$4,$5)`,
			codeID, userID, nilIfEmpty(sn), verified, now)
		return err
	}
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM iot_global.material_code WHERE code_id=$1 FOR UPDATE`, codeID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := record(false); err != nil {
			return false, "", err
		}
		return false, "unknown", tx.Commit(ctx)
	}
	if err != nil {
		return false, "", fmt.Errorf("lock code: %w", err)
	}
	if status != "active" {
		if err := record(false); err != nil {
			return false, "", err
		}
		return false, status, tx.Commit(ctx)
	}
	var okScans, distinctUsers int
	if err := tx.QueryRow(ctx, `SELECT count(*), count(DISTINCT user_id) FROM iot_shard.material_code_scan
		WHERE code_id=$1 AND verified AND scanned_at > $2`, codeID, now.Add(-30*24*time.Hour)).Scan(&okScans, &distinctUsers); err != nil {
		return false, "", fmt.Errorf("count scans: %w", err)
	}
	if userID != nil {
		var seen bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM iot_shard.material_code_scan WHERE code_id=$1 AND verified AND user_id=$2)`, codeID, *userID).Scan(&seen); err != nil {
			return false, "", err
		}
		if !seen {
			distinctUsers++
		}
	}
	reason := ""
	switch {
	case maxScans > 0 && okScans >= maxScans:
		reason = "replay"
	case SuspiciousByScans(distinctUsers, SuspiciousThreshold):
		reason = "suspicious"
	}
	if reason != "" {
		if _, err := tx.Exec(ctx, `UPDATE iot_global.material_code SET status='suspicious' WHERE code_id=$1`, codeID); err != nil {
			return false, "", err
		}
		if err := record(false); err != nil {
			return false, "", err
		}
		return false, reason, tx.Commit(ctx)
	}
	if err := record(true); err != nil {
		return false, "", err
	}
	return true, "active", tx.Commit(ctx)
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ParseLevels 解析 levels_sent "80,50" → [80 50]（纯函数）。
func ParseLevels(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if n, err := strconv.Atoi(p); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// FormatLevels 是 ParseLevels 的逆（升序去重）。
func FormatLevels(ls []int) string {
	seen := map[int]bool{}
	var u []int
	for _, l := range ls {
		if !seen[l] {
			seen[l] = true
			u = append(u, l)
		}
	}
	sort.Ints(u)
	parts := make([]string, 0, len(u))
	for _, l := range u {
		parts = append(parts, strconv.Itoa(l))
	}
	return strings.Join(parts, ",")
}

// translatePG：FK 违规 → ErrNotFound（引用的 reminder / material 不存在）；唯一冲突 → ErrConflict；CHECK → ErrBadParam。
func translatePG(err error) error {
	if err == nil {
		return nil
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return err
	}
	switch pe.Code {
	case "23503":
		return fmt.Errorf("%w: %s", ErrNotFound, pe.ConstraintName)
	case "23505":
		return fmt.Errorf("%w: %s", ErrConflict, pe.ConstraintName)
	case "23514":
		return fmt.Errorf("%w: %s", ErrBadParam, pe.ConstraintName)
	}
	return err
}

// ===================== TDengine =====================

// TDSource 从 TDengine 读小时桶与 OVER_TEMP 计数。
//
// 说明：sql/tdengine.sql 的流目标表 iot.telemetry_1h 由 PARTITION BY tbname 自动建子表（t_<md5>，仅 group_id 标签），
// 无法从子表反查 SN；因此这里用同一口径（last(laser_hours)、avg(power_level)、分档占比）对 iot.telemetry 做
// PARTITION BY sn INTERVAL(1h) 聚合，结果与 telemetry_1h 逐桶一致。
type TDSource struct{ TD *tdengine.Client }

// BucketsSQL 纯函数：生成小时桶聚合 SQL（ts 用毫秒纪元避免时区歧义）。
func BucketsSQL(since time.Time) string {
	return fmt.Sprintf(`SELECT sn, product_key, _wstart, last(laser_hours), avg(power_level),
 sum(case when power_level <= 50 then 1 else 0 end) / count(*),
 sum(case when power_level > 50 and power_level <= 80 then 1 else 0 end) / count(*),
 sum(case when power_level > 80 then 1 else 0 end) / count(*)
 FROM iot.telemetry WHERE ts > %d PARTITION BY sn, product_key INTERVAL(1h)`, since.UnixMilli())
}

// OvertempSQL 纯函数：近窗口内每 SN 的 OVER_TEMP 次数。
func OvertempSQL(since time.Time) string {
	return fmt.Sprintf(`SELECT sn, count(*) FROM iot.events WHERE code='OVER_TEMP' AND ts > %d PARTITION BY sn`, since.UnixMilli())
}

func (t *TDSource) Buckets(ctx context.Context, since time.Time) ([]Bucket, error) {
	r, err := t.TD.Query(ctx, BucketsSQL(since))
	if err != nil {
		return nil, fmt.Errorf("td buckets: %w", err)
	}
	return ParseBuckets(r.Data)
}

// ParseBuckets 纯函数：把 REST 结果行转成 Bucket（列序与 BucketsSQL 一致）。
func ParseBuckets(rows [][]any) ([]Bucket, error) {
	out := make([]Bucket, 0, len(rows))
	for _, row := range rows {
		if len(row) < 8 {
			return nil, fmt.Errorf("td buckets: short row %d", len(row))
		}
		ts, ok := cellTime(row[2])
		if !ok {
			return nil, fmt.Errorf("td buckets: bad ts %v", row[2])
		}
		out = append(out, Bucket{SN: cellString(row[0]), ProductKey: cellString(row[1]), Ts: ts,
			LaserHours: cellFloat(row[3]), AvgPower: cellFloat(row[4]),
			ShareLow: cellFloat(row[5]), ShareMid: cellFloat(row[6]), ShareHigh: cellFloat(row[7])})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].SN != out[j].SN {
			return out[i].SN < out[j].SN
		}
		return out[i].Ts.Before(out[j].Ts)
	})
	return out, nil
}

func (t *TDSource) OvertempCounts(ctx context.Context, since time.Time) (map[string]int, error) {
	r, err := t.TD.Query(ctx, OvertempSQL(since))
	if err != nil {
		return nil, fmt.Errorf("td overtemp: %w", err)
	}
	out := map[string]int{}
	for _, row := range r.Data {
		if len(row) < 2 {
			continue
		}
		if f := cellFloat(row[1]); f != nil && *f > 0 {
			out[cellString(row[0])] = int(*f)
		}
	}
	return out, nil
}

func cellString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func cellFloat(v any) *float64 {
	switch x := v.(type) {
	case float64:
		return &x
	case int64:
		f := float64(x)
		return &f
	case int:
		f := float64(x)
		return &f
	case string:
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return &f
		}
	}
	return nil
}

func cellTime(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.000", "2006-01-02T15:04:05.000"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t, true
			}
		}
	case float64:
		return time.UnixMilli(int64(x)), true
	}
	return time.Time{}, false
}

// ===================== Redis 影子 =====================

// RedisShadow 读影子 reported：module_model / work_state / consumable_notify_optout。
type RedisShadow struct{ RDB *redis.Client }

func (r *RedisShadow) Reported(ctx context.Context, sn string) (Reported, error) {
	m, err := shadow.Read(ctx, r.RDB, sn)
	if err != nil {
		return Reported{}, err
	}
	return ParseReported(m), nil
}

// ParseReported 纯函数：影子 Hash → Reported。
func ParseReported(m map[string]string) Reported {
	if len(m) == 0 {
		return Reported{}
	}
	rep := Reported{Found: true, ModuleModel: strings.TrimSpace(m["module_model"])}
	if ws, err := strconv.Atoi(strings.TrimSpace(m["work_state"])); err == nil {
		rep.WorkState = ws
	}
	switch strings.ToLower(strings.TrimSpace(m["consumable_notify_optout"])) {
	case "true", "1":
		rep.Optout = true
	}
	return rep
}
