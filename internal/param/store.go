package param

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PublishPlan 是一次发布（或回滚）要写入的全部内容。Added 的 ID / VersionAdded 由 Store 填。
type PublishPlan struct {
	ProductKey     string
	Note           string
	CreatedBy      string
	ApprovedBy     string
	RolloutPct     int
	Added          []Profile
	RemoveIDs      []int64
	RolledBackFrom *int64
}

// SnapshotFn 在事务内、COMMIT 之前被调用：把新版本的有效集合写成快照，返回 snapshot_url。
// 返回 error 则整个发布回滚，保证「release 行只在快照写成功后才可见」。
type SnapshotFn func(version int64, releasedAt time.Time, profiles []Profile) (url string, err error)

// Recommendation 是 param_recommendation 中 status=published 的候选。
type Recommendation struct {
	ID          int64
	ProductKey  string
	ModuleModel string
	MaterialID  string
	ParamsHash  string
	Params      json.RawMessage
	SampleCount int
	GoodRatio   float64
}

// UserParam 是用户自定义参数。
type UserParam struct {
	ID          int64           `json:"id"`
	UserID      int64           `json:"user_id"`
	ProductKey  string          `json:"product_key"`
	ModuleModel string          `json:"module_model"`
	MaterialID  string          `json:"material_id"`
	Params      json.RawMessage `json:"params"`
	Version     int64           `json:"version"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// Store 抽象 PG，便于 handler 单测注入 fake。
type Store interface {
	Profiles(ctx context.Context, productKey string) ([]Profile, error)
	Releases(ctx context.Context, productKey string) ([]Release, error) // version 降序
	Publish(ctx context.Context, plan PublishPlan, snap SnapshotFn) (*Release, error)
	PublishedRecommendations(ctx context.Context, productKey string) ([]Recommendation, error)
	CorrectionCfg(ctx context.Context, productKey, moduleModel string) (Cfg, bool, error)
	UserParams(ctx context.Context, userID int64, productKey string) ([]UserParam, error)
	// PutUserParam：ifMatch==0 表示新建；返回写入后的 version；冲突时 conflict=true 且 current 为库内当前 version。
	PutUserParam(ctx context.Context, up UserParam, ifMatch int64) (version int64, current int64, conflict bool, err error)
}

// PGStore 是 Store 的 PostgreSQL 实现。
type PGStore struct{ DB *pgxpool.Pool }

const profileCols = `id, product_key, module_model, material_id, params, source, version_added, version_removed, sample_count, confidence, COALESCE(approved_by,'')`

func scanProfile(row pgx.Row) (Profile, error) {
	var p Profile
	err := row.Scan(&p.ID, &p.ProductKey, &p.ModuleModel, &p.MaterialID, &p.Params, &p.Source, &p.VersionAdded, &p.VersionRemoved, &p.SampleCount, &p.Confidence, &p.ApprovedBy)
	return p, err
}

func (s *PGStore) Profiles(ctx context.Context, productKey string) ([]Profile, error) {
	return queryProfiles(ctx, s.DB, productKey)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func queryProfiles(ctx context.Context, q querier, productKey string) ([]Profile, error) {
	rows, err := q.Query(ctx, `SELECT `+profileCols+` FROM iot_global.param_profile WHERE product_key=$1 ORDER BY id`, productKey)
	if err != nil {
		return nil, fmt.Errorf("profiles: %w", err)
	}
	defer rows.Close()
	var out []Profile
	for rows.Next() {
		p, err := scanProfile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *PGStore) Releases(ctx context.Context, productKey string) ([]Release, error) {
	rows, err := s.DB.Query(ctx, `SELECT product_key, version, COALESCE(note,''), COALESCE(snapshot_url,''), rollout_pct, created_by, approved_by, released_at, rolled_back_from
		FROM iot_global.param_release WHERE product_key=$1 ORDER BY version DESC`, productKey)
	if err != nil {
		return nil, fmt.Errorf("releases: %w", err)
	}
	defer rows.Close()
	var out []Release
	for rows.Next() {
		var r Release
		var at time.Time
		if err := rows.Scan(&r.ProductKey, &r.Version, &r.Note, &r.SnapshotURL, &r.RolloutPct, &r.CreatedBy, &r.ApprovedBy, &at, &r.RolledBackFrom); err != nil {
			return nil, err
		}
		r.ReleasedAt = at.UTC().Format(time.RFC3339)
		out = append(out, r)
	}
	return out, rows.Err()
}

// Publish 在单事务内：advisory lock 串行化同 product_key → 新 version = max+1 → INSERT release（审批约束由 PG 守）
// → INSERT added / UPDATE removed（参数范围约束由 PG 守）→ 重载有效集合 → 写快照 → UPDATE snapshot_url → COMMIT。
func (s *PGStore) Publish(ctx context.Context, plan PublishPlan, snap SnapshotFn) (*Release, error) {
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('param_release:' || $1))`, plan.ProductKey); err != nil {
		return nil, fmt.Errorf("lock: %w", err)
	}
	var maxVer int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(version),0) FROM iot_global.param_release WHERE product_key=$1`, plan.ProductKey).Scan(&maxVer); err != nil {
		return nil, fmt.Errorf("max version: %w", err)
	}
	newVer := maxVer + 1
	if plan.RolloutPct <= 0 {
		plan.RolloutPct = 100
	}
	var approved *string
	if plan.ApprovedBy != "" {
		approved = &plan.ApprovedBy
	}
	var releasedAt time.Time
	err = tx.QueryRow(ctx, `INSERT INTO iot_global.param_release(product_key, version, note, rollout_pct, created_by, approved_by, rolled_back_from)
		VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING released_at`,
		plan.ProductKey, newVer, plan.Note, plan.RolloutPct, plan.CreatedBy, approved, plan.RolledBackFrom).Scan(&releasedAt)
	if err != nil {
		return nil, translatePG(err)
	}

	for _, id := range plan.RemoveIDs {
		tag, err := tx.Exec(ctx, `UPDATE iot_global.param_profile SET version_removed=$3 WHERE id=$1 AND product_key=$2 AND version_removed IS NULL`, id, plan.ProductKey, newVer)
		if err != nil {
			return nil, translatePG(err)
		}
		if tag.RowsAffected() == 0 {
			return nil, fmt.Errorf("%w: profile %d not effective for %s", ErrBadParam, id, plan.ProductKey)
		}
	}
	for _, p := range plan.Added {
		src := p.Source
		if src == "" {
			src = SourceOfficial
		}
		var approvedBy *string
		if p.ApprovedBy != "" {
			approvedBy = &p.ApprovedBy
		} else if approved != nil {
			approvedBy = approved
		}
		if _, err := tx.Exec(ctx, `INSERT INTO iot_global.param_profile(product_key, module_model, material_id, params, source, version_added, sample_count, confidence, approved_by)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			plan.ProductKey, p.ModuleModel, p.MaterialID, p.Params, src, newVer, p.SampleCount, p.Confidence, approvedBy); err != nil {
			return nil, translatePG(err)
		}
	}

	all, err := queryProfiles(ctx, tx, plan.ProductKey)
	if err != nil {
		return nil, err
	}
	url, err := snap(newVer, releasedAt, EffectiveAt(all, newVer))
	if err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE iot_global.param_release SET snapshot_url=$3 WHERE product_key=$1 AND version=$2`, plan.ProductKey, newVer, url); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &Release{ProductKey: plan.ProductKey, Version: newVer, Note: plan.Note, SnapshotURL: url, RolloutPct: plan.RolloutPct,
		CreatedBy: plan.CreatedBy, ApprovedBy: plan.ApprovedBy, ReleasedAt: releasedAt.UTC().Format(time.RFC3339), RolledBackFrom: plan.RolledBackFrom}, nil
}

func (s *PGStore) PublishedRecommendations(ctx context.Context, productKey string) ([]Recommendation, error) {
	rows, err := s.DB.Query(ctx, `SELECT id, product_key, module_model, material_id, params_hash, params, sample_count, good_ratio::float8
		FROM iot_global.param_recommendation WHERE product_key=$1 AND status='published' AND params IS NOT NULL ORDER BY id`, productKey)
	if err != nil {
		return nil, fmt.Errorf("recommendations: %w", err)
	}
	defer rows.Close()
	var out []Recommendation
	for rows.Next() {
		var r Recommendation
		if err := rows.Scan(&r.ID, &r.ProductKey, &r.ModuleModel, &r.MaterialID, &r.ParamsHash, &r.Params, &r.SampleCount, &r.GoodRatio); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PGStore) CorrectionCfg(ctx context.Context, productKey, moduleModel string) (Cfg, bool, error) {
	var c Cfg
	err := s.DB.QueryRow(ctx, `SELECT a::float8, b::float8, c::float8, rated_hours::float8 FROM iot_global.param_correction_cfg WHERE product_key=$1 AND module_model=$2`,
		productKey, moduleModel).Scan(&c.A, &c.B, &c.C, &c.RatedHours)
	if errors.Is(err, pgx.ErrNoRows) {
		return Cfg{}, false, nil
	}
	if err != nil {
		return Cfg{}, false, fmt.Errorf("correction cfg: %w", err)
	}
	return c, true, nil
}

func (s *PGStore) UserParams(ctx context.Context, userID int64, productKey string) ([]UserParam, error) {
	rows, err := s.DB.Query(ctx, `SELECT id, user_id, product_key, module_model, material_id, params, version, updated_at
		FROM iot_shard.user_param WHERE user_id=$1 AND product_key=$2 ORDER BY id`, userID, productKey)
	if err != nil {
		return nil, fmt.Errorf("user params: %w", err)
	}
	defer rows.Close()
	var out []UserParam
	for rows.Next() {
		var u UserParam
		if err := rows.Scan(&u.ID, &u.UserID, &u.ProductKey, &u.ModuleModel, &u.MaterialID, &u.Params, &u.Version, &u.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// PutUserParam：乐观锁。ifMatch==0 → INSERT ... ON CONFLICT DO NOTHING，未插入即冲突；
// 否则 UPDATE ... WHERE version=ifMatch，affected=0 即冲突。冲突时返回库内当前 version（不存在返回 0）。
func (s *PGStore) PutUserParam(ctx context.Context, up UserParam, ifMatch int64) (int64, int64, bool, error) {
	if ifMatch == 0 {
		var ver int64
		err := s.DB.QueryRow(ctx, `INSERT INTO iot_shard.user_param(user_id, product_key, module_model, material_id, params)
			VALUES($1,$2,$3,$4,$5) ON CONFLICT (user_id, product_key, module_model, material_id) DO NOTHING RETURNING version`,
			up.UserID, up.ProductKey, up.ModuleModel, up.MaterialID, up.Params).Scan(&ver)
		if errors.Is(err, pgx.ErrNoRows) {
			cur, cerr := s.currentUserParamVersion(ctx, up)
			return 0, cur, true, cerr
		}
		if err != nil {
			return 0, 0, false, translatePG(err)
		}
		return ver, ver, false, nil
	}
	var ver int64
	err := s.DB.QueryRow(ctx, `UPDATE iot_shard.user_param SET params=$5, version=version+1, updated_at=now()
		WHERE user_id=$1 AND product_key=$2 AND module_model=$3 AND material_id=$4 AND version=$6 RETURNING version`,
		up.UserID, up.ProductKey, up.ModuleModel, up.MaterialID, up.Params, ifMatch).Scan(&ver)
	if errors.Is(err, pgx.ErrNoRows) {
		cur, cerr := s.currentUserParamVersion(ctx, up)
		return 0, cur, true, cerr
	}
	if err != nil {
		return 0, 0, false, translatePG(err)
	}
	return ver, ver, false, nil
}

func (s *PGStore) currentUserParamVersion(ctx context.Context, up UserParam) (int64, error) {
	var cur int64
	err := s.DB.QueryRow(ctx, `SELECT version FROM iot_shard.user_param WHERE user_id=$1 AND product_key=$2 AND module_model=$3 AND material_id=$4`,
		up.UserID, up.ProductKey, up.ModuleModel, up.MaterialID).Scan(&cur)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return cur, err
}

// translatePG 把约束违规翻译成业务错误：ck_release_approved → ErrDenied（403）；ck_profile_* / FK → ErrBadParam（400）；唯一冲突 → ErrConflict。
func translatePG(err error) error {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return err
	}
	switch pe.Code {
	case "23514": // check_violation
		if pe.ConstraintName == "ck_release_approved" {
			return fmt.Errorf("%w: release requires approved_by different from created_by", ErrDenied)
		}
		return fmt.Errorf("%w: %s", ErrBadParam, pe.ConstraintName)
	case "23503": // foreign_key_violation
		return fmt.Errorf("%w: %s", ErrBadParam, pe.ConstraintName)
	case "23505":
		return fmt.Errorf("%w: %s", ErrConflict, pe.ConstraintName)
	}
	return err
}
