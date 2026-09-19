package accessory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// 领域错误 → handler 翻译 HTTP 状态。
var (
	ErrNotFound      = errors.New("not found")
	ErrBadParam      = errors.New("bad param")
	ErrOwnerMismatch = errors.New("host and accessory belong to different owners")
	ErrConflict      = errors.New("conflict")
)

var snRe = regexp.MustCompile(`^[A-Z0-9_-]{4,32}$`)

// ValidSN 与 auth-svc / bootstrap 同一 SN 规则。
func ValidSN(sn string) bool { return snRe.MatchString(sn) }

// LinkageAudit 是 linkage_audit 的一行。
type LinkageAudit struct {
	ID        int64           `json:"id"`
	HostSN    string          `json:"host_sn"`
	AccSN     string          `json:"acc_sn"`
	Trigger   string          `json:"trigger"`
	Action    json.RawMessage `json:"action"`
	Result    string          `json:"result"`
	LatencyMs *int64          `json:"latency_ms,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// AlarmContext 是配件安全事件对主机的关联。
type AlarmContext struct {
	ID            int64     `json:"id"`
	AccSN         string    `json:"acc_sn"`
	Code          string    `json:"code"`
	EventTs       time.Time `json:"event_ts"`
	HostSN        string    `json:"host_sn,omitempty"`
	HostWorkState *int      `json:"host_work_state,omitempty"`
	JobID         string    `json:"job_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// FilterLife 是 filter_life 的一行。
type FilterLife struct {
	AccSN          string     `json:"acc_sn"`
	FilterModel    string     `json:"filter_model"`
	InstalledAt    time.Time  `json:"installed_at"`
	EqAirVolume    float64    `json:"eq_air_volume"`
	RunSeconds     float64    `json:"run_seconds"`
	Health         float64    `json:"health"`
	PredictedEOLAt *time.Time `json:"predicted_eol_at,omitempty"`
	LastHour       *time.Time `json:"last_hour,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// Store 抽象 PG，便于 handler / 引擎单测注入 fake。
type Store interface {
	Pair(ctx context.Context, hostSN, accSN, accType string, enabled bool, offDelayS int, pairKey string) (p *Pairing, created bool, err error)
	Unpair(ctx context.Context, id int64) (*Pairing, error)
	UpdatePairing(ctx context.Context, id int64, enabled *bool, offDelayS *int) (*Pairing, error)
	PairingByID(ctx context.Context, id int64) (*Pairing, error)
	PairingsByHost(ctx context.Context, hostSN string) ([]Pairing, error)
	PairingByAcc(ctx context.Context, accSN string) (*Pairing, error)
	AllActivePairings(ctx context.Context) ([]Pairing, error)

	DeviceProduct(ctx context.Context, sn string) (productKey string, found bool, err error)
	Owners(ctx context.Context, sn string) ([]int64, error)
	Rules(ctx context.Context, productKey string) (map[string]int, error)

	InsertAudit(ctx context.Context, a LinkageAudit) error
	ListAudit(ctx context.Context, hostSN string, limit int) ([]LinkageAudit, error)
	InsertAlarmContext(ctx context.Context, c AlarmContext) error
	GetAlarmContext(ctx context.Context, accSN, code string, eventTs time.Time) (*AlarmContext, error)

	FilterModel(ctx context.Context, model string) (*FilterModelCfg, error)
	FilterLifeGet(ctx context.Context, accSN string) (*FilterLife, error)
	FilterLifeUpsert(ctx context.Context, f FilterLife) error
	AccessorySNs(ctx context.Context, productPrefix string) ([]string, error)
	EnsureAuditPartitions(ctx context.Context, monthsAhead int) (int, error)
}

// PGStore 是 Store 的 PostgreSQL 实现。
type PGStore struct{ Pool *pgxpool.Pool }

const pairingCols = `id, host_sn, acc_sn, acc_type, linkage_enabled, off_delay_s, paired_at, unpaired_at`

func scanPairing(row pgx.Row) (*Pairing, error) {
	var p Pairing
	if err := row.Scan(&p.ID, &p.HostSN, &p.AccSN, &p.AccType, &p.LinkageEnabled, &p.OffDelayS, &p.PairedAt, &p.UnpairedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &p, nil
}

// NewPairKey 生成配对密钥（本地兜底广播的 HMAC key；云端只存不再下发原文之外的地方）。
func NewPairKey() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// Pair：同主机已配 → 幂等返回；配了别的主机 → 先解除再新建；部分唯一索引兜底并发。
func (s *PGStore) Pair(ctx context.Context, hostSN, accSN, accType string, enabled bool, offDelayS int, pairKey string) (*Pairing, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	cur, err := scanPairing(tx.QueryRow(ctx, `SELECT `+pairingCols+` FROM iot_shard.accessory_pairing WHERE acc_sn=$1 AND unpaired_at IS NULL FOR UPDATE`, accSN))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, false, fmt.Errorf("lookup pairing: %w", err)
	}
	if cur != nil {
		if cur.HostSN == hostSN {
			return cur, false, tx.Commit(ctx)
		}
		if _, err := tx.Exec(ctx, `UPDATE iot_shard.accessory_pairing SET unpaired_at=now() WHERE id=$1`, cur.ID); err != nil {
			return nil, false, fmt.Errorf("unpair previous: %w", err)
		}
	}
	p, err := scanPairing(tx.QueryRow(ctx,
		`INSERT INTO iot_shard.accessory_pairing(host_sn, acc_sn, acc_type, linkage_enabled, off_delay_s, pair_key)
		 VALUES($1,$2,$3,$4,$5,$6) RETURNING `+pairingCols, hostSN, accSN, accType, enabled, offDelayS, pairKey))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "23505":
				return nil, false, fmt.Errorf("%w: accessory already paired", ErrConflict)
			case "23514":
				return nil, false, fmt.Errorf("%w: %s", ErrBadParam, pgErr.ConstraintName)
			}
		}
		return nil, false, fmt.Errorf("insert pairing: %w", err)
	}
	return p, true, tx.Commit(ctx)
}

func (s *PGStore) Unpair(ctx context.Context, id int64) (*Pairing, error) {
	p, err := scanPairing(s.Pool.QueryRow(ctx, `UPDATE iot_shard.accessory_pairing SET unpaired_at=now() WHERE id=$1 AND unpaired_at IS NULL RETURNING `+pairingCols, id))
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (s *PGStore) UpdatePairing(ctx context.Context, id int64, enabled *bool, offDelayS *int) (*Pairing, error) {
	p, err := scanPairing(s.Pool.QueryRow(ctx,
		`UPDATE iot_shard.accessory_pairing SET linkage_enabled=COALESCE($2, linkage_enabled), off_delay_s=COALESCE($3, off_delay_s)
		 WHERE id=$1 AND unpaired_at IS NULL RETURNING `+pairingCols, id, enabled, offDelayS))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23514" {
			return nil, fmt.Errorf("%w: off_delay_s must be 60..600", ErrBadParam)
		}
		return nil, err
	}
	return p, nil
}

func (s *PGStore) PairingByID(ctx context.Context, id int64) (*Pairing, error) {
	return scanPairing(s.Pool.QueryRow(ctx, `SELECT `+pairingCols+` FROM iot_shard.accessory_pairing WHERE id=$1`, id))
}

func (s *PGStore) PairingsByHost(ctx context.Context, hostSN string) ([]Pairing, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+pairingCols+` FROM iot_shard.accessory_pairing WHERE host_sn=$1 AND unpaired_at IS NULL ORDER BY id`, hostSN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectPairings(rows)
}

func (s *PGStore) PairingByAcc(ctx context.Context, accSN string) (*Pairing, error) {
	return scanPairing(s.Pool.QueryRow(ctx, `SELECT `+pairingCols+` FROM iot_shard.accessory_pairing WHERE acc_sn=$1 AND unpaired_at IS NULL`, accSN))
}

func (s *PGStore) AllActivePairings(ctx context.Context) ([]Pairing, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+pairingCols+` FROM iot_shard.accessory_pairing WHERE unpaired_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectPairings(rows)
}

func collectPairings(rows pgx.Rows) ([]Pairing, error) {
	var out []Pairing
	for rows.Next() {
		var p Pairing
		if err := rows.Scan(&p.ID, &p.HostSN, &p.AccSN, &p.AccType, &p.LinkageEnabled, &p.OffDelayS, &p.PairedAt, &p.UnpairedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *PGStore) DeviceProduct(ctx context.Context, sn string) (string, bool, error) {
	var pk string
	err := s.Pool.QueryRow(ctx, `SELECT product_key FROM iot_shard.device WHERE sn=$1`, sn).Scan(&pk)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return pk, true, nil
}

func (s *PGStore) Owners(ctx context.Context, sn string) ([]int64, error) {
	rows, err := s.Pool.Query(ctx, `SELECT user_id FROM iot_shard.device_binding WHERE sn=$1 AND unbound_at IS NULL`, sn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var u int64
		if err := rows.Scan(&u); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// Rules 取该 product_key 每种材料的最新 version 档位。
func (s *PGStore) Rules(ctx context.Context, productKey string) (map[string]int, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT DISTINCT ON (material_id) material_id, fan_level FROM iot_global.linkage_rule WHERE product_key=$1 ORDER BY material_id, version DESC`, productKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var m string
		var lv int
		if err := rows.Scan(&m, &lv); err != nil {
			return nil, err
		}
		out[m] = lv
	}
	return out, rows.Err()
}

func (s *PGStore) InsertAudit(ctx context.Context, a LinkageAudit) error {
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now()
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO iot_shard.linkage_audit(host_sn, acc_sn, trigger, action, result, latency_ms, created_at) VALUES($1,$2,$3,$4,$5,$6,$7)`,
		a.HostSN, a.AccSN, a.Trigger, string(a.Action), a.Result, a.LatencyMs, a.CreatedAt)
	return err
}

func (s *PGStore) ListAudit(ctx context.Context, hostSN string, limit int) ([]LinkageAudit, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, host_sn, acc_sn, trigger, action, result, latency_ms, created_at FROM iot_shard.linkage_audit WHERE host_sn=$1 ORDER BY created_at DESC LIMIT $2`, hostSN, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LinkageAudit
	for rows.Next() {
		var a LinkageAudit
		var action []byte
		if err := rows.Scan(&a.ID, &a.HostSN, &a.AccSN, &a.Trigger, &action, &a.Result, &a.LatencyMs, &a.CreatedAt); err != nil {
			return nil, err
		}
		a.Action = json.RawMessage(action)
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *PGStore) InsertAlarmContext(ctx context.Context, c AlarmContext) error {
	_, err := s.Pool.Exec(ctx,
		`INSERT INTO iot_shard.alarm_context(acc_sn, code, event_ts, host_sn, host_work_state, job_id) VALUES($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,''))
		 ON CONFLICT (acc_sn, code, event_ts) DO NOTHING`, c.AccSN, c.Code, c.EventTs, c.HostSN, c.HostWorkState, c.JobID)
	return err
}

func (s *PGStore) GetAlarmContext(ctx context.Context, accSN, code string, eventTs time.Time) (*AlarmContext, error) {
	var c AlarmContext
	var host, job *string
	err := s.Pool.QueryRow(ctx,
		`SELECT id, acc_sn, code, event_ts, host_sn, host_work_state, job_id, created_at FROM iot_shard.alarm_context
		 WHERE acc_sn=$1 AND code=$2 AND event_ts BETWEEN $3::timestamptz - interval '2 seconds' AND $3::timestamptz + interval '2 seconds' ORDER BY id DESC LIMIT 1`, accSN, code, eventTs).
		Scan(&c.ID, &c.AccSN, &c.Code, &c.EventTs, &host, &c.HostWorkState, &job, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if host != nil {
		c.HostSN = *host
	}
	if job != nil {
		c.JobID = *job
	}
	return &c, nil
}

func (s *PGStore) FilterModel(ctx context.Context, model string) (*FilterModelCfg, error) {
	var cfg FilterModelCfg
	var coeff, levels []byte
	err := s.Pool.QueryRow(ctx, `SELECT model, rated_volume::float8, flow_coeff, p0::float8, p_span::float8, k_p::float8, rpm_levels FROM iot_global.filter_model WHERE model=$1`, model).
		Scan(&cfg.Model, &cfg.RatedVolume, &coeff, &cfg.P0, &cfg.PSpan, &cfg.KP, &levels)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := cfg.parse(coeff, levels); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (s *PGStore) FilterLifeGet(ctx context.Context, accSN string) (*FilterLife, error) {
	var f FilterLife
	err := s.Pool.QueryRow(ctx, `SELECT acc_sn, filter_model, installed_at, eq_air_volume::float8, run_seconds::float8, health::float8, predicted_eol_at, last_hour, updated_at FROM iot_shard.filter_life WHERE acc_sn=$1`, accSN).
		Scan(&f.AccSN, &f.FilterModel, &f.InstalledAt, &f.EqAirVolume, &f.RunSeconds, &f.Health, &f.PredictedEOLAt, &f.LastHour, &f.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// FilterLifeUpsert 同事务写 filter_life 与 consumable_health(part='filter')。
func (s *PGStore) FilterLifeUpsert(ctx context.Context, f FilterLife) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx,
		`INSERT INTO iot_shard.filter_life(acc_sn, filter_model, installed_at, eq_air_volume, run_seconds, health, predicted_eol_at, last_hour, updated_at)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8,now())
		 ON CONFLICT (acc_sn) DO UPDATE SET filter_model=EXCLUDED.filter_model, installed_at=EXCLUDED.installed_at, eq_air_volume=EXCLUDED.eq_air_volume,
		   run_seconds=EXCLUDED.run_seconds, health=EXCLUDED.health, predicted_eol_at=EXCLUDED.predicted_eol_at, last_hour=EXCLUDED.last_hour, updated_at=now()`,
		f.AccSN, f.FilterModel, f.InstalledAt, f.EqAirVolume, f.RunSeconds, f.Health, f.PredictedEOLAt, f.LastHour); err != nil {
		return fmt.Errorf("upsert filter_life: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO iot_shard.consumable_health(sn, part, health, used_hours, predicted_eol_at, updated_at) VALUES($1,'filter',$2,$3,$4,now())
		 ON CONFLICT (sn, part) DO UPDATE SET health=EXCLUDED.health, used_hours=EXCLUDED.used_hours, predicted_eol_at=EXCLUDED.predicted_eol_at, updated_at=now()`,
		f.AccSN, f.Health*100, f.RunSeconds/3600, f.PredictedEOLAt); err != nil {
		return fmt.Errorf("upsert consumable_health: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *PGStore) AccessorySNs(ctx context.Context, productPrefix string) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT sn FROM iot_shard.device WHERE product_key LIKE $1 || '%' ORDER BY sn`, productPrefix)
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

func (s *PGStore) EnsureAuditPartitions(ctx context.Context, monthsAhead int) (int, error) {
	var n int
	if err := s.Pool.QueryRow(ctx, `SELECT iot_shard.ensure_linkage_audit_partitions($1)`, monthsAhead).Scan(&n); err != nil {
		return 0, fmt.Errorf("ensure linkage_audit partitions: %w", err)
	}
	return n, nil
}

// ---- 配对缓存：pairing:host:{sn}，TTL 60 s，变更主动 DEL（技术方案 §4.3）----

const PairingCacheTTL = 60 * time.Second

func pairingCacheKey(host string) string { return "pairing:host:" + host }

// CachedPairings 是热路径读：Redis 命中直接返回；miss 回源 PG 并回填。rdb 为 nil 时直连 PG。
func CachedPairings(ctx context.Context, rdb *redis.Client, st Store, host string) ([]Pairing, error) {
	if rdb != nil {
		if b, err := rdb.Get(ctx, pairingCacheKey(host)).Bytes(); err == nil {
			var ps []Pairing
			if json.Unmarshal(b, &ps) == nil {
				return ps, nil
			}
		}
	}
	ps, err := st.PairingsByHost(ctx, host)
	if err != nil {
		return nil, err
	}
	if rdb != nil {
		if b, err := json.Marshal(ps); err == nil {
			_ = rdb.Set(ctx, pairingCacheKey(host), b, PairingCacheTTL).Err()
		}
	}
	return ps, nil
}

// InvalidatePairings 在配对变更后调用。
func InvalidatePairings(ctx context.Context, rdb *redis.Client, host string) {
	if rdb != nil {
		_ = rdb.Del(ctx, pairingCacheKey(host)).Err()
	}
}
