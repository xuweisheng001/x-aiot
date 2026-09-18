package deviceapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AuditRecord 是 iot.cmd.audit 上的审计消息，也是 cmd_audit 表的一行。
type AuditRecord struct {
	CmdID     string          `json:"cmd_id"`
	SN        string          `json:"sn"`
	Action    string          `json:"action"`
	Params    json.RawMessage `json:"params,omitempty"`
	Operator  string          `json:"operator"`
	Source    string          `json:"source"`
	Result    string          `json:"result"`
	CreatedAt time.Time       `json:"created_at"`
}

// Store 是 deviceapi 依赖的 PG 操作集合（便于测试替换）。
type Store interface {
	GetDesired(ctx context.Context, sn string) (desired json.RawMessage, version int64, err error)
	MergeDesired(ctx context.Context, sn string, patch json.RawMessage) (version int64, desired json.RawMessage, err error)
	InsertAudit(ctx context.Context, rec AuditRecord) error
	MarkAcked(ctx context.Context, cmdID, result string) error
}

// PGStore 是 Store 的 pgx 实现（schema iot_shard）。
type PGStore struct{ Pool *pgxpool.Pool }

func NewPGStore(pool *pgxpool.Pool) *PGStore { return &PGStore{Pool: pool} }

func (s *PGStore) GetDesired(ctx context.Context, sn string) (json.RawMessage, int64, error) {
	var desired []byte
	var version int64
	err := s.Pool.QueryRow(ctx, `SELECT desired, version FROM iot_shard.shadow_desired WHERE sn = $1`, sn).Scan(&desired, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return json.RawMessage(`{}`), 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("get desired: %w", err)
	}
	return json.RawMessage(desired), version, nil
}

// MergeDesired 单事务：不存在 INSERT(version=1)，存在则 JSONB 合并 + version++，RETURNING。
func (s *PGStore) MergeDesired(ctx context.Context, sn string, patch json.RawMessage) (int64, json.RawMessage, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var version int64
	var desired []byte
	err = tx.QueryRow(ctx, `
INSERT INTO iot_shard.shadow_desired (sn, desired, version, updated_at) VALUES ($1, $2::jsonb, 1, now())
ON CONFLICT (sn) DO UPDATE
   SET desired = shadow_desired.desired || EXCLUDED.desired,
       version = shadow_desired.version + 1,
       updated_at = now()
RETURNING version, desired`, sn, string(patch)).Scan(&version, &desired)
	if err != nil {
		return 0, nil, fmt.Errorf("merge desired: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, fmt.Errorf("commit: %w", err)
	}
	return version, json.RawMessage(desired), nil
}

func (s *PGStore) InsertAudit(ctx context.Context, r AuditRecord) error {
	params := "null"
	if len(r.Params) > 0 {
		params = string(r.Params)
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	_, err := s.Pool.Exec(ctx, `
INSERT INTO iot_shard.cmd_audit (cmd_id, sn, action, params, operator, source, result, created_at)
VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)
ON CONFLICT DO NOTHING`, r.CmdID, r.SN, r.Action, params, r.Operator, r.Source, r.Result, r.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert audit: %w", err)
	}
	return nil
}

func (s *PGStore) MarkAcked(ctx context.Context, cmdID, result string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE iot_shard.cmd_audit SET result = $2, acked_at = now() WHERE cmd_id = $1 AND acked_at IS NULL`, cmdID, result)
	if err != nil {
		return fmt.Errorf("mark acked: %w", err)
	}
	return nil
}
