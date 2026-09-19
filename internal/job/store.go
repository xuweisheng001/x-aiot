package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store 抽象 PG 访问，便于 handler / service 单测。
type Store interface {
	// RecordStart 写 JOB_START；已存在则忽略（幂等）。返回是否新插入。
	RecordStart(ctx context.Context, f JobFields, sn string, startedAt time.Time) (bool, error)
	// RecordFinish 写终态；job_record 不存在时以 finishedAt 作为 started_at 补插一条（late=true）。
	RecordFinish(ctx context.Context, f JobFields, sn string, finishedAt time.Time, outcome string) (late bool, err error)
	// MergePending 把 job_feedback_pending 中该 job 的标记搬到 job_feedback 并删除暂存。返回是否搬运。
	MergePending(ctx context.Context, jobID string) (bool, error)
	// RecordSN 返回 job_record 中该 job 的 SN；不存在 found=false。
	RecordSN(ctx context.Context, jobID string) (sn string, found bool, err error)
	// UpsertFeedback 写或覆盖标记。
	UpsertFeedback(ctx context.Context, jobID, sn, rating string) error
	// InsertPending 写暂存标记（job_record 未到达）。
	InsertPending(ctx context.Context, jobID, sn, rating string) error
	// OptedOutSNs 返回 desired.job_feedback_optin = false 的 SN 列表（撤回删除的触发源）。
	OptedOutSNs(ctx context.Context) ([]string, error)
	// PurgeSN 删除该 SN 的 job_record（级联 job_feedback）与 job_feedback_pending，返回删除行数。
	PurgeSN(ctx context.Context, sn string) (int64, error)
}

// PGStore 是 Store 的 PostgreSQL 实现。
type PGStore struct{ Pool *pgxpool.Pool }

func (s *PGStore) RecordStart(ctx context.Context, f JobFields, sn string, startedAt time.Time) (bool, error) {
	tag, err := s.Pool.Exec(ctx,
		`INSERT INTO iot_shard.job_record(job_id, sn, material_id, param_profile_id, params_hash, started_at)
		 VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),$6)
		 ON CONFLICT (job_id) DO NOTHING`,
		f.JobID, sn, f.MaterialID, f.ParamProfileID, f.ParamsHash, startedAt)
	if err != nil {
		return false, fmt.Errorf("record start: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) RecordFinish(ctx context.Context, f JobFields, sn string, finishedAt time.Time, outcome string) (bool, error) {
	tag, err := s.Pool.Exec(ctx,
		`UPDATE iot_shard.job_record
		    SET finished_at=$2, outcome=$3, duration_s=GREATEST(0, EXTRACT(EPOCH FROM ($2 - started_at)))::int
		  WHERE job_id=$1`, f.JobID, finishedAt, outcome)
	if err != nil {
		return false, fmt.Errorf("record finish: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return false, nil
	}
	// START 丢了：用终态时间作为 started_at 补一条，duration 未知记 0。
	_, err = s.Pool.Exec(ctx,
		`INSERT INTO iot_shard.job_record(job_id, sn, material_id, param_profile_id, params_hash, started_at, finished_at, outcome, duration_s)
		 VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),$6,$6,$7,0)
		 ON CONFLICT (job_id) DO UPDATE SET finished_at=EXCLUDED.finished_at, outcome=EXCLUDED.outcome`,
		f.JobID, sn, f.MaterialID, f.ParamProfileID, f.ParamsHash, finishedAt, outcome)
	if err != nil {
		return false, fmt.Errorf("record late finish: %w", err)
	}
	return true, nil
}

func (s *PGStore) MergePending(ctx context.Context, jobID string) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("merge pending begin: %w", err)
	}
	defer tx.Rollback(ctx)
	var sn, rating string
	err = tx.QueryRow(ctx, `DELETE FROM iot_shard.job_feedback_pending WHERE job_id=$1 RETURNING sn, rating`, jobID).Scan(&sn, &rating)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("merge pending delete: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO iot_shard.job_feedback(job_id, sn, rating) VALUES ($1,$2,$3)
		 ON CONFLICT (job_id) DO UPDATE SET rating=EXCLUDED.rating, created_at=now()`, jobID, sn, rating); err != nil {
		return false, fmt.Errorf("merge pending insert: %w", err)
	}
	return true, tx.Commit(ctx)
}

func (s *PGStore) RecordSN(ctx context.Context, jobID string) (string, bool, error) {
	var sn string
	err := s.Pool.QueryRow(ctx, `SELECT sn FROM iot_shard.job_record WHERE job_id=$1`, jobID).Scan(&sn)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("record sn: %w", err)
	}
	return sn, true, nil
}

func (s *PGStore) UpsertFeedback(ctx context.Context, jobID, sn, rating string) error {
	_, err := s.Pool.Exec(ctx,
		`INSERT INTO iot_shard.job_feedback(job_id, sn, rating) VALUES ($1,$2,$3)
		 ON CONFLICT (job_id) DO UPDATE SET rating=EXCLUDED.rating, created_at=now()`, jobID, sn, rating)
	if err != nil {
		return fmt.Errorf("upsert feedback: %w", err)
	}
	return nil
}

func (s *PGStore) InsertPending(ctx context.Context, jobID, sn, rating string) error {
	_, err := s.Pool.Exec(ctx,
		`INSERT INTO iot_shard.job_feedback_pending(job_id, sn, rating) VALUES ($1,$2,$3)
		 ON CONFLICT (job_id) DO UPDATE SET rating=EXCLUDED.rating, sn=EXCLUDED.sn, created_at=now()`, jobID, sn, rating)
	if err != nil {
		return fmt.Errorf("insert pending: %w", err)
	}
	return nil
}

func (s *PGStore) OptedOutSNs(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT sn FROM iot_shard.shadow_desired WHERE desired->>'job_feedback_optin' = 'false'`)
	if err != nil {
		return nil, fmt.Errorf("opted out sns: %w", err)
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

func (s *PGStore) PurgeSN(ctx context.Context, sn string) (int64, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("purge begin: %w", err)
	}
	defer tx.Rollback(ctx)
	t1, err := tx.Exec(ctx, `DELETE FROM iot_shard.job_record WHERE sn=$1`, sn) // 级联 job_feedback
	if err != nil {
		return 0, fmt.Errorf("purge records: %w", err)
	}
	t2, err := tx.Exec(ctx, `DELETE FROM iot_shard.job_feedback_pending WHERE sn=$1`, sn)
	if err != nil {
		return 0, fmt.Errorf("purge pending: %w", err)
	}
	return t1.RowsAffected() + t2.RowsAffected(), tx.Commit(ctx)
}
