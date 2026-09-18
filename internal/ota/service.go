package ota

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

var (
	// ErrNeedsApproval：PG CHECK ck_full_stage_approved 拒绝（SQLSTATE 23514）→ 403 10003。
	ErrNeedsApproval = errors.New("full-stage requires dual approval")
	ErrNotFound      = errors.New("not found")
	ErrConflict      = errors.New("state conflict")
	ErrBadParam      = errors.New("bad param")
)

const (
	DefaultDispatchRPS = 200
	DefaultFuseRatio   = 0.02
)

type Service struct {
	DB      *pgxpool.Pool
	Pub     Publisher
	M       *Metrics
	limiter *rate.Limiter
	// MinSamples 熔断最小样本数（默认 50）。
	MinSamples int
}

func NewService(db *pgxpool.Pool, pub Publisher, m *Metrics, dispatchRPS int) *Service {
	if dispatchRPS <= 0 {
		dispatchRPS = DefaultDispatchRPS
	}
	return &Service{DB: db, Pub: pub, M: m, MinSamples: MinFuseSamples,
		limiter: rate.NewLimiter(rate.Limit(dispatchRPS), dispatchRPS)}
}

// ---------- firmware ----------

type FirmwareReq struct {
	ProductKey  string `json:"product_key"`
	Version     string `json:"version"`
	FullURL     string `json:"full_url"`
	FullSize    int64  `json:"full_size"`
	SHA256      string `json:"sha256"`
	Signature   string `json:"signature"`
	ReleaseNote string `json:"release_note"`
}

func (r FirmwareReq) validate() error {
	switch {
	case r.ProductKey == "", r.Version == "", r.FullURL == "", r.SHA256 == "", r.Signature == "":
		return fmt.Errorf("%w: product_key/version/full_url/sha256/signature required", ErrBadParam)
	case len(r.SHA256) != 64:
		return fmt.Errorf("%w: sha256 must be 64 hex chars", ErrBadParam)
	case r.FullSize <= 0:
		return fmt.Errorf("%w: full_size must be > 0", ErrBadParam)
	}
	return nil
}

func (s *Service) CreateFirmware(ctx context.Context, r FirmwareReq) (int64, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	var id int64
	err := s.DB.QueryRow(ctx,
		`INSERT INTO iot_global.firmware(product_key,version,full_url,full_size,sha256,signature,release_note,status)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,'released') RETURNING id`,
		r.ProductKey, r.Version, r.FullURL, r.FullSize, r.SHA256, r.Signature, nilIfEmpty(r.ReleaseNote)).Scan(&id)
	if err != nil {
		switch sqlState(err) {
		case "23503":
			return 0, fmt.Errorf("%w: unknown product_key", ErrBadParam)
		case "23505":
			return 0, fmt.Errorf("%w: firmware version already exists", ErrConflict)
		}
		return 0, fmt.Errorf("insert firmware: %w", err)
	}
	s.M.Inc("firmwares_created")
	return id, nil
}

// ---------- batch ----------

type BatchReq struct {
	FirmwareID    int64    `json:"firmware_id"`
	Stage         string   `json:"stage"`
	CreatedBy     string   `json:"created_by"`
	ApprovedBy    string   `json:"approved_by,omitempty"`
	FailRatioFuse *float64 `json:"fail_ratio_fuse,omitempty"`
}

type Batch struct {
	ID            int64      `json:"id"`
	FirmwareID    int64      `json:"firmware_id"`
	Stage         string     `json:"stage"`
	Status        string     `json:"status"`
	TargetTotal   int        `json:"target_total"`
	OkCount       int        `json:"ok_count"`
	FailCount     int        `json:"fail_count"`
	FailRatioFuse float64    `json:"fail_ratio_fuse"`
	CreatedBy     string     `json:"created_by"`
	ApprovedBy    *string    `json:"approved_by,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	PausedAt      *time.Time `json:"paused_at,omitempty"`
	FusedAt       *time.Time `json:"fused_at,omitempty"`
}

// CreateBatch 建批次并圈选设备。stage='100' 且 approved_by 为空时 **由 PG CHECK 拒绝**，
// 代码只负责把 23514 翻译成 ErrNeedsApproval——约束即护栏，不在代码里重复判断。
func (s *Service) CreateBatch(ctx context.Context, r BatchReq) (*Batch, error) {
	pct, err := StagePct(r.Stage)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadParam, err)
	}
	if r.FirmwareID <= 0 || r.CreatedBy == "" {
		return nil, fmt.Errorf("%w: firmware_id/created_by required", ErrBadParam)
	}
	fuse := DefaultFuseRatio
	if r.FailRatioFuse != nil {
		if *r.FailRatioFuse < 0 || *r.FailRatioFuse > 1 {
			return nil, fmt.Errorf("%w: fail_ratio_fuse must be in [0,1]", ErrBadParam)
		}
		fuse = *r.FailRatioFuse
	}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var productKey, version string
	err = tx.QueryRow(ctx, `SELECT product_key, version FROM iot_global.firmware WHERE id=$1`, r.FirmwareID).Scan(&productKey, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: firmware", ErrNotFound)
	}
	if err != nil {
		return nil, err
	}

	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO iot_global.ota_batch(firmware_id,stage,status,fail_ratio_fuse,created_by,approved_by)
		 VALUES ($1,$2,'running',$3,$4,$5) RETURNING id`,
		r.FirmwareID, r.Stage, fuse, r.CreatedBy, nilIfEmpty(r.ApprovedBy)).Scan(&id)
	if err != nil {
		if sqlState(err) == "23514" {
			s.M.Inc("batches_rejected_unapproved")
			return nil, ErrNeedsApproval
		}
		return nil, fmt.Errorf("insert batch: %w", err)
	}

	// 圈选：同产品、固件版本不同（含 NULL），且未被同一固件的其它批次圈过（累进档不重复计数）。
	rows, err := tx.Query(ctx,
		`SELECT d.sn FROM iot_shard.device d
		 WHERE d.product_key=$1 AND d.fw_version IS DISTINCT FROM $2
		   AND NOT EXISTS (SELECT 1 FROM iot_shard.ota_device_task t
		                   JOIN iot_global.ota_batch b ON b.id=t.batch_id
		                   WHERE b.firmware_id=$3 AND t.sn=d.sn)
		 ORDER BY d.sn`, productKey, version, r.FirmwareID)
	if err != nil {
		return nil, err
	}
	var targets []string
	for rows.Next() {
		var sn string
		if err := rows.Scan(&sn); err != nil {
			rows.Close()
			return nil, err
		}
		if InStage(sn, pct) {
			targets = append(targets, sn)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(targets) > 0 {
		_, err = tx.Exec(ctx,
			`INSERT INTO iot_shard.ota_device_task(batch_id, sn, status)
			 SELECT $1, unnest($2::text[]), 'pending' ON CONFLICT DO NOTHING`, id, targets)
		if err != nil {
			return nil, fmt.Errorf("insert tasks: %w", err)
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE iot_global.ota_batch SET target_total=$2 WHERE id=$1`, id, len(targets)); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.M.Inc("batches_created")
	s.M.Add("tasks_targeted", int64(len(targets)))
	slog.Info("ota batch created", "batch_id", id, "firmware_id", r.FirmwareID, "stage", r.Stage, "targets", len(targets))
	return s.GetBatch(ctx, id)
}

func (s *Service) GetBatch(ctx context.Context, id int64) (*Batch, error) {
	var b Batch
	err := s.DB.QueryRow(ctx,
		`SELECT id,firmware_id,stage,status,target_total,ok_count,fail_count,fail_ratio_fuse::float8,created_by,approved_by,created_at,paused_at,fused_at
		 FROM iot_global.ota_batch WHERE id=$1`, id).Scan(
		&b.ID, &b.FirmwareID, &b.Stage, &b.Status, &b.TargetTotal, &b.OkCount, &b.FailCount, &b.FailRatioFuse,
		&b.CreatedBy, &b.ApprovedBy, &b.CreatedAt, &b.PausedAt, &b.FusedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: batch %d", ErrNotFound, id)
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// BatchView 是 GET /batches/{id} 的响应：计数 + fail_ratio + 任务状态直方图。
type BatchView struct {
	Batch
	FailRatio   float64        `json:"fail_ratio"`
	TaskStatus  map[string]int `json:"task_status"`
	AllTerminal bool           `json:"all_terminal"`
}

func (s *Service) GetBatchView(ctx context.Context, id int64) (*BatchView, error) {
	b, err := s.GetBatch(ctx, id)
	if err != nil {
		return nil, err
	}
	hist, err := s.taskHistogram(ctx, id)
	if err != nil {
		return nil, err
	}
	v := &BatchView{Batch: *b, FailRatio: FailRatio(b.OkCount, b.FailCount), TaskStatus: hist, AllTerminal: true}
	for st, n := range hist {
		if !IsTerminal(st) && n > 0 {
			v.AllTerminal = false
		}
	}
	return v, nil
}

func (s *Service) taskHistogram(ctx context.Context, batchID int64) (map[string]int, error) {
	rows, err := s.DB.Query(ctx, `SELECT status, count(*) FROM iot_shard.ota_device_task WHERE batch_id=$1 GROUP BY status`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	hist := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		hist[st] = n
	}
	return hist, rows.Err()
}

// Pause：running → paused。
func (s *Service) Pause(ctx context.Context, id int64) error {
	return s.transition(ctx, id, `UPDATE iot_global.ota_batch SET status='paused', paused_at=now() WHERE id=$1 AND status='running'`, "batches_paused")
}

// Resume：paused|fused → running。fused 没有自动恢复，这是唯一出口（人工）。
func (s *Service) Resume(ctx context.Context, id int64) error {
	return s.transition(ctx, id, `UPDATE iot_global.ota_batch SET status='running' WHERE id=$1 AND status IN ('paused','fused')`, "batches_resumed")
}

func (s *Service) transition(ctx context.Context, id int64, sql, metric string) error {
	tag, err := s.DB.Exec(ctx, sql, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if _, err := s.GetBatch(ctx, id); err != nil {
			return err
		}
		return ErrConflict
	}
	s.M.Inc(metric)
	return nil
}

// Advance 为同一固件创建下一档批次：要求当前批次全部任务终态、失败率不超过阈值、非 fused；
// 下一档为 100 时 approvedBy 必填（否则同样由 CHECK 拒绝 → 403）。当前批次标记 completed。
func (s *Service) Advance(ctx context.Context, id int64, createdBy, approvedBy string) (*Batch, error) {
	v, err := s.GetBatchView(ctx, id)
	if err != nil {
		return nil, err
	}
	next, ok := NextStage(v.Stage)
	if !ok {
		return nil, fmt.Errorf("%w: stage 100 has no next stage", ErrConflict)
	}
	if v.Status == BatchFused {
		return nil, fmt.Errorf("%w: batch is fused; resume first", ErrConflict)
	}
	if !v.AllTerminal {
		return nil, fmt.Errorf("%w: batch has non-terminal tasks", ErrConflict)
	}
	if v.FailRatio > v.FailRatioFuse {
		return nil, fmt.Errorf("%w: fail ratio %.4f exceeds threshold %.4f", ErrConflict, v.FailRatio, v.FailRatioFuse)
	}
	if createdBy == "" {
		createdBy = v.CreatedBy
	}
	fuse := v.FailRatioFuse
	nb, err := s.CreateBatch(ctx, BatchReq{FirmwareID: v.FirmwareID, Stage: next, CreatedBy: createdBy, ApprovedBy: approvedBy, FailRatioFuse: &fuse})
	if err != nil {
		return nil, err
	}
	if _, err := s.DB.Exec(ctx, `UPDATE iot_global.ota_batch SET status='completed' WHERE id=$1 AND status IN ('running','paused')`, id); err != nil {
		slog.Warn("mark previous batch completed failed", "batch_id", id, "err", err)
	}
	s.M.Inc("batches_advanced")
	return nb, nil
}

// ---------- dispatch ----------

type otaCmd struct {
	CmdID  string    `json:"cmd_id"`
	Action string    `json:"action"`
	Params otaParams `json:"params"`
}
type otaParams struct {
	BatchID int64          `json:"batch_id"`
	URL     string         `json:"url"`
	Size    int64          `json:"size"`
	SHA256  string         `json:"sha256"`
	Sig     string         `json:"sig"`
	Policy  map[string]any `json:"policy"`
}

// RunDispatcher 周期扫描 running 批次的 pending 任务并限速下发。
func (s *Service) RunDispatcher(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := s.DispatchOnce(ctx); err != nil && ctx.Err() == nil {
				slog.Error("dispatch round failed", "err", err)
			} else if n > 0 {
				slog.Info("dispatched", "tasks", n)
			}
		}
	}
}

// DispatchOnce 下发一轮；paused/fused 批次不会被选中，且每 chunk 复查批次状态。
func (s *Service) DispatchOnce(ctx context.Context) (int, error) {
	rows, err := s.DB.Query(ctx,
		`SELECT b.id, f.full_url, f.full_size, f.sha256, f.signature
		 FROM iot_global.ota_batch b JOIN iot_global.firmware f ON f.id=b.firmware_id
		 WHERE b.status='running' ORDER BY b.id`)
	if err != nil {
		return 0, err
	}
	type bt struct {
		id       int64
		url, sha string
		size     int64
		sig      string
	}
	var batches []bt
	for rows.Next() {
		var b bt
		if err := rows.Scan(&b.id, &b.url, &b.size, &b.sha, &b.sig); err != nil {
			rows.Close()
			return 0, err
		}
		batches = append(batches, b)
	}
	rows.Close()
	total := 0
	for _, b := range batches {
		for {
			tasks, err := s.pendingTasks(ctx, b.id, 200)
			if err != nil || len(tasks) == 0 {
				if err != nil {
					return total, err
				}
				break
			}
			var status string
			if err := s.DB.QueryRow(ctx, `SELECT status FROM iot_global.ota_batch WHERE id=$1`, b.id).Scan(&status); err != nil || status != BatchRunning {
				break // paused/fused 中途停止
			}
			for _, t := range tasks {
				if err := s.limiter.Wait(ctx); err != nil {
					return total, err
				}
				cmd := otaCmd{CmdID: newUUID(), Action: "ota", Params: otaParams{
					BatchID: b.id, URL: b.url, Size: b.size, SHA256: b.sha, Sig: b.sig, Policy: map[string]any{"idle_only": true}}}
				payload, _ := json.Marshal(cmd)
				if err := s.Pub.Publish(ctx, "down/"+t.sn+"/cmd", payload); err != nil {
					s.M.Inc("publish_errors")
					return total, fmt.Errorf("publish %s: %w", t.sn, err)
				}
				tag, err := s.DB.Exec(ctx, `UPDATE iot_shard.ota_device_task SET status='notified', updated_at=now() WHERE id=$1 AND status='pending'`, t.id)
				if err != nil {
					return total, err
				}
				if tag.RowsAffected() == 1 {
					total++
					s.M.Inc("tasks_dispatched")
				}
			}
		}
	}
	return total, nil
}

type task struct {
	id int64
	sn string
}

func (s *Service) pendingTasks(ctx context.Context, batchID int64, limit int) ([]task, error) {
	rows, err := s.DB.Query(ctx, `SELECT id, sn FROM iot_shard.ota_device_task WHERE batch_id=$1 AND status='pending' ORDER BY id LIMIT $2`, batchID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []task
	for rows.Next() {
		var t task
		if err := rows.Scan(&t.id, &t.sn); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ---------- progress ----------

type progressPayload struct {
	Seq       int64  `json:"seq"`
	Ts        int64  `json:"ts"`
	BatchID   int64  `json:"batch_id"`
	Phase     string `json:"phase"`
	Pct       int    `json:"pct"`
	ErrorCode string `json:"error_code"`
}

// ProgressOutcome 用于计数/测试。
type ProgressOutcome string

const (
	ProgressDropped  ProgressOutcome = "dropped"  // 解析失败/非法 phase
	ProgressIgnored  ProgressOutcome = "ignored"  // 任务不存在或已终态（重复终态上报）
	ProgressUpdated  ProgressOutcome = "updated"  // 非终态更新
	ProgressTerminal ProgressOutcome = "terminal" // 首次进入终态并计数
	ProgressFused    ProgressOutcome = "fused"    // 本次计数触发熔断
)

// HandleProgress 处理一条 ota_progress 信封。返回 err 表示瞬时故障（Nak）。
func (s *Service) HandleProgress(ctx context.Context, env *envelope.Envelope) (ProgressOutcome, error) {
	s.M.Inc("progress_total")
	var p progressPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil || p.BatchID <= 0 || !ValidPhase(p.Phase) {
		s.M.Inc("progress_bad")
		return ProgressDropped, nil
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	retryInc := 0
	if p.Phase == TaskFailed {
		retryInc = 1
	}
	tag, err := tx.Exec(ctx,
		`UPDATE iot_shard.ota_device_task
		 SET status=$3, retry=retry+$4, error_code=$5, updated_at=now()
		 WHERE batch_id=$1 AND sn=$2 AND NOT (status = ANY($6))`,
		p.BatchID, env.SN, p.Phase, retryInc, nilIfEmpty(p.ErrorCode), TerminalStates())
	if err != nil {
		return "", fmt.Errorf("update task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		s.M.Inc("progress_ignored")
		return ProgressIgnored, tx.Commit(ctx)
	}
	if !IsTerminal(p.Phase) {
		s.M.Inc("progress_updated")
		return ProgressUpdated, tx.Commit(ctx)
	}

	okInc, failInc := 0, 0
	if p.Phase == TaskSuccess {
		okInc = 1
	} else {
		failInc = 1
	}
	// 计数与任务状态同一事务；paused 期间仍计数（设备已在升级），但只有 running 才评估熔断。
	var ok, fail int
	var threshold float64
	var status string
	err = tx.QueryRow(ctx,
		`UPDATE iot_global.ota_batch SET ok_count=ok_count+$2, fail_count=fail_count+$3
		 WHERE id=$1 AND status IN ('running','paused')
		 RETURNING ok_count, fail_count, fail_ratio_fuse::float8, status`, p.BatchID, okInc, failInc).
		Scan(&ok, &fail, &threshold, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		// 批次已 fused/completed：任务状态仍落地，但不再计数
		s.M.Inc("progress_terminal_uncounted")
		return ProgressTerminal, tx.Commit(ctx)
	}
	if err != nil {
		return "", fmt.Errorf("update batch counts: %w", err)
	}
	outcome := ProgressTerminal
	if status == BatchRunning && ShouldFuse(ok, fail, threshold, s.MinSamples) {
		tag, err := tx.Exec(ctx, `UPDATE iot_global.ota_batch SET status='fused', fused_at=now() WHERE id=$1 AND status='running'`, p.BatchID)
		if err != nil {
			return "", fmt.Errorf("fuse batch: %w", err)
		}
		if tag.RowsAffected() == 1 {
			outcome = ProgressFused
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	s.M.Inc("progress_terminal")
	if outcome == ProgressFused {
		s.M.Inc("batches_fused")
		slog.Error("OTA BATCH FUSED (alert): fail ratio above threshold, manual resume required",
			"batch_id", p.BatchID, "ok", ok, "fail", fail, "fail_ratio", FailRatio(ok, fail), "threshold", threshold)
	}
	return outcome, nil
}

// ---------- helpers ----------

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// newUUID 生成 UUIDv4（crypto/rand）。
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
