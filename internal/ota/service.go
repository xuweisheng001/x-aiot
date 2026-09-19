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
	// MinAbsFail 绝对数熔断阈值（默认 5；<=0 关闭），见 ShouldFuseAbs。
	MinAbsFail int
	// Now 供测试固定时钟（维护窗口判定用）；nil = time.Now。
	Now func() time.Time
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func NewService(db *pgxpool.Pool, pub Publisher, m *Metrics, dispatchRPS int) *Service {
	if dispatchRPS <= 0 {
		dispatchRPS = DefaultDispatchRPS
	}
	return &Service{DB: db, Pub: pub, M: m, MinSamples: MinFuseSamples, MinAbsFail: DefaultMinAbsFail,
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
	// ExplicitSNs 非空时跳过 product_key + md5 抽样圈选，直接按给定 SN 建任务（BL5 §08 组织子批次）。
	ExplicitSNs []string `json:"explicit_sns,omitempty"`
	// ParentBatchID 非空表示这是某个平台批次的子批次：不参与该固件的档位顺序链，
	// 档位 / 审批人 / 熔断阈值一律继承父批次（组织只决定「哪些设备、什么时候」，§8.2）。
	ParentBatchID *int64 `json:"parent_batch_id,omitempty"`
	// Policy 是下发策略，目前只识别 window（维护窗口，见 window.go）。
	Policy json.RawMessage `json:"policy,omitempty"`
}

type Batch struct {
	ID            int64      `json:"id"`
	FirmwareID    int64      `json:"firmware_id"`
	ParentBatchID *int64     `json:"parent_batch_id,omitempty"`
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
	if r.FirmwareID <= 0 || r.CreatedBy == "" {
		return nil, fmt.Errorf("%w: firmware_id/created_by required", ErrBadParam)
	}
	// 窗口在建批时就校验：错格式当场 400，而不是等 DispatchOnce 静默跳过（window.go）。
	if _, err := ParseWindow(r.Policy); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadParam, err)
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

	if r.ParentBatchID != nil {
		// 子批次（组织批量 OTA）：**不参与父固件的档位顺序链**——否则每个组织都要从 0.1 重来一遍。
		// 档位、审批人、熔断阈值一律继承父批次：组织只决定「哪些设备、什么时候」（§8.2），
		// ck_full_stage_approved 与熔断照常生效（继承来的 approved_by 就是父批次的双人审批记录）。
		var pStage string
		var pApproved *string
		var pFuse float64
		var pFirmware int64
		var pParent *int64
		err = tx.QueryRow(ctx,
			`SELECT firmware_id, parent_batch_id, stage, approved_by, fail_ratio_fuse::float8
			 FROM iot_global.ota_batch WHERE id=$1 FOR UPDATE`, *r.ParentBatchID).
			Scan(&pFirmware, &pParent, &pStage, &pApproved, &pFuse)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: parent batch %d", ErrNotFound, *r.ParentBatchID)
		}
		if err != nil {
			return nil, err
		}
		if pParent != nil {
			return nil, fmt.Errorf("%w: batch %d is itself a sub-batch", ErrBadParam, *r.ParentBatchID)
		}
		if pFirmware != r.FirmwareID {
			return nil, fmt.Errorf("%w: parent batch %d belongs to firmware %d", ErrBadParam, *r.ParentBatchID, pFirmware)
		}
		if r.Stage == "" {
			r.Stage = pStage
		}
		if r.ApprovedBy == "" && pApproved != nil {
			r.ApprovedBy = *pApproved
		}
		fuse = pFuse // 请求里的 fail_ratio_fuse 一律忽略：子批次不能放宽熔断
	} else {
		// 档位顺序：同一固件必须 0.1 → 1 → 10 → 50 → 100 逐档建批，不允许越级（预推演 INC-17）。
		// FOR UPDATE 锁住最新**平台**批次行（子批次不入链，故 parent_batch_id IS NULL），防止并发建两个"下一档"。
		var latest string
		err = tx.QueryRow(ctx, `SELECT stage FROM iot_global.ota_batch WHERE firmware_id=$1 AND parent_batch_id IS NULL ORDER BY id DESC LIMIT 1 FOR UPDATE`, r.FirmwareID).Scan(&latest)
		exists := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		expected, ok := ExpectedNextStage(latest, exists)
		if !ok {
			return nil, fmt.Errorf("%w: firmware %d already has a stage-100 batch", ErrConflict, r.FirmwareID)
		}
		if r.Stage != expected {
			s.M.Inc("batches_rejected_stage_order")
			if exists {
				return nil, fmt.Errorf("%w: expected stage %s (latest batch is %s), got %s", ErrConflict, expected, latest, r.Stage)
			}
			return nil, fmt.Errorf("%w: first batch of a firmware must be stage %s, got %s", ErrConflict, expected, r.Stage)
		}
	}

	pct, err := StagePct(r.Stage)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadParam, err)
	}

	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO iot_global.ota_batch(firmware_id,parent_batch_id,stage,status,fail_ratio_fuse,created_by,approved_by,policy)
		 VALUES ($1,$2,$3,'running',$4,$5,$6,$7::jsonb) RETURNING id`,
		r.FirmwareID, r.ParentBatchID, r.Stage, fuse, r.CreatedBy, nilIfEmpty(r.ApprovedBy), nilIfEmptyJSON(r.Policy)).Scan(&id)
	if err != nil {
		if sqlState(err) == "23514" {
			s.M.Inc("batches_rejected_unapproved")
			return nil, ErrNeedsApproval
		}
		return nil, fmt.Errorf("insert batch: %w", err)
	}

	var targets []string
	if len(r.ExplicitSNs) > 0 {
		// 显式 SN（组织子批次）：跳过 product_key + md5 抽样圈选，只保留 iot_shard.device 里真实存在的 SN，
		// 不存在的忽略并计数（组织名单可能含已退役 / 未激活设备，不该让整批失败）。
		targets, err = existingSNs(ctx, tx, r.ExplicitSNs)
		if err != nil {
			return nil, err
		}
		if miss := len(dedupSNs(r.ExplicitSNs)) - len(targets); miss > 0 {
			s.M.Add("explicit_sns_unknown", int64(miss))
		}
	} else if targets, err = s.sampleTargets(ctx, tx, productKey, version, r.FirmwareID, pct); err != nil {
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
	slog.Info("ota batch created", "batch_id", id, "firmware_id", r.FirmwareID, "stage", r.Stage,
		"parent_batch_id", r.ParentBatchID, "explicit", len(r.ExplicitSNs) > 0, "targets", len(targets))
	return s.GetBatch(ctx, id)
}

// existingSNs 返回 sns 中真实存在于 iot_shard.device 的那些（去重、有序）。
func existingSNs(ctx context.Context, tx pgx.Tx, sns []string) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT sn FROM iot_shard.device WHERE sn = ANY($1) ORDER BY sn`, dedupSNs(sns))
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

func dedupSNs(sns []string) []string {
	seen := make(map[string]bool, len(sns))
	out := make([]string, 0, len(sns))
	for _, sn := range sns {
		if sn == "" || seen[sn] {
			continue
		}
		seen[sn] = true
		out = append(out, sn)
	}
	return out
}

// sampleTargets 是平台批次的圈选：同产品、固件版本不同（含 NULL），且未被同一固件的其它批次圈过
// （累进档不重复计数），再按 stage 百分比 md5(sn) 稳定抽样。
func (s *Service) sampleTargets(ctx context.Context, tx pgx.Tx, productKey, version string, firmwareID int64, pct float64) ([]string, error) {
	rows, err := tx.Query(ctx,
		`SELECT d.sn FROM iot_shard.device d
		 WHERE d.product_key=$1 AND d.fw_version IS DISTINCT FROM $2
		   AND NOT EXISTS (SELECT 1 FROM iot_shard.ota_device_task t
		                   JOIN iot_global.ota_batch b ON b.id=t.batch_id
		                   WHERE b.firmware_id=$3 AND t.sn=d.sn)
		 ORDER BY d.sn`, productKey, version, firmwareID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []string
	for rows.Next() {
		var sn string
		if err := rows.Scan(&sn); err != nil {
			return nil, err
		}
		if InStage(sn, pct) {
			targets = append(targets, sn)
		}
	}
	return targets, rows.Err()
}

func (s *Service) GetBatch(ctx context.Context, id int64) (*Batch, error) {
	var b Batch
	err := s.DB.QueryRow(ctx,
		`SELECT id,firmware_id,parent_batch_id,stage,status,target_total,ok_count,fail_count,fail_ratio_fuse::float8,created_by,approved_by,created_at,paused_at,fused_at
		 FROM iot_global.ota_batch WHERE id=$1`, id).Scan(
		&b.ID, &b.FirmwareID, &b.ParentBatchID, &b.Stage, &b.Status, &b.TargetTotal, &b.OkCount, &b.FailCount, &b.FailRatioFuse,
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
		`SELECT b.id, f.full_url, f.full_size, f.sha256, f.signature, b.policy
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
		policy   []byte
	}
	var batches []bt
	for rows.Next() {
		var b bt
		if err := rows.Scan(&b.id, &b.url, &b.size, &b.sha, &b.sig, &b.policy); err != nil {
			rows.Close()
			return 0, err
		}
		batches = append(batches, b)
	}
	rows.Close()
	now := s.now()
	total := 0
	for _, b := range batches {
		// 维护窗口：窗口外整批跳过（BL5 §08）。解析失败当作无窗口，不阻塞下发（建批时已校验过）。
		if w, err := ParseWindow(b.policy); err == nil && !InWindow(w, now) {
			s.M.Inc("window_skipped")
			continue
		}
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
	retryInc := 0
	if p.Phase == TaskFailed {
		retryInc = 1
	}
	return s.applyPhase(ctx, phaseChange{batchID: p.BatchID, sn: env.SN, phase: p.Phase, errorCode: p.ErrorCode, retryInc: retryInc, metricPrefix: "progress"})
}

// phaseChange 是一次任务状态变更请求：设备上报（HandleProgress）与 stale sweeper（SweepStale）共用同一事务逻辑。
type phaseChange struct {
	batchID   int64
	sn        string
	phase     string
	errorCode string
	retryInc  int
	// onlyIfStaleFor > 0 时，仅当任务 updated_at 早于 now-onlyIfStaleFor 才变更（sweeper 的乐观守卫：
	// SELECT 与 UPDATE 之间设备若刚上报过进度，则本轮放过）。
	onlyIfStaleFor time.Duration
	// metricPrefix：progress | stale，让两条来源的计数可分开观察。
	metricPrefix string
}

// applyPhase 在一个事务里：更新任务状态（终态不可再迁）→ 终态时给批次计数 → running 批次评估熔断。
func (s *Service) applyPhase(ctx context.Context, c phaseChange) (ProgressOutcome, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE iot_shard.ota_device_task
		 SET status=$3, retry=retry+$4, error_code=$5, updated_at=now()
		 WHERE batch_id=$1 AND sn=$2 AND NOT (status = ANY($6))
		   AND ($7::float8 <= 0 OR updated_at < now() - make_interval(secs => $7))`,
		c.batchID, c.sn, c.phase, c.retryInc, nilIfEmpty(c.errorCode), TerminalStates(), c.onlyIfStaleFor.Seconds())
	if err != nil {
		return "", fmt.Errorf("update task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		s.M.Inc(c.metricPrefix + "_ignored")
		return ProgressIgnored, tx.Commit(ctx)
	}
	if !IsTerminal(c.phase) {
		s.M.Inc(c.metricPrefix + "_updated")
		return ProgressUpdated, tx.Commit(ctx)
	}

	okInc, failInc := 0, 0
	if c.phase == TaskSuccess {
		okInc = 1
	} else {
		failInc = 1 // failed 与 rolled_back 都计失败
	}
	// 计数与任务状态同一事务；paused 期间仍计数（设备已在升级），但只有 running 才评估熔断。
	var ok, fail int
	var threshold float64
	var status string
	err = tx.QueryRow(ctx,
		`UPDATE iot_global.ota_batch SET ok_count=ok_count+$2, fail_count=fail_count+$3
		 WHERE id=$1 AND status IN ('running','paused')
		 RETURNING ok_count, fail_count, fail_ratio_fuse::float8, status`, c.batchID, okInc, failInc).
		Scan(&ok, &fail, &threshold, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		// 批次已 fused/completed：任务状态仍落地，但不再计数
		s.M.Inc(c.metricPrefix + "_terminal_uncounted")
		return ProgressTerminal, tx.Commit(ctx)
	}
	if err != nil {
		return "", fmt.Errorf("update batch counts: %w", err)
	}
	outcome := ProgressTerminal
	if status == BatchRunning && ShouldFuseAbs(ok, fail, threshold, s.MinSamples, s.MinAbsFail) {
		tag, err := tx.Exec(ctx, `UPDATE iot_global.ota_batch SET status='fused', fused_at=now() WHERE id=$1 AND status='running'`, c.batchID)
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
	s.M.Inc(c.metricPrefix + "_terminal")
	if outcome == ProgressFused {
		s.M.Inc("batches_fused")
		slog.Error("OTA BATCH FUSED (alert): fail ratio/abs above threshold, manual resume required",
			"batch_id", c.batchID, "ok", ok, "fail", fail, "fail_ratio", FailRatio(ok, fail), "threshold", threshold,
			"min_abs_fail", s.MinAbsFail, "source", c.metricPrefix)
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

// nilIfEmptyJSON 把空 policy 写成 SQL NULL（而不是字符串 ""，那会让 ::jsonb 转换报错）。
func nilIfEmptyJSON(raw json.RawMessage) *string {
	if len(raw) == 0 {
		return nil
	}
	s := string(raw)
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
