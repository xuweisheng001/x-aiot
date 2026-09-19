package reco

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// TDQuerier 是安全关联依赖的 TDengine 子集；nil 表示跳过该步。
type TDQuerier interface {
	Query(ctx context.Context, sql string) (*tdengine.Result, error)
}

// Runner 编排一轮离线批：候选生成 → 功率上限 → 安全关联 → 校正分布。
type Runner struct {
	DB         *pgxpool.Pool
	RDB        *redis.Client
	TD         TDQuerier
	Rules      Rules
	WindowDays int
	Now        func() time.Time
}

// Report 是一轮批处理的统计，全部数字写日志，不落库。
type Report struct {
	Rows, Groups, Candidates int
	Excluded                 map[string]int
	Upserted, PowerCapped    int
	NoParams, UnknownDims    int
	SafetyChecked            int
	SafetyRemoved            int
	SafetySkipped            bool
	CorrectionDevices        int
	CorrectionCapShare       float64
	CorrectionAlarm          bool
}

func NewRunner(db *pgxpool.Pool, rdb *redis.Client, td TDQuerier, windowDays int) *Runner {
	if windowDays <= 0 {
		windowDays = 30
	}
	return &Runner{DB: db, RDB: rdb, TD: td, Rules: DefaultRules(), WindowDays: windowDays, Now: time.Now}
}

// RunOnce 执行一轮；候选生成阶段的错误返回 err，安全关联与校正分布失败只 WARN。
func (r *Runner) RunOnce(ctx context.Context) (Report, error) {
	rep := Report{Excluded: map[string]int{}}
	rows, err := r.loadFeedbackRows(ctx)
	if err != nil {
		return rep, err
	}
	rep.Rows = len(rows)
	cands := Aggregate(rows, r.Rules)
	rep.Groups = len(cands)
	for _, c := range cands {
		if c.Excluded {
			rep.Excluded[c.Reason]++
			if c.Reason == ReasonUnknownDims {
				rep.UnknownDims++
			}
			continue
		}
		rep.Candidates++
		params, err := r.lookupParams(ctx, c.GroupKey)
		if err != nil {
			return rep, err
		}
		if params == nil {
			rep.NoParams++
			continue
		}
		official, err := r.officialPower(ctx, c.GroupKey)
		if err != nil {
			return rep, err
		}
		status, reason := "candidate", ""
		if p, ok := paramsPower(params); ok && !WithinPowerCap(p, official) {
			status, reason = "removed", "power_cap"
			rep.PowerCapped++
		}
		if err := r.upsertRecommendation(ctx, c, params, status, reason); err != nil {
			return rep, err
		}
		rep.Upserted++
	}
	if r.TD != nil {
		if err := r.safetyPass(ctx, &rep); err != nil {
			rep.SafetySkipped = true
			slog.Warn("reco safety pass skipped", "err", err)
		}
	} else {
		rep.SafetySkipped = true
	}
	if err := r.correctionPass(ctx, &rep); err != nil {
		slog.Warn("reco correction pass skipped", "err", err)
	}
	slog.Info("reco run", "rows", rep.Rows, "groups", rep.Groups, "candidates", rep.Candidates, "upserted", rep.Upserted,
		"power_capped", rep.PowerCapped, "no_params", rep.NoParams, "excluded", rep.Excluded,
		"safety_checked", rep.SafetyChecked, "safety_removed", rep.SafetyRemoved, "safety_skipped", rep.SafetySkipped,
		"correction_devices", rep.CorrectionDevices, "correction_cap_share", fmt.Sprintf("%.3f", rep.CorrectionCapShare), "correction_alarm", rep.CorrectionAlarm)
	return rep, nil
}

// Run 常驻：立即跑一轮，之后每 interval 一轮。
func (r *Runner) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("reco run failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ---- 候选生成 ----

func (r *Runner) loadFeedbackRows(ctx context.Context) ([]FeedbackRow, error) {
	since := r.Now().Add(-time.Duration(r.WindowDays) * 24 * time.Hour)
	rows, err := r.DB.Query(ctx,
		`SELECT r.sn, r.material_id, r.params_hash, f.rating, f.created_at
		   FROM iot_shard.job_record r JOIN iot_shard.job_feedback f ON f.job_id = r.job_id
		  WHERE f.created_at >= $1 AND r.params_hash IS NOT NULL AND r.material_id IS NOT NULL`, since)
	if err != nil {
		return nil, fmt.Errorf("load feedback: %w", err)
	}
	defer rows.Close()
	dims := map[string][2]string{} // sn → (pk, module)
	var out []FeedbackRow
	for rows.Next() {
		var fr FeedbackRow
		if err := rows.Scan(&fr.SN, &fr.MaterialID, &fr.ParamsHash, &fr.Rating, &fr.At); err != nil {
			return nil, err
		}
		fr.ParamsHash = strings.TrimSpace(fr.ParamsHash)
		d, ok := dims[fr.SN]
		if !ok {
			d = r.deviceDims(ctx, fr.SN)
			dims[fr.SN] = d
		}
		fr.ProductKey, fr.ModuleModel = d[0], d[1]
		out = append(out, fr)
	}
	return out, rows.Err()
}

// deviceDims 从 Redis 维表 device:{sn} 取 pk（缺失回退查 iot_shard.device）、从影子 reported 取 module_model；
// 取不到用 UNKNOWN（聚合时排除）。
func (r *Runner) deviceDims(ctx context.Context, sn string) [2]string {
	pk, module := "UNKNOWN", "UNKNOWN"
	if r.RDB != nil {
		if v, err := r.RDB.HGet(ctx, "device:"+sn, "pk").Result(); err == nil && v != "" {
			pk = v
		}
		if rep, err := shadow.Read(ctx, r.RDB, sn); err == nil {
			if v := strings.TrimSpace(rep["module_model"]); v != "" {
				module = v
			}
		}
	}
	if pk == "UNKNOWN" && r.DB != nil {
		var v string
		if err := r.DB.QueryRow(ctx, `SELECT product_key FROM iot_shard.device WHERE sn=$1`, sn).Scan(&v); err == nil && v != "" {
			pk = v
		}
	}
	return [2]string{pk, module}
}

// lookupParams 在 user_param 里找规范化 hash 相等的参数；找不到返回 nil。
func (r *Runner) lookupParams(ctx context.Context, k GroupKey) (json.RawMessage, error) {
	rows, err := r.DB.Query(ctx,
		`SELECT params FROM iot_shard.user_param WHERE product_key=$1 AND module_model=$2 AND material_id=$3 LIMIT 500`,
		k.ProductKey, k.ModuleModel, k.MaterialID)
	if err != nil {
		return nil, fmt.Errorf("lookup params: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw json.RawMessage
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if CanonicalHash(raw) == k.ParamsHash {
			return raw, nil
		}
	}
	return nil, rows.Err()
}

// officialPower 返回同维度最新有效官方档的功率；无官方档返回 0。
func (r *Runner) officialPower(ctx context.Context, k GroupKey) (float64, error) {
	var p *float64
	err := r.DB.QueryRow(ctx,
		`SELECT (params->>'power')::float8 FROM iot_global.param_profile
		  WHERE product_key=$1 AND module_model=$2 AND material_id=$3 AND source='official' AND version_removed IS NULL
		  ORDER BY version_added DESC LIMIT 1`, k.ProductKey, k.ModuleModel, k.MaterialID).Scan(&p)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return 0, nil
		}
		return 0, fmt.Errorf("official power: %w", err)
	}
	if p == nil {
		return 0, nil
	}
	return *p, nil
}

func paramsPower(raw json.RawMessage) (float64, bool) {
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return 0, false
	}
	switch v := m["power"].(type) {
	case float64:
		return v, true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	}
	return 0, false
}

// upsertRecommendation 写候选；已 published / removed 的状态不回退为 candidate，功率超限则强制 removed。
func (r *Runner) upsertRecommendation(ctx context.Context, c Candidate, params json.RawMessage, status, reason string) error {
	_, err := r.DB.Exec(ctx,
		`INSERT INTO iot_global.param_recommendation
		   (product_key, module_model, material_id, params_hash, params, sample_count, distinct_users, good_ratio, status, removed_reason)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,''))
		 ON CONFLICT (product_key, module_model, material_id, params_hash) DO UPDATE SET
		   params = EXCLUDED.params, sample_count = EXCLUDED.sample_count, distinct_users = EXCLUDED.distinct_users,
		   good_ratio = EXCLUDED.good_ratio, updated_at = now(),
		   status = CASE WHEN EXCLUDED.status = 'removed' THEN 'removed'
		                 WHEN param_recommendation.status IN ('published','removed') THEN param_recommendation.status
		                 ELSE EXCLUDED.status END,
		   removed_reason = CASE WHEN EXCLUDED.status = 'removed' THEN EXCLUDED.removed_reason ELSE param_recommendation.removed_reason END`,
		c.ProductKey, c.ModuleModel, c.MaterialID, c.ParamsHash, params, c.Samples, c.DistinctSN, c.GoodRatio, status, reason)
	if err != nil {
		return fmt.Errorf("upsert recommendation: %w", err)
	}
	return nil
}

// ---- 安全关联 ----

type recoRow struct {
	ID         int64
	MaterialID string
	ParamsHash string
}

func (r *Runner) safetyPass(ctx context.Context, rep *Report) error {
	since := r.Now().Add(-SafetyWindow)
	res, err := r.TD.Query(ctx, BuildSafetyEventsQuery(since))
	if err != nil {
		return fmt.Errorf("safety events: %w", err)
	}
	events, skipped := ParseSafetyEvents(res)
	if skipped > 0 {
		slog.Warn("reco safety: bad event rows skipped", "n", skipped)
	}
	recos, err := r.activeRecommendations(ctx)
	if err != nil {
		return err
	}
	for _, rc := range recos {
		rep.SafetyChecked++
		recoJobs, err := r.jobSpans(ctx, since, rc.MaterialID, rc.ParamsHash, true)
		if err != nil {
			return err
		}
		baseJobs, err := r.jobSpans(ctx, since, rc.MaterialID, rc.ParamsHash, false)
		if err != nil {
			return err
		}
		recoAbs := CountJobsWithSafety(recoJobs, events)
		recoRate := Rate(recoAbs, len(recoJobs))
		baseRate := Rate(CountJobsWithSafety(baseJobs, events), len(baseJobs))
		if SafetyUnpublish(recoRate, baseRate, recoAbs) {
			if _, err := r.DB.Exec(ctx,
				`UPDATE iot_global.param_recommendation SET status='removed', removed_reason='safety', updated_at=now() WHERE id=$1`, rc.ID); err != nil {
				return fmt.Errorf("safety remove: %w", err)
			}
			rep.SafetyRemoved++
			slog.Warn("recommendation removed for safety", "id", rc.ID, "material", rc.MaterialID,
				"reco_rate", fmt.Sprintf("%.3f", recoRate), "baseline_rate", fmt.Sprintf("%.3f", baseRate), "reco_abs", recoAbs)
		}
	}
	return nil
}

func (r *Runner) activeRecommendations(ctx context.Context) ([]recoRow, error) {
	rows, err := r.DB.Query(ctx, `SELECT id, material_id, params_hash FROM iot_global.param_recommendation WHERE status IN ('candidate','published')`)
	if err != nil {
		return nil, fmt.Errorf("active recommendations: %w", err)
	}
	defer rows.Close()
	var out []recoRow
	for rows.Next() {
		var rc recoRow
		if err := rows.Scan(&rc.ID, &rc.MaterialID, &rc.ParamsHash); err != nil {
			return nil, err
		}
		rc.ParamsHash = strings.TrimSpace(rc.ParamsHash)
		out = append(out, rc)
	}
	return out, rows.Err()
}

// jobSpans 返回窗口内同材料、参数 hash 等于（same=true）或不等于（same=false，基线）的任务时间窗。
func (r *Runner) jobSpans(ctx context.Context, since time.Time, material, hash string, same bool) ([]JobSpan, error) {
	op := "="
	if !same {
		op = "<>"
	}
	rows, err := r.DB.Query(ctx,
		`SELECT sn, started_at, finished_at FROM iot_shard.job_record
		  WHERE started_at >= $1 AND material_id = $2 AND params_hash IS NOT NULL AND params_hash `+op+` $3`, since, material, hash)
	if err != nil {
		return nil, fmt.Errorf("job spans: %w", err)
	}
	defer rows.Close()
	var out []JobSpan
	for rows.Next() {
		var js JobSpan
		var fin *time.Time
		if err := rows.Scan(&js.SN, &js.Start, &fin); err != nil {
			return nil, err
		}
		if fin != nil {
			js.End = *fin
		}
		out = append(out, js)
	}
	return out, rows.Err()
}

// ---- 校正系数分布 ----

func (r *Runner) correctionPass(ctx context.Context, rep *Report) error {
	rows, err := r.DB.Query(ctx, `SELECT sn, health FROM iot_shard.consumable_health WHERE part IN ('laser','module') LIMIT 10000`)
	if err != nil {
		return fmt.Errorf("consumable_health: %w", err)
	}
	defer rows.Close()
	cfgCache := map[[2]string]CorrectionCfg{}
	var ks []float64
	for rows.Next() {
		var sn string
		var health float64
		if err := rows.Scan(&sn, &health); err != nil {
			return err
		}
		var lh *float64
		dims := r.deviceDims(ctx, sn)
		if r.RDB != nil {
			if rep, err := shadow.Read(ctx, r.RDB, sn); err == nil {
				if v, err := strconv.ParseFloat(rep["laser_hours"], 64); err == nil {
					lh = &v
				}
			}
		}
		cfg, ok := cfgCache[dims]
		if !ok {
			cfg = r.correctionCfg(ctx, dims[0], dims[1])
			cfgCache[dims] = cfg
		}
		h := health
		ks = append(ks, Correction(&h, lh, cfg).KPower)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(ks) == 0 {
		slog.Info("reco correction distribution skipped (no consumable_health rows)")
		return nil
	}
	rep.CorrectionDevices = len(ks)
	rep.CorrectionCapShare, rep.CorrectionAlarm = CapShareAlarm(ks, 0.2)
	sorted := append([]float64(nil), ks...)
	sort.Float64s(sorted)
	q := func(p float64) float64 { return sorted[int(p*float64(len(sorted)-1))] }
	slog.Info("reco k_power distribution (last 24h devices)", "n", len(ks),
		"p50", fmt.Sprintf("%.3f", q(0.5)), "p90", fmt.Sprintf("%.3f", q(0.9)), "p99", fmt.Sprintf("%.3f", q(0.99)),
		"cap_share", fmt.Sprintf("%.3f", rep.CorrectionCapShare), "alarm", rep.CorrectionAlarm)
	return nil
}

func (r *Runner) correctionCfg(ctx context.Context, pk, module string) CorrectionCfg {
	cfg := DefaultCorrectionCfg()
	_ = r.DB.QueryRow(ctx,
		`SELECT a::float8, b::float8, c::float8, rated_hours::float8 FROM iot_global.param_correction_cfg WHERE product_key=$1 AND module_model=$2`,
		pk, module).Scan(&cfg.A, &cfg.B, &cfg.C, &cfg.RatedHours)
	return cfg
}
