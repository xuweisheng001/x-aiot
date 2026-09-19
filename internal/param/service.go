package param

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// Service 编排纯函数与 Store。
type Service struct {
	Store       Store
	M           *Metrics
	SnapshotDir string // 本地快照目录（原型代替对象存储 + CDN）
	Now         func() time.Time
}

func NewService(st Store, m *Metrics, snapshotDir string) *Service {
	if m == nil {
		m = NewMetrics()
	}
	if snapshotDir == "" {
		snapshotDir = "./data/params"
	}
	return &Service{Store: st, M: m, SnapshotDir: snapshotDir, Now: time.Now}
}

// AddedReq 是发布请求中的新增档。
type AddedReq struct {
	ModuleModel string          `json:"module_model"`
	MaterialID  string          `json:"material_id"`
	Params      json.RawMessage `json:"params"`
	Source      string          `json:"source,omitempty"`
}

// PublishReq 是 POST /internal/params/releases 的请求体。
type PublishReq struct {
	ProductKey string     `json:"product_key"`
	Note       string     `json:"note"`
	CreatedBy  string     `json:"created_by"`
	ApprovedBy string     `json:"approved_by"`
	RolloutPct int        `json:"rollout_pct,omitempty"`
	Added      []AddedReq `json:"added"`
	Removed    []int64    `json:"removed"`
	Force      bool       `json:"force,omitempty"`
}

// DiffError 携带差异校验违规明细（HTTP 409）。
type DiffError struct{ Violations []Violation }

func (e *DiffError) Error() string {
	return fmt.Sprintf("diff validation rejected: %d violations exceed %d", len(e.Violations), DiffMaxCount)
}

// Snapshot 是全量快照文件内容。
type Snapshot struct {
	ProductKey string    `json:"product_key"`
	Version    int64     `json:"version"`
	ReleasedAt string    `json:"released_at"`
	Profiles   []Profile `json:"profiles"`
}

var identRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,32}$`)

// Publish 发布新版本：校验 → 差异校验 → 合并已发布推荐 → Store.Publish（事务内写快照）。
func (s *Service) Publish(ctx context.Context, req PublishReq) (*Release, error) {
	if !identRe.MatchString(req.ProductKey) || req.CreatedBy == "" {
		return nil, fmt.Errorf("%w: product_key/created_by required", ErrBadParam)
	}
	if req.RolloutPct < 0 || req.RolloutPct > 100 {
		return nil, fmt.Errorf("%w: rollout_pct 0..100", ErrBadParam)
	}
	added := make([]Profile, 0, len(req.Added))
	for _, a := range req.Added {
		if !ValidIdent(a.ModuleModel) || !ValidIdent(a.MaterialID) || len(a.Params) == 0 {
			return nil, fmt.Errorf("%w: added item needs module_model/material_id/params", ErrBadParam)
		}
		src := a.Source
		if src == "" {
			src = SourceOfficial
		}
		if src != SourceOfficial && src != SourceRecommended {
			return nil, fmt.Errorf("%w: source", ErrBadParam)
		}
		added = append(added, Profile{ProductKey: req.ProductKey, ModuleModel: a.ModuleModel, MaterialID: a.MaterialID, Params: a.Params, Source: src})
	}

	all, err := s.Store.Profiles(ctx, req.ProductKey)
	if err != nil {
		return nil, err
	}
	cur := int64(0)
	for _, p := range all {
		if p.VersionAdded > cur {
			cur = p.VersionAdded
		}
		if p.VersionRemoved != nil && *p.VersionRemoved > cur {
			cur = *p.VersionRemoved
		}
	}
	old := EffectiveAt(all, cur)
	removed := map[int64]bool{}
	for _, id := range req.Removed {
		removed[id] = true
	}
	next := make([]Profile, 0, len(old)+len(added))
	for _, p := range old {
		if !removed[p.ID] {
			next = append(next, p)
		}
	}
	next = append(next, added...)

	// 发布前差异校验（INC-4-13）
	violations := DiffValidate(old, next, DiffPctThreshold, DiffMaxCount)
	if DiffRejected(violations, DiffMaxCount) {
		if !req.Force {
			s.M.Inc(MDiffRejected)
			return nil, &DiffError{Violations: violations}
		}
		if req.ApprovedBy == "" {
			return nil, fmt.Errorf("%w: force requires approved_by", ErrDenied)
		}
		s.M.Inc(MDiffForced)
		slog.Warn("param publish forced past diff validation", "product_key", req.ProductKey, "violations", len(violations), "approved_by", req.ApprovedBy)
	}

	// 合并已发布的推荐候选（reco-job 产出）
	recs, err := s.Store.PublishedRecommendations(ctx, req.ProductKey)
	if err != nil {
		return nil, err
	}
	existing := map[string]bool{}
	for _, p := range next {
		if p.Source == SourceRecommended {
			existing[p.ModuleModel+"|"+p.MaterialID+"|"+CanonicalHash(p.Params)] = true
		}
	}
	recoAdded := 0
	for _, r := range recs {
		key := r.ModuleModel + "|" + r.MaterialID + "|" + r.ParamsHash
		if existing[key] || len(r.Params) == 0 {
			continue
		}
		conf := r.GoodRatio
		added = append(added, Profile{ProductKey: req.ProductKey, ModuleModel: r.ModuleModel, MaterialID: r.MaterialID, Params: r.Params,
			Source: SourceRecommended, SampleCount: r.SampleCount, Confidence: &conf})
		existing[key] = true
		recoAdded++
	}

	rel, err := s.Store.Publish(ctx, PublishPlan{ProductKey: req.ProductKey, Note: req.Note, CreatedBy: req.CreatedBy, ApprovedBy: req.ApprovedBy,
		RolloutPct: req.RolloutPct, Added: added, RemoveIDs: req.Removed}, s.snapshotFor(req.ProductKey))
	if err != nil {
		return nil, err
	}
	s.M.Inc(MPublished)
	s.M.Add(MRecoPublished, int64(recoAdded))
	slog.Info("param release published", "product_key", rel.ProductKey, "version", rel.Version, "added", len(added), "removed", len(req.Removed), "rollout_pct", rel.RolloutPct)
	return rel, nil
}

// RollbackReq 是回滚请求体。
type RollbackReq struct {
	ToVersion  int64  `json:"to_version"`
	CreatedBy  string `json:"created_by"`
	ApprovedBy string `json:"approved_by"`
	Note       string `json:"note,omitempty"`
}

// Rollback 发布一个新 version，其有效集合等于 to_version 的有效集合。
func (s *Service) Rollback(ctx context.Context, productKey string, req RollbackReq) (*Release, error) {
	if !identRe.MatchString(productKey) || req.CreatedBy == "" || req.ToVersion <= 0 {
		return nil, fmt.Errorf("%w: product_key/created_by/to_version required", ErrBadParam)
	}
	rels, err := s.Store.Releases(ctx, productKey)
	if err != nil {
		return nil, err
	}
	var latest int64
	found := false
	for _, r := range rels {
		if r.Version > latest {
			latest = r.Version
		}
		if r.Version == req.ToVersion {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("%w: version %d of %s", ErrNotFound, req.ToVersion, productKey)
	}
	all, err := s.Store.Profiles(ctx, productKey)
	if err != nil {
		return nil, err
	}
	toRemove, toReadd := RollbackPlan(EffectiveAt(all, latest), EffectiveAt(all, req.ToVersion))
	note := req.Note
	if note == "" {
		note = fmt.Sprintf("rollback to v%d", req.ToVersion)
	}
	to := req.ToVersion
	rel, err := s.Store.Publish(ctx, PublishPlan{ProductKey: productKey, Note: note, CreatedBy: req.CreatedBy, ApprovedBy: req.ApprovedBy,
		RolloutPct: 100, Added: toReadd, RemoveIDs: toRemove, RolledBackFrom: &to}, s.snapshotFor(productKey))
	if err != nil {
		return nil, err
	}
	s.M.Inc(MRollbacks)
	slog.Warn("param release rolled back", "product_key", productKey, "new_version", rel.Version, "to_version", to, "removed", len(toRemove), "readded", len(toReadd))
	return rel, nil
}

// snapshotFor 返回把有效集合写到 SnapshotDir/{pk}/v{version}.json 的 SnapshotFn：
// 先写临时文件再 rename；文件名含版本永不复用，对应 CDN immutable 缓存策略（INC-4-15）。
func (s *Service) snapshotFor(pk string) SnapshotFn {
	return func(version int64, releasedAt time.Time, profiles []Profile) (string, error) {
		if profiles == nil {
			profiles = []Profile{}
		}
		dir := filepath.Join(s.SnapshotDir, pk)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", err
		}
		name := fmt.Sprintf("v%d.json", version)
		b, err := json.Marshal(Snapshot{ProductKey: pk, Version: version, ReleasedAt: releasedAt.UTC().Format(time.RFC3339), Profiles: profiles})
		if err != nil {
			return "", err
		}
		tmp := filepath.Join(dir, name+".tmp")
		if err := os.WriteFile(tmp, b, 0o644); err != nil {
			return "", err
		}
		if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
			return "", err
		}
		return "/snapshots/" + pk + "/" + name, nil
	}
}

// LatestResp 是 GET /params/releases/latest 的响应。
type LatestResp struct {
	Version     int64  `json:"version"`
	SnapshotURL string `json:"snapshot_url"`
	ReleasedAt  string `json:"released_at"`
	RolloutPct  int    `json:"rollout_pct"`
}

// Latest 返回该 bucket 可见的最高版本。
func (s *Service) Latest(ctx context.Context, productKey string, bucket float64, hasBucket bool) (*LatestResp, error) {
	rels, err := s.Store.Releases(ctx, productKey)
	if err != nil {
		return nil, err
	}
	r, ok := VisibleVersion(rels, bucket, hasBucket)
	if !ok {
		return nil, fmt.Errorf("%w: no visible release for %s", ErrNotFound, productKey)
	}
	return &LatestResp{Version: r.Version, SnapshotURL: r.SnapshotURL, ReleasedAt: r.ReleasedAt, RolloutPct: r.RolloutPct}, nil
}

// DeltaResp 是增量响应：字段顺序即客户端应用顺序——先 removed 再 added。
type DeltaResp struct {
	Version     int64     `json:"version"`
	ForceFull   bool      `json:"force_full,omitempty"`
	SnapshotURL string    `json:"snapshot_url,omitempty"`
	Removed     []int64   `json:"removed"`
	Added       []Profile `json:"added"`
}

// Delta 计算 since → 可见最新版本 的增量；落后超过 ForceFullBehind 返回 force_full。
func (s *Service) Delta(ctx context.Context, productKey string, since int64, bucket float64, hasBucket bool) (*DeltaResp, error) {
	latest, err := s.Latest(ctx, productKey, bucket, hasBucket)
	if err != nil {
		return nil, err
	}
	if since < 0 {
		since = 0
	}
	if since > latest.Version {
		return nil, fmt.Errorf("%w: since_version %d ahead of visible %d", ErrBadParam, since, latest.Version)
	}
	if latest.Version-since > ForceFullBehind {
		s.M.Inc(MFullServed)
		return &DeltaResp{Version: latest.Version, ForceFull: true, SnapshotURL: latest.SnapshotURL, Removed: []int64{}, Added: []Profile{}}, nil
	}
	all, err := s.Store.Profiles(ctx, productKey)
	if err != nil {
		return nil, err
	}
	removed, added := Delta(all, since, latest.Version)
	s.M.Inc(MDeltaServed)
	return &DeltaResp{Version: latest.Version, Removed: removed, Added: added}, nil
}

// PutUserParam 写自定义参数（乐观锁）。conflict=true 时 current 为库内版本。
func (s *Service) PutUserParam(ctx context.Context, up UserParam, ifMatch int64) (version int64, current int64, conflict bool, err error) {
	if !ValidIdent(up.ProductKey) || !ValidIdent(up.ModuleModel) || !ValidIdent(up.MaterialID) || len(up.Params) == 0 || !json.Valid(up.Params) {
		return 0, 0, false, fmt.Errorf("%w: product_key/module_model/material_id/params", ErrBadParam)
	}
	version, current, conflict, err = s.Store.PutUserParam(ctx, up, ifMatch)
	if err != nil {
		return
	}
	if conflict {
		s.M.Inc(MUserParamRefused)
	} else {
		s.M.Inc(MUserParamPut)
	}
	return
}

// Correct 读取配置并计算校正系数。
func (s *Service) Correct(ctx context.Context, productKey, moduleModel string, laserHours, health float64, hoursKnown, healthKnown bool) (Result, error) {
	cfg := DefaultCfg
	if productKey != "" && moduleModel != "" {
		c, ok, err := s.Store.CorrectionCfg(ctx, productKey, moduleModel)
		if err != nil {
			return Result{}, err
		}
		if ok {
			cfg = c
		}
	}
	res := Correction(cfg, laserHours, health, healthKnown, hoursKnown)
	if !healthKnown || !hoursKnown {
		s.M.Inc(MCorrectionMiss)
	} else {
		s.M.Inc(MCorrection)
	}
	return res, nil
}

// ReadSnapshot 读本地快照文件（GET /snapshots/{pk}/v{n}.json），路径分量白名单防穿越。
func (s *Service) ReadSnapshot(productKey, file string) ([]byte, error) {
	if !identRe.MatchString(productKey) || !snapshotFileRe.MatchString(file) {
		return nil, ErrNotFound
	}
	b, err := os.ReadFile(filepath.Join(s.SnapshotDir, productKey, file))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return b, err
}

var snapshotFileRe = regexp.MustCompile(`^v\d{1,12}\.json$`)
