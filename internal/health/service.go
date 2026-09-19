package health

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// NotifyKind 是 JetStream 通知 subject 的 kind：iot.notify.health（IOT_NOTIFY 流，与 alarm-svc 无共享路径）。
const NotifyKind = "health"

// Options 是服务配置（cmd/health-svc 从环境变量装配）。
type Options struct {
	Interval    time.Duration // IOT_HEALTH_INTERVAL 1h
	Cooldown    time.Duration // IOT_HEALTH_COOLDOWN 168h
	StaleAfter  time.Duration // IOT_HEALTH_STALE_AFTER 2h
	ScanWindow  time.Duration // IOT_HEALTH_SCAN_WINDOW 720h（也是 OVER_TEMP 与 rate_30d 窗口）
	FuseRatio   float64       // IOT_HEALTH_FUSE_RATIO 0.3
	MaxScans    int           // IOT_MATERIAL_MAX_SCANS 1（材料码一次性）
	MaterialKey []byte        // IOT_MATERIAL_KEY 主密钥；按 batch 派生
}

// DefaultOptions 与 spec §6 缺省值一致。
func DefaultOptions() Options {
	return Options{Interval: time.Hour, Cooldown: DefaultCooldown, StaleAfter: 2 * time.Hour, ScanWindow: 720 * time.Hour, FuseRatio: 0.3, MaxScans: 1}
}

// Service 编排纯函数与 Store / Source / Shadow / Notifier。
type Service struct {
	Store  Store
	Source Source
	Shadow ShadowReader
	Notify Notifier // nil → 只打日志
	M      *Metrics
	Opt    Options
	Now    func() time.Time

	runMu sync.Mutex // 批与提醒发送串行化（INC-3-09）
}

func NewService(st Store, src Source, sh ShadowReader, n Notifier, m *Metrics, opt Options) *Service {
	if m == nil {
		m = NewMetrics()
	}
	def := DefaultOptions()
	if opt.Interval <= 0 {
		opt.Interval = def.Interval
	}
	if opt.Cooldown <= 0 {
		opt.Cooldown = def.Cooldown
	}
	if opt.StaleAfter <= 0 {
		opt.StaleAfter = def.StaleAfter
	}
	if opt.ScanWindow <= 0 {
		opt.ScanWindow = def.ScanWindow
	}
	if opt.FuseRatio <= 0 {
		opt.FuseRatio = def.FuseRatio
	}
	return &Service{Store: st, Source: src, Shadow: sh, Notify: n, M: m, Opt: opt, Now: time.Now}
}

// ===================== 批计算 =====================

// RunReport 是一轮批计算的结果摘要。
type RunReport struct {
	Total     int            `json:"total"`
	Scored    int            `json:"scored"`
	Skipped   int            `json:"skipped"`
	Unchanged int            `json:"unchanged"`
	Fused     bool           `json:"fused"`
	Reminders int            `json:"reminders"`
	Skips     map[string]int `json:"skips,omitempty"`
}

// pending 是一台设备算好但未写库的结果。
type pending struct {
	up         HealthUpdate
	prevHealth float64 // 首次 / 更换 → 101
	rep        Reported
	productKey string
	cfg        Cfg
}

// Run 按 Interval 循环执行 RunOnce，直到 ctx 取消（启动即跑一轮）。
func (s *Service) Run(ctx context.Context) {
	t := time.NewTicker(s.Opt.Interval)
	defer t.Stop()
	for {
		if _, err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("health batch failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RunOnce 执行一轮：读窗口内小时桶与 OVER_TEMP → 逐 SN 算分（缺失 skip 不写 0）→ 缺失率熔断 → 写库 → 提醒。
// only 非空时仅处理这些 SN（recompute 接口 / 测试用）。
func (s *Service) RunOnce(ctx context.Context, only ...string) (RunReport, error) {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	rep := RunReport{Skips: map[string]int{}}
	now := s.Now()
	since := now.Add(-s.Opt.ScanWindow)

	buckets, err := s.Source.Buckets(ctx, since)
	if err != nil {
		s.M.Inc(MBatchErrors)
		return rep, err
	}
	over, err := s.Source.OvertempCounts(ctx, since)
	if err != nil {
		s.M.Inc(MBatchErrors)
		return rep, err
	}
	filter := map[string]bool{}
	for _, sn := range only {
		filter[sn] = true
	}
	bySN := map[string][]Bucket{}
	var sns []string
	for _, b := range buckets {
		if len(filter) > 0 && !filter[b.SN] {
			continue
		}
		if _, ok := bySN[b.SN]; !ok {
			sns = append(sns, b.SN)
		}
		bySN[b.SN] = append(bySN[b.SN], b)
	}
	sort.Strings(sns)

	var writes []pending
	for _, sn := range sns {
		p, skip, err := s.scoreDevice(ctx, sn, bySN[sn], over[sn], now)
		if err != nil {
			s.M.Inc(MBatchErrors)
			return rep, fmt.Errorf("score %s: %w", sn, err)
		}
		switch {
		case skip != "":
			rep.Skipped++
			rep.Skips[skip]++
			s.M.Inc(MSkippedMissing)
			slog.Debug("health skip", "sn", sn, "reason", skip)
		case p == nil:
			rep.Unchanged++
		default:
			writes = append(writes, *p)
		}
	}
	rep.Total = rep.Skipped + len(writes)
	if ShouldFuseBatch(rep.Skipped, rep.Total, s.Opt.FuseRatio) {
		rep.Fused = true
		s.M.Inc(MBatchFused)
		s.M.Inc(MBatchRuns)
		slog.Warn("health batch fused: skipped ratio over threshold, nothing written", "skipped", rep.Skipped, "total", rep.Total, "ratio", s.Opt.FuseRatio, "skips", rep.Skips)
		return rep, nil
	}

	for _, p := range writes {
		sent, err := s.commit(ctx, p, now)
		if err != nil {
			s.M.Inc(MBatchErrors)
			slog.Error("health write failed", "sn", p.up.Row.SN, "err", err)
			continue
		}
		rep.Scored++
		s.M.Inc(MDevicesScored)
		if sent {
			rep.Reminders++
		}
	}
	s.M.Inc(MBatchRuns)
	slog.Info("health batch done", "total", rep.Total, "scored", rep.Scored, "skipped", rep.Skipped, "unchanged", rep.Unchanged, "reminders", rep.Reminders, "skips", rep.Skips)
	return rep, nil
}

// scoreDevice 返回 (pending, skipReason, err)：pending==nil 且 skip=="" 表示无新桶（unchanged）。
func (s *Service) scoreDevice(ctx context.Context, sn string, bs []Bucket, overtemp int, now time.Time) (*pending, string, error) {
	prevRow, err := s.Store.HealthRow(ctx, sn, PartModule)
	if err != nil {
		return nil, "", err
	}
	explain, err := s.Store.Explain(ctx, sn, PartModule)
	if err != nil {
		return nil, "", err
	}
	// 新桶：严格大于 last_bucket_ts（重跑同一小时幂等）
	var newBs []Bucket
	for _, b := range bs {
		if explain == nil || explain.LastBucketTs == nil || b.Ts.After(*explain.LastBucketTs) {
			newBs = append(newBs, b)
		}
	}
	if len(newBs) == 0 {
		return nil, "", nil
	}
	rep, err := s.Shadow.Reported(ctx, sn)
	if err != nil {
		return nil, "shadow_error", nil
	}
	module := rep.ModuleModel
	if module == "" && explain != nil {
		module = explain.ModuleModel
	}
	if module == "" {
		return nil, "missing_model", nil
	}
	productKey := bs[0].ProductKey
	if productKey == "" {
		return nil, "missing_cfg", nil
	}
	cfg, ok, err := s.Store.Cfg(ctx, productKey, module)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "missing_cfg", nil
	}

	// laser_hours 历史：上次记录值 + 新桶
	var hist []float64
	var prevHours *float64
	if explain != nil && explain.LastHours != nil {
		v := *explain.LastHours
		hist = append(hist, v)
		prevHours = &v
	}
	valid := 0
	for _, b := range newBs {
		if b.LaserHours != nil {
			hist = append(hist, *b.LaserHours)
			valid++
		}
	}
	if valid == 0 {
		return nil, "missing_hours", nil
	}
	prevModel := ""
	if explain != nil {
		prevModel = explain.ModuleModel
	}
	swapped, reason := DetectSwap(prevModel, module, hist, nil, nil, now)

	used := 0.0
	if explain != nil && !swapped {
		used = explain.UsedWeighted
	}
	if swapped {
		prevHours = nil
		s.M.Inc(MSwaps)
	}
	var lo, mid, hi float64
	if explain != nil {
		lo, mid, hi = explain.ShareLow, explain.ShareMid, explain.ShareHigh
	}
	var lastTs time.Time
	for _, b := range newBs {
		lastTs = b.Ts
		if b.LaserHours == nil {
			continue
		}
		h := *b.LaserHours
		if prevHours == nil {
			prevHours = &h
			continue
		}
		bl, bm, bh := bucketShares(b)
		w, flag := WeightedHours(h-*prevHours, bl, bm, bh, cfg)
		switch flag {
		case FlagRegress:
			s.M.Inc(MHoursRegress)
		case FlagClipped:
			s.M.Inc(MHoursClipped)
		}
		used += w
		lo, mid, hi = bl, bm, bh
		prevHours = &h
	}

	rate := windowRate(bs, cfg)
	score, eol, ok := ComputeHealth(Inputs{UsedWeighted: used, HasUsed: true, OvertempCount: overtemp, Cfg: cfg, Rate30d: rate, Now: now})
	if !ok {
		return nil, "missing_cfg", nil
	}
	_, penalty := Health(used, cfg.Rated, overtemp, cfg)
	prevHealth := -1.0
	if prevRow != nil && !swapped {
		prevHealth = prevRow.Health
	}
	value, kept := Monotonic(prevHealth, score, swapped)
	if kept {
		s.M.Inc(MHealthNonmono)
		eol = PredictEOL(value, cfg.Rated, rate, now)
	}

	p := &pending{rep: rep, productKey: productKey, cfg: cfg, prevHealth: 101}
	if prevHealth >= 0 {
		p.prevHealth = prevHealth
	}
	var levels []int
	if explain != nil && !swapped {
		levels = explain.LevelsSent
	}
	p.up = HealthUpdate{
		Row: HealthRow{SN: sn, Part: PartModule, Health: value, UsedHours: used, PredictedEOLAt: eol, UpdatedAt: now},
		Explain: Explain{SN: sn, Part: PartModule, ModelVersion: cfg.Version, UsedWeighted: used, Rated: cfg.Rated,
			ShareLow: lo, ShareMid: mid, ShareHigh: hi, OvertempCount: overtemp, Penalty: penalty,
			LastBucketTs: &lastTs, LastHours: prevHours, ModuleModel: module, LevelsSent: levels, ComputedAt: now},
	}
	if swapped {
		var eolBefore *time.Time
		if prevRow != nil {
			eolBefore = prevRow.PredictedEOLAt
		}
		p.up.Swap = &Swap{FromModel: prevModel, ToModel: module, Reason: reason, PredictedEOLBefore: eolBefore}
	}
	return p, "", nil
}

// bucketShares：share_* 三者有值用之；否则用 avg_power 单档兜底（老流 / 直接聚合缺列）。
func bucketShares(b Bucket) (lo, mid, hi float64) {
	if b.ShareLow != nil || b.ShareMid != nil || b.ShareHigh != nil {
		lo, mid, hi = deref(b.ShareLow), deref(b.ShareMid), deref(b.ShareHigh)
		if lo+mid+hi > 0 {
			return lo, mid, hi
		}
	}
	if b.AvgPower != nil {
		return ShareFromAvgPower(*b.AvgPower)
	}
	return 0, 0, 0
}

func deref(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

// windowRate 对窗口内全部桶重算 Σweighted / 天数 → rate_30d（不足 7 天不预测）。
func windowRate(bs []Bucket, cfg Cfg) float64 {
	var prev *float64
	sum := 0.0
	var first, last time.Time
	for _, b := range bs {
		if b.LaserHours == nil {
			continue
		}
		if first.IsZero() {
			first = b.Ts
		}
		last = b.Ts
		h := *b.LaserHours
		if prev != nil {
			lo, mid, hi := bucketShares(b)
			w, _ := WeightedHours(h-*prev, lo, mid, hi, cfg)
			sum += w
		}
		prev = &h
	}
	if first.IsZero() {
		return 0
	}
	days := (last.Sub(first) + time.Hour).Hours() / 24
	return Rate30d(sum, days)
}

// commit 写一台设备并处理提醒（判定 → 写 levels_sent → 写行 → 发布）。返回是否发出提醒。
func (s *Service) commit(ctx context.Context, p pending, now time.Time) (bool, error) {
	lastSent, err := s.Store.LastSentAt(ctx, p.up.Row.SN, PartModule)
	if err != nil {
		return false, err
	}
	dec := DecideReminder(ReminderInput{HealthPrev: p.prevHealth, HealthNow: p.up.Row.Health, LevelsSent: p.up.Explain.LevelsSent,
		LastSentAt: lastSent, WorkState: p.rep.WorkState, Optout: p.rep.Optout, Now: now, Cooldown: s.Opt.Cooldown})
	if dec.Action == ActionSend {
		p.up.Explain.LevelsSent = append(append([]int(nil), p.up.Explain.LevelsSent...), dec.Level)
	}
	if err := s.Store.WriteHealth(ctx, p.up); err != nil {
		return false, err
	}
	if dec.Action == ActionNone {
		return false, nil
	}
	r := &Reminder{SN: p.up.Row.SN, Part: PartModule, Level: strconv.Itoa(dec.Level), HealthAt: p.up.Row.Health}
	switch dec.Action {
	case ActionSend:
		r.SentAt = &now
		if sku := s.pickSKU(ctx, p.productKey, PartModule, p.up.Explain.ModuleModel); sku != nil {
			r.SkuID = sku.SkuID
		}
	case ActionSuppressed:
		r.SuppressedReason = dec.Reason
	case ActionDeferred:
		r.SuppressedReason = "deferred"
	}
	if err := s.Store.InsertReminder(ctx, r); err != nil {
		return false, err
	}
	switch dec.Action {
	case ActionSend:
		s.M.Inc(MRemindersSent)
		s.publishReminder(ctx, r, p.up.Row.PredictedEOLAt)
		return true, nil
	case ActionSuppressed:
		if dec.Reason == ReasonCooldown {
			s.M.Inc(MRemindersCooled)
		} else {
			s.M.Inc(MRemindersOptout)
		}
	case ActionDeferred:
		s.M.Inc(MRemindersDeferred)
	}
	return false, nil
}

// pickSKU：精确 module_model 优先，其次 '*'；只取可售。
func (s *Service) pickSKU(ctx context.Context, productKey, part, module string) *SKU {
	skus, err := s.Store.SKUs(ctx, productKey, part)
	if err != nil {
		slog.Warn("sku lookup failed", "err", err)
		return nil
	}
	return PickSKU(skus, module)
}

// PickSKU 纯函数：module 精确匹配 > '*'，仅 sellable。
func PickSKU(skus []SKU, module string) *SKU {
	var wildcard *SKU
	for i := range skus {
		k := &skus[i]
		if !k.Sellable {
			continue
		}
		if k.ModuleModel == module {
			return k
		}
		if k.ModuleModel == "*" && wildcard == nil {
			wildcard = k
		}
	}
	return wildcard
}

// Notification 是发布到 iot.notify.health 的 JSON 载荷（不含 user_id）。
type Notification struct {
	Type           string     `json:"type"`
	ReminderID     int64      `json:"reminder_id"`
	SN             string     `json:"sn"`
	Part           string     `json:"part"`
	Level          string     `json:"level"`
	Health         float64    `json:"health"`
	PredictedEOLAt *time.Time `json:"predicted_eol_at"`
	SkuID          string     `json:"sku_id,omitempty"`
	SentAt         time.Time  `json:"sent_at"`
}

func (s *Service) publishReminder(ctx context.Context, r *Reminder, eol *time.Time) {
	n := Notification{Type: "consumable", ReminderID: r.ID, SN: r.SN, Part: r.Part, Level: r.Level, Health: r.HealthAt, PredictedEOLAt: eol, SkuID: r.SkuID, SentAt: *r.SentAt}
	b, _ := json.Marshal(n)
	slog.Info("consumable reminder", "sn", r.SN, "level", r.Level, "health", r.HealthAt, "sku_id", r.SkuID, "reminder_id", r.ID)
	if s.Notify == nil {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.Notify.Publish(pctx, envelope.SubjectNotify(NotifyKind), b); err != nil {
		s.M.Inc(MNotifyErr)
		slog.Warn("notify publish failed", "reminder_id", r.ID, "err", err)
	}
}

// ===================== 读接口 =====================

// HealthResp 是 GET /api/v1/health/{sn} 的响应。
type HealthResp struct {
	HealthRow
	Stale        bool     `json:"stale"`
	ModelVersion int      `json:"model_version,omitempty"`
	ModuleModel  string   `json:"module_model,omitempty"`
	Explain      *Explain `json:"explain,omitempty"`
}

// GetHealth 返回 consumable_health(sn,'module') 行；updated_at 超 StaleAfter → stale=true（由 DB 字段计算，INC-3-17）。
func (s *Service) GetHealth(ctx context.Context, sn string) (*HealthResp, error) {
	row, err := s.Store.HealthRow(ctx, sn, PartModule)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: health %s", ErrNotFound, sn)
	}
	resp := &HealthResp{HealthRow: *row, Stale: IsStale(row.UpdatedAt, s.Now(), s.Opt.StaleAfter)}
	if ex, err := s.Store.Explain(ctx, sn, PartModule); err == nil && ex != nil {
		resp.ModelVersion, resp.ModuleModel, resp.Explain = ex.ModelVersion, ex.ModuleModel, ex
	}
	return resp, nil
}

// SKUs 查 sku_mapping；命中计数 sku_hits。
func (s *Service) SKUs(ctx context.Context, productKey, part string) ([]SKU, error) {
	if productKey == "" {
		return nil, fmt.Errorf("%w: product_key required", ErrBadParam)
	}
	list, err := s.Store.SKUs(ctx, productKey, part)
	if err != nil {
		return nil, err
	}
	if len(list) > 0 {
		s.M.Inc(MSkuHits)
	}
	if list == nil {
		list = []SKU{}
	}
	return list, nil
}

// Click 记点击归因（user_id 只进 reminder_attribution，不与 SN 并存）。
func (s *Service) Click(ctx context.Context, reminderID, userID int64) error {
	r, err := s.Store.Reminder(ctx, reminderID)
	if err != nil {
		return err
	}
	if err := s.Store.Click(ctx, reminderID, userID, r.SkuID, s.Now()); err != nil {
		return err
	}
	s.M.Inc(MClicks)
	return nil
}

// AttributeReq 是 POST /internal/orders/attribute 的请求体。
type AttributeReq struct {
	ReminderID int64  `json:"reminder_id"`
	OrderNo    string `json:"order_no"`
}

// Attribute 下单归因：提醒须已发出且在 30 天窗口内；同 reminder 只归因一次（幂等重放同 order_no 成功）。
func (s *Service) Attribute(ctx context.Context, req AttributeReq) error {
	if req.ReminderID <= 0 || req.OrderNo == "" || len(req.OrderNo) > 64 {
		return fmt.Errorf("%w: reminder_id/order_no required", ErrBadParam)
	}
	r, err := s.Store.Reminder(ctx, req.ReminderID)
	if err != nil {
		return err
	}
	now := s.Now()
	if r.SentAt == nil || !AttributionWindowOK(*r.SentAt, now, AttributionWindow) {
		s.M.Inc(MAttributionReject)
		return fmt.Errorf("%w: reminder %d not sent or outside attribution window", ErrDenied, req.ReminderID)
	}
	if err := s.Store.Attribute(ctx, req.ReminderID, req.OrderNo, r.SkuID, now); err != nil {
		if errors.Is(err, ErrConflict) {
			s.M.Inc(MAttributionReject)
		}
		return err
	}
	s.M.Inc(MAttributed)
	return nil
}

// ===================== 材料码（§9.1） =====================

// 材料码格式：base32(material_idx 4B | batch 3B | seq 4B | sig 8B) = 19 字节 → 31 字符；sig = HMAC-SHA256(k_batch, 前 11 字节)[:8]。
const (
	CodeLen     = 31
	codeBodyLen = 11
	codeSigLen  = 8
)

var codeEnc = base32.StdEncoding.WithPadding(base32.NoPadding)

// ErrBadCode：格式 / 签名不合法。
var ErrBadCode = errors.New("bad material code")

// Code 是解码后的字段。
type Code struct {
	MaterialIdx uint32
	Batch       uint32 // 24 bit
	Seq         uint32
}

// DeriveBatchKey 纯函数：k_batch = HMAC-SHA256(master, "matcode:batch:"+batch)（可按批次吊销）。
func DeriveBatchKey(master []byte, batch uint32) []byte {
	m := hmac.New(sha256.New, master)
	m.Write([]byte("matcode:batch:" + strconv.FormatUint(uint64(batch), 10)))
	return m.Sum(nil)
}

func codeBody(materialIdx, batch, seq uint32) []byte {
	b := make([]byte, codeBodyLen)
	binary.BigEndian.PutUint32(b[0:4], materialIdx)
	b[4], b[5], b[6] = byte(batch>>16), byte(batch>>8), byte(batch)
	binary.BigEndian.PutUint32(b[7:11], seq)
	return b
}

func codeSig(body, key []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(body)
	return m.Sum(nil)[:codeSigLen]
}

// EncodeCode 纯函数：生成 31 字符材料码；key 为该 batch 的派生密钥。batch 只取低 24 位。
func EncodeCode(materialIdx, batch, seq uint32, key []byte) string {
	body := codeBody(materialIdx, batch&0xFFFFFF, seq)
	return codeEnc.EncodeToString(append(body, codeSig(body, key)...))
}

// DecodeVerify 纯函数：长度 / base32 / 签名（常量时间比较）校验，任一失败 → ErrBadCode。
func DecodeVerify(code string, keyFor func(batch uint32) []byte) (Code, error) {
	if len(code) != CodeLen {
		return Code{}, fmt.Errorf("%w: length %d", ErrBadCode, len(code))
	}
	raw, err := codeEnc.DecodeString(code)
	if err != nil || len(raw) != codeBodyLen+codeSigLen {
		return Code{}, fmt.Errorf("%w: base32", ErrBadCode)
	}
	c := Code{MaterialIdx: binary.BigEndian.Uint32(raw[0:4]),
		Batch: uint32(raw[4])<<16 | uint32(raw[5])<<8 | uint32(raw[6]),
		Seq:   binary.BigEndian.Uint32(raw[7:11])}
	want := codeSig(raw[:codeBodyLen], keyFor(c.Batch))
	if subtle.ConstantTimeCompare(want, raw[codeBodyLen:]) != 1 {
		return Code{}, fmt.Errorf("%w: signature", ErrBadCode)
	}
	return c, nil
}

func (s *Service) keyFor(batch uint32) []byte { return DeriveBatchKey(s.Opt.MaterialKey, batch) }

// VerifyResult：valid=false 时不带任何参数字段（material_id / batch / thickness_mm 全 omitempty）。
type VerifyResult struct {
	Valid       bool     `json:"valid"`
	Reason      string   `json:"reason,omitempty"`
	MaterialID  string   `json:"material_id,omitempty"`
	Batch       string   `json:"batch,omitempty"`
	ThicknessMM *float64 `json:"thickness_mm,omitempty"`
	Status      string   `json:"status,omitempty"`
}

// Verify 校验材料码：签名 → 事务内状态 / 一次性 / 多账号阈值 → 记扫码。
func (s *Service) Verify(ctx context.Context, code, sn string, userID *int64) (VerifyResult, error) {
	c, err := DecodeVerify(code, s.keyFor)
	if err != nil {
		s.M.Inc(MVerifyBad)
		return VerifyResult{Valid: false, Reason: "bad_signature"}, nil
	}
	now := s.Now()
	ok, reason, err := s.Store.ConsumeScan(ctx, code, userID, sn, s.Opt.MaxScans, now)
	if err != nil {
		return VerifyResult{}, err
	}
	if !ok {
		s.M.Inc(MVerifyBad)
		return VerifyResult{Valid: false, Reason: reason}, nil
	}
	mc, err := s.Store.MaterialCode(ctx, code)
	if err != nil {
		return VerifyResult{}, err
	}
	mi, err := s.Store.MaterialByIdx(ctx, c.MaterialIdx)
	if err != nil || mi.MaterialID != mc.MaterialID {
		s.M.Inc(MVerifyBad)
		return VerifyResult{Valid: false, Reason: "mismatch"}, nil
	}
	s.M.Inc(MVerifyOK)
	return VerifyResult{Valid: true, MaterialID: mc.MaterialID, Batch: mc.Batch, ThicknessMM: mi.ThicknessMM, Status: mc.Status}, nil
}

// MintCode 供应链登记：生成并写入 material_code（原型内部接口 / 测试用）。
func (s *Service) MintCode(ctx context.Context, materialIdx, batch, seq uint32) (string, error) {
	if batch > 0xFFFFFF {
		return "", fmt.Errorf("%w: batch exceeds 24 bit", ErrBadParam)
	}
	mi, err := s.Store.MaterialByIdx(ctx, materialIdx)
	if err != nil {
		return "", err
	}
	code := EncodeCode(materialIdx, batch, seq, s.keyFor(batch))
	if err := s.Store.InsertMaterialCode(ctx, MaterialCode{CodeID: code, MaterialID: mi.MaterialID, Batch: strconv.FormatUint(uint64(batch), 10), Seq: int64(seq), Status: "active"}); err != nil {
		return "", err
	}
	return code, nil
}
