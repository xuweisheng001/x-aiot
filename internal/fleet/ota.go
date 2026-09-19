package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// ---- 组织批量 OTA（技术方案 BL5 §08 / §17.1「批量 OTA 适配」） ----
//
// 组织只决定「哪些设备、什么时候」：固件档位、审批、熔断阈值全在 ota-svc 的父批次上，
// fleet 只做三件事——校验设备确实属于本组织、校验父批次够格、把维护窗口透传下去。

// OTAWindow 是维护窗口的线上契约（与 ota-svc internal/ota.Window 同形）。
// 这里不 import internal/ota：服务之间只通过 JSON 契约耦合（仓库惯例见 internal/support/grantcheck）。
type OTAWindow struct {
	Start    string `json:"start"`
	End      string `json:"end"`
	TZ       string `json:"tz,omitempty"`
	Weekdays []int  `json:"weekdays,omitempty"`
}

// Empty 报告窗口为零值（不限制下发时间）。
func (w OTAWindow) Empty() bool {
	return strings.TrimSpace(w.Start) == "" && strings.TrimSpace(w.End) == ""
}

// ValidateWindow 纯函数：start/end 要么都空要么都是 "HH:MM"，weekdays ∈ [0,6]（0=周日），tz 可解析。
func ValidateWindow(w OTAWindow) error {
	start, end := strings.TrimSpace(w.Start), strings.TrimSpace(w.End)
	if (start == "") != (end == "") {
		return fmt.Errorf("window: start and end must both be set")
	}
	if start != "" {
		if !validHHMM(start) {
			return fmt.Errorf("window: bad start %q (want HH:MM)", start)
		}
		if !validHHMM(end) {
			return fmt.Errorf("window: bad end %q (want HH:MM)", end)
		}
	}
	for _, d := range w.Weekdays {
		if d < 0 || d > 6 {
			return fmt.Errorf("window: weekday %d out of range 0..6", d)
		}
	}
	if tz := strings.TrimSpace(w.TZ); tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return fmt.Errorf("window: bad tz %q", tz)
		}
	}
	return nil
}

func validHHMM(s string) bool {
	if len(s) != 5 || s[2] != ':' {
		return false
	}
	h, err1 := strconv.Atoi(s[0:2])
	m, err2 := strconv.Atoi(s[3:5])
	for _, i := range []int{0, 1, 3, 4} {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return err1 == nil && err2 == nil && h >= 0 && h <= 23 && m >= 0 && m <= 59
}

// MinOrgStagePct 是组织能建子批次的最低父档位：固件必须已在平台侧铺到 10%（§8.1）。
const MinOrgStagePct = 10

// PlatformBatch 是某固件最新的平台批次（parent_batch_id IS NULL 的那条）。
type PlatformBatch struct {
	ID     int64  `json:"batch_id"`
	Stage  string `json:"stage"`
	Status string `json:"status"`
}

// ParentEligible 纯函数：父批次是否允许组织在其下建子批次——档位 ≥ 10% 且未熔断（§8.1）。
func ParentEligible(stage, status string) bool {
	if status == "fused" {
		return false
	}
	pct, err := strconv.ParseFloat(strings.TrimSpace(stage), 64)
	if err != nil {
		return false
	}
	return pct >= MinOrgStagePct
}

// OrgOTABatch 是 iot_shard.org_ota_batch 一行。
type OrgOTABatch struct {
	OrgID     int64      `json:"org_id"`
	BatchID   int64      `json:"batch_id"`
	Window    *OTAWindow `json:"window,omitempty"`
	CreatedBy int64      `json:"created_by"`
	CreatedAt time.Time  `json:"created_at"`
}

// ---- ota-svc 客户端 ----

// OTABatchReq 是 ota-svc POST /api/v1/ota/batches 的请求体（子批次形态）。
type OTABatchReq struct {
	FirmwareID    int64          `json:"firmware_id"`
	CreatedBy     string         `json:"created_by"`
	ExplicitSNs   []string       `json:"explicit_sns"`
	ParentBatchID *int64         `json:"parent_batch_id,omitempty"`
	Policy        map[string]any `json:"policy,omitempty"`
}

// OTABatchResp 是 ota-svc 返回的批次。
type OTABatchResp struct {
	ID          int64  `json:"id"`
	Stage       string `json:"stage"`
	Status      string `json:"status"`
	TargetTotal int    `json:"target_total"`
}

// OTAClient 是「在 ota-svc 建子批次」的抽象；生产是 HTTP 客户端，测试注入 fake。
type OTAClient interface {
	CreateBatch(ctx context.Context, req OTABatchReq) (*OTABatchResp, error)
}

// HTTPOTAClient 调 ota-svc POST /api/v1/ota/batches。
type HTTPOTAClient struct {
	Base string
	HTTP *http.Client
}

func NewHTTPOTAClient(base string) *HTTPOTAClient {
	return &HTTPOTAClient{Base: strings.TrimRight(base, "/"), HTTP: &http.Client{Timeout: 10 * time.Second}}
}

func (c *HTTPOTAClient) CreateBatch(ctx context.Context, r OTABatchReq) (*OTABatchResp, error) {
	body, _ := json.Marshal(r)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+"/api/v1/ota/batches", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ota-svc create batch: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		// ota-svc 的 403（未审批）/409（档位、熔断）原样透出给组织管理员，别翻成 500
		return nil, fmt.Errorf("%w: ota-svc status %d: %s", otaErrFor(resp.StatusCode), resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Code int          `json:"code"`
		Data OTABatchResp `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("ota-svc decode: %w", err)
	}
	if out.Data.ID == 0 {
		return nil, fmt.Errorf("ota-svc returned no batch id: %s", strings.TrimSpace(string(raw)))
	}
	return &out.Data, nil
}

func otaErrFor(status int) error {
	switch status {
	case http.StatusForbidden:
		return ErrDenied
	case http.StatusConflict:
		return ErrConflict
	case http.StatusBadRequest, http.StatusNotFound:
		return ErrBadParam
	}
	return errors.New("ota-svc")
}

// ---- Store 扩展（org_ota_batch 与父批次查询） ----

// OTAStore 是批量 OTA 用到的 Store 子集。Store 主接口（store.go）不动：
// 这里用窄接口 + 类型断言，PGStore 与测试 fake 各自实现即可。
type OTAStore interface {
	GetSite(ctx context.Context, orgID, siteID int64) (*Site, error)
	SiteSNs(ctx context.Context, orgID, siteID int64) ([]string, error)
	OrgSNSet(ctx context.Context, orgID int64, sns []string) (map[string]bool, error)
	LatestPlatformBatch(ctx context.Context, firmwareID int64) (*PlatformBatch, error)
	InsertOrgOTABatch(ctx context.Context, b OrgOTABatch) error
	ListOrgOTABatches(ctx context.Context, orgID int64) ([]OrgOTABatch, error)
}

func (s *Service) otaStore() (OTAStore, error) {
	st, ok := s.Store.(OTAStore)
	if !ok {
		return nil, fmt.Errorf("store does not support org OTA batches")
	}
	return st, nil
}

// OrgSNSet 返回 sns 中确实属于该组织的那些（租户隔离：org_id 是第一个条件）。
func (s *PGStore) OrgSNSet(ctx context.Context, orgID int64, sns []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(sns) == 0 {
		return out, nil
	}
	rows, err := s.DB.Query(ctx, `SELECT sn FROM iot_shard.device_org WHERE org_id=$1 AND sn = ANY($2)`, orgID, sns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var sn string
		if err := rows.Scan(&sn); err != nil {
			return nil, err
		}
		out[sn] = true
	}
	return out, rows.Err()
}

// LatestPlatformBatch 取某固件最新的平台批次（子批次不算），找不到 → ErrNotFound。
func (s *PGStore) LatestPlatformBatch(ctx context.Context, firmwareID int64) (*PlatformBatch, error) {
	var b PlatformBatch
	err := s.DB.QueryRow(ctx,
		`SELECT id, stage, status FROM iot_global.ota_batch
		  WHERE firmware_id=$1 AND parent_batch_id IS NULL ORDER BY id DESC LIMIT 1`, firmwareID).
		Scan(&b.ID, &b.Stage, &b.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("latest platform batch: %w", err)
	}
	return &b, nil
}

func (s *PGStore) InsertOrgOTABatch(ctx context.Context, b OrgOTABatch) error {
	var win *string
	if b.Window != nil && !b.Window.Empty() {
		raw, _ := json.Marshal(b.Window)
		str := string(raw)
		win = &str
	}
	_, err := s.DB.Exec(ctx,
		`INSERT INTO iot_shard.org_ota_batch(org_id,batch_id,dispatch_window,created_by) VALUES($1,$2,$3::jsonb,$4)
		 ON CONFLICT (org_id,batch_id) DO NOTHING`, b.OrgID, b.BatchID, win, b.CreatedBy)
	if err != nil {
		return fmt.Errorf("insert org ota batch: %w", err)
	}
	return nil
}

func (s *PGStore) ListOrgOTABatches(ctx context.Context, orgID int64) ([]OrgOTABatch, error) {
	rows, err := s.DB.Query(ctx,
		`SELECT org_id,batch_id,dispatch_window,COALESCE(created_by,0),created_at FROM iot_shard.org_ota_batch
		  WHERE org_id=$1 ORDER BY batch_id DESC LIMIT 200`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []OrgOTABatch{}
	for rows.Next() {
		var b OrgOTABatch
		var win []byte
		if err := rows.Scan(&b.OrgID, &b.BatchID, &win, &b.CreatedBy, &b.CreatedAt); err != nil {
			return nil, err
		}
		if len(win) > 0 {
			var w OTAWindow
			if json.Unmarshal(win, &w) == nil {
				b.Window = &w
			}
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ---- 业务 ----

// OrgOTAReq 是 POST /api/v1/orgs/{org}/ota/batches 的请求体。
type OrgOTAReq struct {
	FirmwareID int64     `json:"firmware_id"`
	SNs        []string  `json:"sns"`
	SiteIDs    []int64   `json:"site_ids"`
	Window     OTAWindow `json:"window"`
}

// OrgOTAResp 是建子批次的响应。
type OrgOTAResp struct {
	BatchID       int64  `json:"batch_id"`
	ParentBatchID int64  `json:"parent_batch_id"`
	Stage         string `json:"stage"`
	Status        string `json:"status"`
	TargetTotal   int    `json:"target_total"`
	RequestedSNs  int    `json:"requested_sns"`
}

// CreateOrgOTABatch：校验窗口 → 展开 / 校验 SN 归属 → 校验父批次够格 → ota-svc 建子批次 → 写 org_ota_batch。
func (s *Service) CreateOrgOTABatch(ctx context.Context, orgID, actor int64, req OrgOTAReq) (*OrgOTAResp, error) {
	st, err := s.otaStore()
	if err != nil {
		return nil, err
	}
	if s.OTA == nil {
		return nil, fmt.Errorf("ota client not configured (IOT_OTA_URL)")
	}
	if req.FirmwareID <= 0 {
		return nil, fmt.Errorf("%w: firmware_id required", ErrBadParam)
	}
	if err := ValidateWindow(req.Window); err != nil {
		s.M.Inc(MOTAWindowRejected)
		return nil, fmt.Errorf("%w: %v", ErrBadParam, err)
	}

	sns, err := s.resolveOrgSNs(ctx, st, orgID, req)
	if err != nil {
		return nil, err
	}

	parent, err := st.LatestPlatformBatch(ctx, req.FirmwareID)
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("%w: firmware %d has no platform batch yet", ErrConflict, req.FirmwareID)
	}
	if err != nil {
		return nil, err
	}
	if !ParentEligible(parent.Stage, parent.Status) {
		return nil, fmt.Errorf("%w: firmware %d not open to orgs yet (platform batch %d is stage %s / %s; need stage ≥ %d%% and not fused)",
			ErrConflict, req.FirmwareID, parent.ID, parent.Stage, parent.Status, MinOrgStagePct)
	}

	policy := map[string]any{"idle_only": true}
	if !req.Window.Empty() || len(req.Window.Weekdays) > 0 {
		policy["window"] = req.Window
	}
	batch, err := s.OTA.CreateBatch(ctx, OTABatchReq{FirmwareID: req.FirmwareID, CreatedBy: fmt.Sprintf("org:%d:%s", orgID, actorOf(actor)),
		ExplicitSNs: sns, ParentBatchID: &parent.ID, Policy: policy})
	if err != nil {
		return nil, err
	}

	row := OrgOTABatch{OrgID: orgID, BatchID: batch.ID, CreatedBy: actor}
	if !req.Window.Empty() || len(req.Window.Weekdays) > 0 {
		w := req.Window
		row.Window = &w
	}
	if err := st.InsertOrgOTABatch(ctx, row); err != nil {
		// 子批次已经在 ota-svc 建好：本地台账写失败只告警，不假装整件事没发生
		slog.Error("org ota batch created but ledger insert failed", "org", orgID, "batch", batch.ID, "err", err)
	}
	s.M.Inc(MOTABatchesCreated)
	slog.Info("org ota batch created", "org", orgID, "batch", batch.ID, "parent", parent.ID,
		"sns", len(sns), "window", req.Window.Start+"-"+req.Window.End)
	return &OrgOTAResp{BatchID: batch.ID, ParentBatchID: parent.ID, Stage: batch.Stage, Status: batch.Status,
		TargetTotal: batch.TargetTotal, RequestedSNs: len(sns)}, nil
}

// resolveOrgSNs 把 sns / site_ids 解析成本组织的 SN 集合；出现不属于本组织的 SN → 403 并列出（§4.4 第一道锁）。
func (s *Service) resolveOrgSNs(ctx context.Context, st OTAStore, orgID int64, req OrgOTAReq) ([]string, error) {
	set := map[string]bool{}
	if len(req.SNs) > 0 {
		owned, err := st.OrgSNSet(ctx, orgID, req.SNs)
		if err != nil {
			return nil, err
		}
		var foreign []string
		for _, sn := range req.SNs {
			if owned[sn] {
				set[sn] = true
				continue
			}
			foreign = append(foreign, sn)
		}
		if len(foreign) > 0 {
			sort.Strings(foreign)
			if len(foreign) > 20 {
				foreign = append(foreign[:20], "...")
			}
			return nil, fmt.Errorf("%w: sns not in this org: %s", ErrDenied, strings.Join(foreign, ","))
		}
	}
	for _, siteID := range req.SiteIDs {
		if _, err := st.GetSite(ctx, orgID, siteID); err != nil {
			return nil, err // 站点不属本组织 → 404
		}
		sns, err := st.SiteSNs(ctx, orgID, siteID)
		if err != nil {
			return nil, err
		}
		for _, sn := range sns {
			set[sn] = true
		}
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("%w: sns or site_ids required (no device resolved)", ErrBadParam)
	}
	out := make([]string, 0, len(set))
	for sn := range set {
		out = append(out, sn)
	}
	sort.Strings(out)
	return out, nil
}

// ListOrgOTABatches 列出本组织建过的子批次。
func (s *Service) ListOrgOTABatches(ctx context.Context, orgID int64) ([]OrgOTABatch, error) {
	st, err := s.otaStore()
	if err != nil {
		return nil, err
	}
	return st.ListOrgOTABatches(ctx, orgID)
}

// registerOTA 挂载组织批量 OTA 路由（租户子树内，由 handler.go 的 registerTenant 调用）。
// 权限沿用已有的 manage_queue（org_admin / teacher）：批量 OTA 与排班同属「班级设备的运维动作」，
// 为它单开一个角色动作会让 §4.2 的矩阵多一列而没有新的角色边界。
func registerOTA(mux *http.ServeMux, svc *Service) {
	mux.HandleFunc("POST /api/v1/orgs/{org}/ota/batches", svc.guard(ActManageQueue, func(w http.ResponseWriter, r *http.Request) {
		var req OrgOTAReq
		if err := httpx.Decode(r, &req); err != nil {
			httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
			return
		}
		ctx := r.Context()
		resp, err := svc.CreateOrgOTABatch(ctx, OrgOf(ctx), UserOf(ctx), req)
		if err != nil {
			fail(w, err)
			return
		}
		httpx.JSON(w, http.StatusCreated, httpx.Resp{Code: httpx.CodeOK, Data: resp})
	}))
	mux.HandleFunc("GET /api/v1/orgs/{org}/ota/batches", svc.guard(ActView, func(w http.ResponseWriter, r *http.Request) {
		list, err := svc.ListOrgOTABatches(r.Context(), OrgOf(r.Context()))
		if err != nil {
			fail(w, err)
			return
		}
		if list == nil {
			list = []OrgOTABatch{}
		}
		httpx.OK(w, map[string]any{"batches": list})
	}))
}
