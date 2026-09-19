package support

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"sync"
	"time"
)

var identRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// ===== 诊断包内容：字段白名单即结构体字段（附录 B）。未知字段无法进入 =====

// Untrusted 是自由文本（事件 msg、用户描述），存储即打标签，供 Agent 代理隔离（§7.2）。
type Untrusted struct {
	Text string `json:"text"`
	Kind string `json:"kind"` // 恒为 "untrusted"
}

func TagUntrusted(text string, limit int) *Untrusted {
	if text == "" {
		return nil
	}
	if limit > 0 && len(text) > limit {
		text = text[:limit]
	}
	return &Untrusted{Text: text, Kind: "untrusted"}
}

type ShadowSection struct {
	Reported       map[string]string `json:"reported"`
	Desired        json.RawMessage   `json:"desired,omitempty"`
	DesiredVersion int64             `json:"desired_version"`
	Switches       json.RawMessage   `json:"switches,omitempty"`
}

type TelemetryPoint struct {
	Ts         int64   `json:"ts"`
	TempCavity float64 `json:"temp_cavity"`
	WorkState  int     `json:"work_state"`
	PowerLevel int     `json:"power_level"`
}

type DictBrief struct {
	Cause       string `json:"cause"`
	Steps       string `json:"steps"`
	NeedService bool   `json:"need_service"`
}

type EventItem struct {
	Ts   int64      `json:"ts"`
	Code string     `json:"code"`
	Msg  *Untrusted `json:"msg,omitempty"`
	Dict *DictBrief `json:"dict,omitempty"`
}

type AlarmItem struct {
	ID      int64     `json:"id"`
	Code    string    `json:"code"`
	Level   string    `json:"level"`
	Status  string    `json:"status"`
	EventTs time.Time `json:"event_ts"`
}

type AuditItem struct {
	CmdID     string    `json:"cmd_id"`
	Action    string    `json:"action"`
	Source    string    `json:"source"`
	Operator  string    `json:"operator"`
	CreatedAt time.Time `json:"created_at"`
	Result    string    `json:"result"`
}

type OTAItem struct {
	BatchID   int64     `json:"batch_id"`
	Status    string    `json:"status"`
	FwTo      string    `json:"fw_to"`
	UpdatedAt time.Time `json:"updated_at"`
}

type JobItem struct {
	JobID          string    `json:"job_id"`
	MaterialID     string    `json:"material_id"`
	ParamProfileID string    `json:"param_profile_id"`
	Outcome        string    `json:"outcome"`
	StartedAt      time.Time `json:"started_at"`
}

// BundleContent 是诊断包全部内容。只含 SN 与设备数据，不含任何身份字段。
type BundleContent struct {
	SN          string            `json:"sn"`
	ProductKey  string            `json:"product_key"`
	FWVersion   string            `json:"fw_version"`
	ModuleModel string            `json:"module_model"`
	GeneratedAt time.Time         `json:"generated_at"`
	Shadow      *ShadowSection    `json:"shadow,omitempty"`
	Telemetry   []TelemetryPoint  `json:"telemetry_10m"`
	Events      []EventItem       `json:"events_24h"`
	Alarms      []AlarmItem       `json:"alarms_7d"`
	Audit       []AuditItem       `json:"audit_30d"`
	OTA         *OTAItem          `json:"ota,omitempty"`
	Jobs        []JobItem         `json:"jobs_5"`
	UserNote    *Untrusted        `json:"user_note,omitempty"`
	Sources     map[string]string `json:"sources"`
	Degraded    bool              `json:"degraded"`
}

// AllowedKeys 是 content JSON 中允许出现的全部键（附录 B + degraded）。单测用它断言白名单。
var AllowedKeys = map[string]bool{
	"sn": true, "product_key": true, "fw_version": true, "module_model": true, "generated_at": true,
	"shadow": true, "reported": true, "desired": true, "desired_version": true, "switches": true,
	"telemetry_10m": true, "ts": true, "temp_cavity": true, "work_state": true, "power_level": true,
	"events_24h": true, "code": true, "msg": true, "text": true, "kind": true, "dict": true, "cause": true, "steps": true, "need_service": true,
	"alarms_7d": true, "id": true, "level": true, "status": true, "event_ts": true,
	"audit_30d": true, "cmd_id": true, "action": true, "source": true, "operator": true, "created_at": true, "result": true,
	"ota": true, "batch_id": true, "fw_to": true, "updated_at": true,
	"jobs_5": true, "job_id": true, "material_id": true, "param_profile_id": true, "outcome": true, "started_at": true,
	"user_note": true, "sources": true, "degraded": true,
	// switches 子键（deviceapi BuildSwitches 输出）与 reported 白名单键
	"value": true, "state": true,
}

// reportedAllow 是影子 reported 允许进入诊断包的键（设备属性；任何未知键一律丢弃，防止身份字段混入）。
var reportedAllow = map[string]bool{
	"work_state": true, "power_level": true, "temp_cavity": true, "temp_water": true, "fan_rpm": true,
	"laser_hours": true, "progress": true, "fw_version": true, "schema_version": true, "module_model": true,
	"camera_cloud_optin": true, "job_feedback_optin": true, "updated_at": true, "last_event_code": true,
	"last_event_ts": true, "seq": true, "ts": true,
}

// FilterReported 纯函数：只保留白名单键。
func FilterReported(rep map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range rep {
		if reportedAllow[k] {
			out[k] = v
		}
	}
	return out
}

// 允许作为 reported 键进入 content 的键也要加入 AllowedKeys。
func init() {
	for k := range reportedAllow {
		AllowedKeys[k] = true
	}
	// sources 的键是数据源名
	for _, s := range []string{SrcShadow, SrcTelemetry, SrcEvents, SrcAlarms, SrcAudit, SrcOTA, SrcJobs, SrcDevice} {
		AllowedKeys[s] = true
	}
}

// Downsample 纯函数：把 points 均匀抽成最多 n 个点（保留首尾），n<=0 返回原样。
func Downsample(points []TelemetryPoint, n int) []TelemetryPoint {
	if n <= 0 || len(points) <= n {
		return points
	}
	out := make([]TelemetryPoint, 0, n)
	step := float64(len(points)-1) / float64(n-1)
	for i := 0; i < n; i++ {
		out = append(out, points[int(float64(i)*step+0.5)])
	}
	return out
}

// CollectKeys 递归收集 JSON 中出现的全部对象键（单测白名单断言用）。
func CollectKeys(v any, into map[string]bool) {
	switch x := v.(type) {
	case map[string]any:
		for k, vv := range x {
			into[k] = true
			CollectKeys(vv, into)
		}
	case []any:
		for _, vv := range x {
			CollectKeys(vv, into)
		}
	}
}

// ===== 数据源 =====

const (
	SrcDevice    = "device"
	SrcShadow    = "shadow"
	SrcTelemetry = "telemetry"
	SrcEvents    = "events"
	SrcAlarms    = "alarms"
	SrcAudit     = "audit"
	SrcOTA       = "ota"
	SrcJobs      = "jobs"

	SourceOK          = "ok"
	SourceUnavailable = "unavailable"

	// SourceTimeout 每个数据源的上限（§5.1）。
	SourceTimeout = 2 * time.Second
	// BundleTTL 诊断包保留期。
	BundleTTL = 30 * 24 * time.Hour
)

// DeviceInfo 来自 iot_shard.device。
type DeviceInfo struct {
	ProductKey   string
	FWVersion    string
	Status       string
	ActivatedAt  *time.Time
	LastOnlineAt *time.Time
}

// ShadowResp 是 deviceapi GET /shadow 的 data。
type ShadowResp struct {
	Reported       map[string]string `json:"reported"`
	Desired        json.RawMessage   `json:"desired"`
	DesiredVersion int64             `json:"desired_version"`
	Switches       json.RawMessage   `json:"switches"`
}

// TelemetrySource / EventsSource 来自 TDengine；其余来自 PG（Store）。
type TelemetrySource interface {
	Telemetry(ctx context.Context, sn string, since time.Time) ([]TelemetryPoint, error)
	Events(ctx context.Context, sn string, since time.Time, limit int) ([]EventItem, error)
}

// Bundle 是 diagnostic_bundle 的一行。
type Bundle struct {
	BundleID  string            `json:"bundle_id"`
	SN        string            `json:"sn"`
	Trigger   string            `json:"trigger"`
	TicketID  string            `json:"ticket_id,omitempty"`
	Content   BundleContent     `json:"content"`
	Sources   map[string]string `json:"sources"`
	CreatedAt time.Time         `json:"created_at"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// BundleMeta 是列表项。
type BundleMeta struct {
	BundleID  string    `json:"bundle_id"`
	Trigger   string    `json:"trigger"`
	TicketID  string    `json:"ticket_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Degraded  bool      `json:"degraded"`
}

// ValidTrigger：user | support | agent。
func ValidTrigger(t string) bool { return t == "user" || t == "support" || t == "agent" }

// Generate 并发拉取七个源（每源 SourceTimeout），失败写 sources[src]=unavailable 并置 degraded；不阻塞生成。
func (s *Service) Generate(ctx context.Context, sn, trigger, ticketID, userNote string) (Bundle, error) {
	if !identRe.MatchString(sn) {
		return Bundle{}, fmt.Errorf("%w: sn", ErrBadParam)
	}
	if !ValidTrigger(trigger) {
		return Bundle{}, fmt.Errorf("%w: trigger", ErrBadParam)
	}
	now := s.now()
	c := BundleContent{SN: sn, GeneratedAt: now, Sources: map[string]string{},
		Telemetry: []TelemetryPoint{}, Events: []EventItem{}, Alarms: []AlarmItem{}, Audit: []AuditItem{}, Jobs: []JobItem{}}
	c.UserNote = TagUntrusted(userNote, 512)

	var mu sync.Mutex
	var wg sync.WaitGroup
	run := func(src string, f func(ctx context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sctx, cancel := context.WithTimeout(ctx, s.sourceTimeout())
			defer cancel()
			err := f(sctx)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				c.Sources[src] = SourceUnavailable
				c.Degraded = true
				s.M.Inc(MBundleSourceFail)
				slog.Warn("bundle source unavailable", "src", src, "sn", sn, "err", err)
				return
			}
			c.Sources[src] = SourceOK
		}()
	}
	run(SrcDevice, func(ctx context.Context) error {
		d, err := s.Store.DeviceInfo(ctx, sn)
		if err != nil {
			return err
		}
		mu.Lock()
		c.ProductKey, c.FWVersion = d.ProductKey, d.FWVersion
		mu.Unlock()
		return nil
	})
	run(SrcShadow, func(ctx context.Context) error {
		if s.Devices == nil {
			return ErrUnavailable
		}
		sh, err := s.Devices.Shadow(ctx, sn)
		if err != nil {
			return err
		}
		mu.Lock()
		c.Shadow = &ShadowSection{Reported: FilterReported(sh.Reported), Desired: sh.Desired, DesiredVersion: sh.DesiredVersion, Switches: sh.Switches}
		c.ModuleModel = sh.Reported["module_model"]
		if c.FWVersion == "" {
			c.FWVersion = sh.Reported["fw_version"]
		}
		mu.Unlock()
		return nil
	})
	run(SrcTelemetry, func(ctx context.Context) error {
		if s.TD == nil {
			return ErrUnavailable
		}
		pts, err := s.TD.Telemetry(ctx, sn, now.Add(-10*time.Minute))
		if err != nil {
			return err
		}
		mu.Lock()
		c.Telemetry = Downsample(pts, 60)
		mu.Unlock()
		return nil
	})
	run(SrcEvents, func(ctx context.Context) error {
		if s.TD == nil {
			return ErrUnavailable
		}
		evs, err := s.TD.Events(ctx, sn, now.Add(-24*time.Hour), 200)
		if err != nil {
			return err
		}
		codes := map[string]bool{}
		for _, e := range evs {
			codes[e.Code] = true
		}
		list := make([]string, 0, len(codes))
		for k := range codes {
			list = append(list, k)
		}
		sort.Strings(list)
		dict, derr := s.Store.DictLookup(ctx, list)
		if derr != nil {
			dict = nil
		}
		for i := range evs {
			if d, ok := dict[evs[i].Code]; ok {
				evs[i].Dict = &DictBrief{Cause: d.Cause, Steps: d.Steps, NeedService: d.NeedService}
			} else if isErrorCode(evs[i].Code) {
				s.M.Inc(MDictUnknown)
			}
		}
		mu.Lock()
		c.Events = evs
		mu.Unlock()
		return nil
	})
	run(SrcAlarms, func(ctx context.Context) error {
		a, err := s.Store.RecentAlarms(ctx, sn, now.Add(-7*24*time.Hour))
		if err != nil {
			return err
		}
		mu.Lock()
		c.Alarms = a
		mu.Unlock()
		return nil
	})
	run(SrcAudit, func(ctx context.Context) error {
		a, err := s.Store.RecentAudit(ctx, sn, now.Add(-30*24*time.Hour), 100)
		if err != nil {
			return err
		}
		mu.Lock()
		c.Audit = a
		mu.Unlock()
		return nil
	})
	run(SrcOTA, func(ctx context.Context) error {
		o, err := s.Store.CurrentOTA(ctx, sn)
		if err != nil {
			return err
		}
		mu.Lock()
		c.OTA = o
		mu.Unlock()
		return nil
	})
	run(SrcJobs, func(ctx context.Context) error {
		j, err := s.Store.RecentJobs(ctx, sn, 5)
		if err != nil {
			return err
		}
		mu.Lock()
		c.Jobs = j
		mu.Unlock()
		return nil
	})
	wg.Wait()

	b := Bundle{BundleID: NewID(), SN: sn, Trigger: trigger, TicketID: ticketID, Content: c, Sources: c.Sources,
		CreatedAt: now, ExpiresAt: now.Add(BundleTTL)}
	if err := s.Store.InsertBundle(ctx, b); err != nil {
		return Bundle{}, err
	}
	s.M.Inc(MBundles)
	if c.Degraded {
		s.M.Inc(MBundleDegraded)
	}
	return b, nil
}

// GetBundle：过期返回 ErrGone（410）。
func (s *Service) GetBundle(ctx context.Context, id string) (Bundle, error) {
	b, err := s.Store.GetBundle(ctx, id)
	if err != nil {
		return Bundle{}, err
	}
	if !b.ExpiresAt.After(s.now()) {
		return Bundle{}, fmt.Errorf("%w: bundle expired", ErrGone)
	}
	return b, nil
}

// isErrorCode：非安全码、非任务/系统事件码即视为错误码（字典覆盖对象）。
func isErrorCode(code string) bool {
	if excludedFromDefect[code] {
		return false
	}
	return len(code) > 2 && code[:2] == "E_"
}
