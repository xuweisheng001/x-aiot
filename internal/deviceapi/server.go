package deviceapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
	"github.com/xtool/xtool-aiot/internal/support/grantcheck"
)

const (
	Name    = "deviceapi"
	Version = "0.1.0"

	DesiredCacheTTL = time.Hour
	CmdResultTTL    = time.Hour
)

// JSPublisher 是审计消息发布抽象（jetstream.JetStream 满足）。
type JSPublisher interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// TDQuerier 是遥测查询抽象（*tdengine.Client 满足）。
type TDQuerier interface {
	Query(ctx context.Context, sql string) (*tdengine.Result, error)
}

// Server 装配所有依赖并提供 Handler()。
type Server struct {
	Store Store
	RDB   *redis.Client
	TD    TDQuerier
	MQTT  MQTTPublisher
	JS    JSPublisher
	Now   func() time.Time
	// Grants 校验 support / agent 来源的指令授权（BL6）；nil 时这两类来源一律 403（fail-closed）。
	Grants GrantChecker

	// 限流参数
	DeviceRPS   float64
	DeviceBurst int
	IPRPS       float64
	IPBurst     int
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Handler 组路由：cmd 走 DeviceAuth+设备维度限流；其余按 IP 限流。
func (s *Server) Handler() http.Handler {
	if s.DeviceRPS <= 0 {
		s.DeviceRPS, s.DeviceBurst = 1, 2
	}
	if s.IPRPS <= 0 {
		s.IPRPS, s.IPBurst = 50, 100
	}
	ipLimit := httpx.RateLimitByKey(IPRateKey, s.IPRPS, s.IPBurst)
	devLimit := DeviceRateLimit(s.DeviceRPS, s.DeviceBurst)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpx.Healthz(Name, Version))
	mux.Handle("GET /api/v1/devices/{sn}/shadow", ipLimit(http.HandlerFunc(s.getShadow)))
	// 批量影子（BL5 看板，≤ 200 SN 一批）
	mux.Handle("POST /api/v1/devices/shadows", ipLimit(http.HandlerFunc(s.postShadows)))
	mux.Handle("PATCH /api/v1/devices/{sn}/desired", ipLimit(http.HandlerFunc(s.patchDesired)))
	mux.Handle("POST /api/v1/devices/{sn}/cmd", devLimit(http.HandlerFunc(s.postCmd)))
	mux.Handle("GET /api/v1/cmds/{cmd_id}", ipLimit(http.HandlerFunc(s.getCmd)))
	mux.Handle("GET /api/v1/devices/{sn}/telemetry", ipLimit(http.HandlerFunc(s.getTelemetry)))
	return httpx.Chain(mux, httpx.Recover, httpx.Logging)
}

func desiredCacheKey(sn string) string { return "desired:" + sn }

type desiredCache struct {
	Version int64           `json:"version"`
	Desired json.RawMessage `json:"desired"`
}

func (s *Server) loadDesired(ctx context.Context, sn string) (json.RawMessage, int64, error) {
	if s.RDB != nil {
		if b, err := s.RDB.Get(ctx, desiredCacheKey(sn)).Bytes(); err == nil {
			var c desiredCache
			if json.Unmarshal(b, &c) == nil && len(c.Desired) > 0 {
				return c.Desired, c.Version, nil
			}
		}
	}
	desired, version, err := s.Store.GetDesired(ctx, sn)
	if err != nil {
		return nil, 0, err
	}
	if s.RDB != nil {
		if b, err := json.Marshal(desiredCache{Version: version, Desired: desired}); err == nil {
			if err := s.RDB.Set(ctx, desiredCacheKey(sn), b, DesiredCacheTTL).Err(); err != nil {
				slog.Warn("desired cache set", "sn", sn, "err", err)
			}
		}
	}
	return desired, version, nil
}

// GET /api/v1/devices/{sn}/shadow
func (s *Server) getShadow(w http.ResponseWriter, r *http.Request) {
	sn := r.PathValue("sn")
	ctx := r.Context()
	reported, err := shadow.Read(ctx, s.RDB, sn)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
		return
	}
	desired, version, err := s.loadDesired(ctx, sn)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
		return
	}
	// switches：desired 中每个布尔开关的三态（applied / pending / offline），客户端只渲染 state（INC-4-06）
	httpx.OK(w, map[string]any{"sn": sn, "reported": reported, "desired": desired, "desired_version": version,
		"switches": BuildSwitches(reported, desired, s.now())})
}

// PATCH /api/v1/devices/{sn}/desired
func (s *Server) patchDesired(w http.ResponseWriter, r *http.Request) {
	sn := r.PathValue("sn")
	var patch map[string]json.RawMessage
	if err := httpx.Decode(r, &patch); err != nil || len(patch) == 0 {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "body must be a non-empty JSON object")
		return
	}
	raw, _ := json.Marshal(patch)
	ctx := r.Context()
	version, desired, err := s.Store.MergeDesired(ctx, sn, raw)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
		return
	}
	if s.RDB != nil {
		if err := s.RDB.Del(ctx, desiredCacheKey(sn)).Err(); err != nil {
			slog.Warn("desired cache invalidate", "sn", sn, "err", err)
		}
	}
	msg, _ := json.Marshal(map[string]any{"version": version, "desired": desired})
	if err := s.MQTT.Publish(TopicDesired(sn), msg); err != nil {
		slog.Error("publish desired", "sn", sn, "err", err) // PG 已提交，设备重连后会拉取
	}
	httpx.OK(w, map[string]any{"version": version, "desired": desired})
}

type cmdRequest struct {
	Action   string          `json:"action"`
	Params   json.RawMessage `json:"params"`
	Operator string          `json:"operator"`
	Source   string          `json:"source"`
}

// POST /api/v1/devices/{sn}/cmd
func (s *Server) postCmd(w http.ResponseWriter, r *http.Request) {
	sn := r.PathValue("sn")
	var req cmdRequest
	if err := httpx.Decode(r, &req); err != nil {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, "invalid json")
		return
	}
	d := DecideCmd(req.Action)
	if !d.Allowed {
		httpx.Error(w, d.HTTPStatus, d.BizCode, d.Msg)
		return
	}
	// job_start 额外一道（BL5 §17.3）：只有 fleet 来源且设备空闲未锁才放行。放在白名单之后、
	// grant 与审计之前，不改变「grant → 审计 → 下发」的既有顺序。
	if req.Action == ActionJobStart {
		if jd := s.decideJobStart(r.Context(), sn, r.Header.Get(grantcheck.HeaderSource)); !jd.Allowed {
			slog.Warn("job_start refused", "sn", sn, "reason", jd.Msg)
			httpx.Error(w, jd.HTTPStatus, jd.BizCode, jd.Msg)
			return
		}
	}
	if len(req.Params) == 0 || string(req.Params) == "null" {
		req.Params = json.RawMessage(`{}`)
	}
	// 来源由服务身份头推导，请求体 source 忽略（BL6 §4.3）；operator 优先取 X-Operator 头。
	source := resolveSource(r.Header.Get(grantcheck.HeaderSource), req.Source)
	if op := strings.TrimSpace(r.Header.Get(grantcheck.HeaderOperator)); op != "" {
		req.Operator = op
	}
	if req.Operator == "" {
		req.Operator = "console"
	}
	cmdID := NewUUIDv4()
	// 授权在白名单之后、审计之前：support / agent 必须持有有效 grant，否则 403 并留一条 denied 审计（尽力）。
	if d := s.authorize(r.Context(), sn, source, req.Action); !d.OK {
		denied := AuditRecord{CmdID: cmdID, SN: sn, Action: req.Action, Params: req.Params, Operator: req.Operator,
			Source: source, Result: "denied", CreatedAt: s.now().UTC()}
		if err := s.publishAudit(r.Context(), denied); err != nil {
			slog.Warn("publish denied audit", "cmd_id", cmdID, "err", err)
		}
		slog.Warn("cmd refused: no valid grant", "sn", sn, "source", source, "action", req.Action, "reason", d.Reason)
		httpx.Error(w, http.StatusForbidden, httpx.CodeDenied, d.Reason)
		return
	} else if d.GrantID != "" {
		req.Params = withGrantID(req.Params, d.GrantID)
	}
	// 审计先于指令：审计流不可用则拒绝下发（503/100013），保证 FR-19「审计 100% 落库」。
	rec := AuditRecord{CmdID: cmdID, SN: sn, Action: req.Action, Params: req.Params, Operator: req.Operator,
		Source: source, Result: "dispatched", CreatedAt: s.now().UTC()}
	if err := s.publishAudit(r.Context(), rec); err != nil {
		slog.Error("publish audit refused command", "cmd_id", cmdID, "sn", sn, "err", err)
		httpx.Error(w, http.StatusServiceUnavailable, httpx.CodeAuditUnavailable, "audit stream unavailable, command not dispatched")
		return
	}
	down, _ := json.Marshal(map[string]any{"cmd_id": cmdID, "action": req.Action, "params": req.Params})
	if err := s.MQTT.Publish(TopicCmd(sn), down); err != nil {
		rec.Result = "dispatch_failed"
		if aerr := s.publishAudit(r.Context(), rec); aerr != nil {
			slog.Error("publish dispatch_failed audit", "cmd_id", cmdID, "err", aerr) // 尽力而为
		}
		httpx.Error(w, http.StatusBadGateway, httpx.CodeInternal, "mqtt publish failed: "+err.Error())
		return
	}
	httpx.OK(w, map[string]any{"cmd_id": cmdID})
}

// AuditPublishTimeout 是等待 JetStream PubAck 的上限。
const AuditPublishTimeout = 5 * time.Second

// publishAudit 同步发布审计消息并等待 PubAck。
func (s *Server) publishAudit(ctx context.Context, rec AuditRecord) error {
	body, _ := json.Marshal(rec)
	actx, cancel := context.WithTimeout(ctx, AuditPublishTimeout)
	defer cancel()
	_, err := s.JS.Publish(actx, envelope.SubjectCmdAudit, body)
	return err
}

// GET /api/v1/cmds/{cmd_id}
func (s *Server) getCmd(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("cmd_id")
	ctx := r.Context()
	raw, err := s.RDB.Get(ctx, "cmdres:"+id).Bytes()
	if errors.Is(err, redis.Nil) {
		httpx.OK(w, map[string]any{"cmd_id": id, "status": "dispatched"})
		return
	}
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, httpx.CodeInternal, err.Error())
		return
	}
	var ack struct {
		Result string `json:"result"`
	}
	_ = json.Unmarshal(raw, &ack)
	result := strings.TrimSpace(ack.Result)
	if result == "" {
		result = "acked"
	}
	if len(result) > 16 {
		result = result[:16]
	}
	if err := s.Store.MarkAcked(ctx, id, result); err != nil {
		slog.Warn("mark acked", "cmd_id", id, "err", err)
	}
	if !json.Valid(raw) {
		raw, _ = json.Marshal(string(raw))
	}
	httpx.OK(w, map[string]any{"cmd_id": id, "status": "acked", "ack": json.RawMessage(raw)})
}

// GET /api/v1/devices/{sn}/telemetry?from=&to=&limit=
func (s *Server) getTelemetry(w http.ResponseWriter, r *http.Request) {
	sn := r.PathValue("sn")
	rng, err := ParseTelemetryRange(r.URL.Query(), s.now())
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, httpx.CodeBadParam, err.Error())
		return
	}
	res, err := s.TD.Query(r.Context(), BuildTelemetryQuery(sn, rng))
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not exist") {
			httpx.OK(w, map[string]any{"sn": sn, "from": rng.From, "to": rng.To, "rows": []map[string]any{}})
			return
		}
		httpx.Error(w, http.StatusBadGateway, httpx.CodeInternal, err.Error())
		return
	}
	httpx.OK(w, map[string]any{"sn": sn, "from": rng.From, "to": rng.To, "rows": RowsToMaps(res)})
}
