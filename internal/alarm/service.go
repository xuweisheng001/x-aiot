package alarm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

var (
	// ErrIllegalTransition：带前置状态的 UPDATE affected=0。
	ErrIllegalTransition = errors.New("illegal alarm state transition")
	ErrBadPayload        = errors.New("bad event payload")
)

const (
	SquelchTTL         = 5 * time.Minute
	EscalateAfter      = 10 * time.Minute
	EscalationInterval = 30 * time.Second
	LevelCritical      = "critical"
)

// Alarm 是 iot_shard.alarm 的一行。
type Alarm struct {
	ID               int64      `json:"id"`
	SN               string     `json:"sn"`
	Code             string     `json:"code"`
	Level            string     `json:"level"`
	EventTs          time.Time  `json:"event_ts"`
	Status           string     `json:"status"`
	NotifiedChannels *string    `json:"notified_channels,omitempty"`
	NotifiedAt       *time.Time `json:"notified_at,omitempty"`
	AckedAt          *time.Time `json:"acked_at,omitempty"`
	EscalatedAt      *time.Time `json:"escalated_at,omitempty"`
	ClosedBy         *string    `json:"closed_by,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

// Outcome 描述一条事件的处理结果（用于计数与测试）。
type Outcome string

const (
	OutcomeIgnored   Outcome = "ignored"   // 非安全码
	OutcomeDropped   Outcome = "dropped"   // payload 解析失败，Ack 丢弃
	OutcomeSquelched Outcome = "squelched" // 5 分钟窗口内重复
	OutcomeNotified  Outcome = "notified"
	OutcomeOpen      Outcome = "open" // 已入库但 open→notified 迁移失败（非法迁移）
)

type Service struct {
	DB  *pgxpool.Pool
	RDB *redis.Client
	M   *Metrics
	// Notify 模拟 App 推送；默认打一行 slog。返回 error 时告警停留在 open。
	Notify func(ctx context.Context, a Alarm) error
	// Targets 是 fleet-svc 的告警目标查询（BL5 §09.1，见 targets.go）；nil = 不查，按个人绑定推送。
	Targets TargetLookup
}

func NewService(db *pgxpool.Pool, rdb *redis.Client, m *Metrics) *Service {
	s := &Service{DB: db, RDB: rdb, M: m}
	s.Notify = func(_ context.Context, a Alarm) error {
		slog.Info("alarm notify (simulated app push)", "alarm_id", a.ID, "sn", a.SN, "code", a.Code, "channel", "app")
		return nil
	}
	return s
}

type eventPayload struct {
	Seq  int64  `json:"seq"`
	Ts   int64  `json:"ts"`
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

func SquelchKey(sn, code string) string { return fmt.Sprintf("alarm:squelch:%s:%s", sn, code) }

// HandleEvent 处理一条 event 信封。返回 err 表示瞬时故障（调用方 Nak）；
// 解析类错误不返回 err（调用方 Ack 丢弃），以 Outcome 区分。
func (s *Service) HandleEvent(ctx context.Context, env *envelope.Envelope) (Outcome, error) {
	return s.handleEvent(ctx, env, true)
}

// HandleEventNoSquelch 绕过 5 分钟 squelch 直接建告警。只供对账补录使用：
// 对账已经证明这条事件在 PG 里没有对应告警，再走 squelch 可能被同 SN 同 code 的近期告警吞掉。
func (s *Service) HandleEventNoSquelch(ctx context.Context, env *envelope.Envelope) (Outcome, error) {
	return s.handleEvent(ctx, env, false)
}

func (s *Service) handleEvent(ctx context.Context, env *envelope.Envelope, squelch bool) (Outcome, error) {
	s.M.Inc("events_total")
	var p eventPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil || p.Code == "" {
		s.M.Inc("events_bad_payload")
		return OutcomeDropped, nil
	}
	if !envelope.SafetyCodes[p.Code] {
		s.M.Inc("events_ignored")
		return OutcomeIgnored, nil
	}
	s.M.Inc("events_safety")

	ts := p.Ts
	if ts == 0 {
		ts = env.RecvTs
	}
	// squelch：Redis 出错时放行并计数（宁可重复告警不可漏报）。
	if squelch {
		ok, err := s.RDB.SetNX(ctx, SquelchKey(env.SN, p.Code), 1, SquelchTTL).Result()
		if err != nil {
			s.M.Inc("squelch_redis_errors")
			slog.Warn("squelch redis error, passing through", "err", err)
		} else if !ok {
			s.M.Inc("events_squelched")
			return OutcomeSquelched, nil
		}
	}

	var a Alarm
	err := s.DB.QueryRow(ctx,
		`INSERT INTO iot_shard.alarm(sn,code,level,event_ts,status)
		 VALUES ($1,$2,$3,to_timestamp($4::double precision/1000),'open')
		 RETURNING id, event_ts`, env.SN, p.Code, LevelCritical, ts).Scan(&a.ID, &a.EventTs)
	if err != nil {
		s.M.Inc("db_errors")
		return "", fmt.Errorf("insert alarm: %w", err)
	}
	a.SN, a.Code, a.Level, a.Status = env.SN, p.Code, LevelCritical, StatusOpen
	s.M.Inc("alarms_opened")

	// 组织订阅目标（只读辅助查询，失败即回退个人绑定；状态机不受影响，见 targets.go）
	if targets := s.resolveTargets(ctx, env.SN); len(targets) > 0 {
		slog.Info("alarm targets resolved", "alarm_id", a.ID, "sn", a.SN, "targets", len(targets), "first_source", targets[0].Source)
	}
	if err := s.Notify(ctx, a); err != nil {
		s.M.Inc("notify_errors")
		slog.Error("notify failed, alarm stays open", "alarm_id", a.ID, "err", err)
		return OutcomeOpen, nil
	}
	notifiedAt, err := s.markNotified(ctx, a.ID)
	if errors.Is(err, ErrIllegalTransition) {
		s.M.Inc("illegal_transitions")
		slog.Warn("open→notified rejected (already moved)", "alarm_id", a.ID)
		return OutcomeOpen, nil
	}
	if err != nil {
		s.M.Inc("db_errors")
		return "", err
	}
	s.M.Inc("alarms_notified")
	s.M.ObserveNotifyLatency(notifiedAt.Sub(a.EventTs))
	return OutcomeNotified, nil
}

// markNotified 是 open→notified 的带前置状态 UPDATE。
func (s *Service) markNotified(ctx context.Context, id int64) (time.Time, error) {
	var at time.Time
	err := s.DB.QueryRow(ctx,
		`UPDATE iot_shard.alarm SET status='notified', notified_at=now(), notified_channels='app'
		 WHERE id=$1 AND status='open' RETURNING notified_at`, id).Scan(&at)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrIllegalTransition
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("mark notified: %w", err)
	}
	return at, nil
}

// Ack：open|notified → acked。重复 ack affected=0 → ErrIllegalTransition，不会二次计数。
func (s *Service) Ack(ctx context.Context, id int64) error {
	tag, err := s.DB.Exec(ctx,
		`UPDATE iot_shard.alarm SET status='acked', acked_at=now()
		 WHERE id=$1 AND status = ANY($2)`, id, Sources(StatusAcked))
	if err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	if tag.RowsAffected() == 0 {
		s.M.Inc("illegal_transitions")
		return ErrIllegalTransition
	}
	s.M.Inc("alarms_acked")
	return nil
}

// Close：acked → closed。
func (s *Service) Close(ctx context.Context, id int64, closedBy string) error {
	tag, err := s.DB.Exec(ctx,
		`UPDATE iot_shard.alarm SET status='closed', closed_by=$2
		 WHERE id=$1 AND status = ANY($3)`, id, closedBy, Sources(StatusClosed))
	if err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if tag.RowsAffected() == 0 {
		s.M.Inc("illegal_transitions")
		return ErrIllegalTransition
	}
	s.M.Inc("alarms_closed")
	return nil
}

// List 按状态过滤（status 为空返回全部），最多 200 条。
func (s *Service) List(ctx context.Context, status string, limit int) ([]Alarm, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.DB.Query(ctx,
		`SELECT id,sn,code,level,event_ts,status,notified_channels,notified_at,acked_at,escalated_at,closed_by,created_at
		 FROM iot_shard.alarm WHERE ($1 = '' OR status = $1) ORDER BY id DESC LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Alarm{}
	for rows.Next() {
		var a Alarm
		if err := rows.Scan(&a.ID, &a.SN, &a.Code, &a.Level, &a.EventTs, &a.Status, &a.NotifiedChannels,
			&a.NotifiedAt, &a.AckedAt, &a.EscalatedAt, &a.ClosedBy, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// EscalateOnce 把 notified 超 10 分钟未 ack 的 critical 告警标记 escalated_at 并模拟短信。
func (s *Service) EscalateOnce(ctx context.Context) (int, error) {
	rows, err := s.DB.Query(ctx,
		`UPDATE iot_shard.alarm SET escalated_at=now()
		 WHERE status='notified' AND level='critical' AND notified_at < now() - interval '10 minutes' AND escalated_at IS NULL
		 RETURNING id, sn, code`)
	if err != nil {
		return 0, fmt.Errorf("escalate: %w", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id int64
		var sn, code string
		if err := rows.Scan(&id, &sn, &code); err != nil {
			return n, err
		}
		n++
		slog.Warn("alarm escalated: SMS fallback (simulated)", "alarm_id", id, "sn", sn, "code", code, "channel", "sms")
	}
	s.M.Add("alarms_escalated", int64(n))
	return n, rows.Err()
}

// RunEscalation 每 interval 跑一轮，直到 ctx 取消。
func (s *Service) RunEscalation(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.EscalateOnce(ctx); err != nil && ctx.Err() == nil {
				slog.Error("escalation round failed", "err", err)
			}
		}
	}
}
