package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// Outcome 描述一条事件的处理结果（供日志与测试断言）。
type Outcome string

const (
	OutcomeStarted  Outcome = "started"
	OutcomeFinished Outcome = "finished"
	OutcomeIgnored  Outcome = "ignored"   // 非 JOB 事件
	OutcomeDropped  Outcome = "dropped"   // opt-in 不允许或字段非法
	OutcomeBad      Outcome = "bad"       // 载荷无法解析
	OutcomeDupStart Outcome = "dup_start" // 重复 JOB_START
)

// 反哺接口错误。
var (
	ErrOptInDenied = errors.New("job feedback opt-in not granted")
	ErrSNMismatch  = errors.New("sn does not own this job")
	ErrBadRating   = errors.New("bad rating")
)

// Service 是 job-svc 的核心：事件处理、标记、撤回删除。
type Service struct {
	Store  Store
	Shadow ShadowReader
	M      *Metrics
	Now    func() time.Time
}

func NewService(st Store, sh ShadowReader, m *Metrics) *Service {
	if m == nil {
		m = NewMetrics()
	}
	return &Service{Store: st, Shadow: sh, M: m, Now: time.Now}
}

// optIn 读影子并判定；不允许时计数。
func (s *Service) optIn(ctx context.Context, sn string) OptInState {
	rep, err := s.Shadow.Read(ctx, sn)
	st := DecideOptIn(rep, err)
	if st == OptInErr {
		slog.Warn("job optin shadow read failed, dropping (fail-closed)", "sn", sn, "err", err)
	}
	if m := st.Metric(); m != "" {
		s.M.Inc(m)
	}
	return st
}

// HandleEvent 处理一条 event 信封。返回 err 表示瞬时故障（调用方 Nak）；
// 解析 / 合规 / 格式类问题不返回 err（调用方 Ack 丢弃），以 Outcome 区分。
func (s *Service) HandleEvent(ctx context.Context, env *envelope.Envelope) (Outcome, error) {
	s.M.Inc("consumed")
	p, err := ParseEvent(env.Payload)
	if err != nil {
		s.M.Inc("payload_bad")
		return OutcomeBad, nil
	}
	if !IsJobCode(p.Code) {
		s.M.Inc("ignored_code")
		return OutcomeIgnored, nil
	}
	// 1 合规：opt-in fail-closed（先于任何格式校验与落库）
	if st := s.optIn(ctx, env.SN); st != OptInAllowed {
		return OutcomeDropped, nil
	}
	// 2 字段白名单
	if err := ValidateJobFields(p.JobFields); err != nil {
		s.M.Inc("dropped_bad_fields")
		slog.Warn("job fields rejected", "sn", env.SN, "code", p.Code, "reason", err.Error())
		return OutcomeDropped, nil
	}
	ts := p.Ts
	if ts == 0 {
		ts = env.RecvTs
	}
	at := time.UnixMilli(ts).UTC()

	// 3 落库
	var out Outcome
	if p.Code == CodeStart {
		inserted, err := s.Store.RecordStart(ctx, p.JobFields, env.SN, at)
		if err != nil {
			s.M.Inc("db_errors")
			return "", err
		}
		if inserted {
			s.M.Inc("records_started")
			out = OutcomeStarted
		} else {
			out = OutcomeDupStart
		}
	} else {
		late, err := s.Store.RecordFinish(ctx, p.JobFields, env.SN, at, OutcomeFor(p.Code))
		if err != nil {
			s.M.Inc("db_errors")
			return "", err
		}
		s.M.Inc("records_finished")
		if late {
			s.M.Inc("late_start")
		}
		out = OutcomeFinished
	}
	// 4 合并暂存标记
	merged, err := s.Store.MergePending(ctx, p.JobID)
	if err != nil {
		s.M.Inc("db_errors")
		return "", err
	}
	if merged {
		s.M.Inc("pending_merged")
	}
	return out, nil
}

// FeedbackStatus 是标记接口的结果。
type FeedbackStatus string

const (
	FeedbackStored  FeedbackStatus = "stored"  // 200
	FeedbackPending FeedbackStatus = "pending" // 202
)

// Feedback 写结果标记。sn 来自请求头（BFF 已校验 user ↔ sn 归属），必须与 job_record.sn 一致。
func (s *Service) Feedback(ctx context.Context, jobID, sn, rating string) (FeedbackStatus, error) {
	if !Ratings[rating] {
		return "", ErrBadRating
	}
	if !uuidRe.MatchString(jobID) || !idRe.MatchString(sn) {
		return "", fmt.Errorf("%w: job_id/sn format", ErrBadRating)
	}
	if st := s.optIn(ctx, sn); st != OptInAllowed {
		s.M.Inc("feedback_denied")
		return "", ErrOptInDenied
	}
	owner, found, err := s.Store.RecordSN(ctx, jobID)
	if err != nil {
		s.M.Inc("db_errors")
		return "", err
	}
	if !found {
		if err := s.Store.InsertPending(ctx, jobID, sn, rating); err != nil {
			s.M.Inc("db_errors")
			return "", err
		}
		s.M.Inc("feedback_pending")
		return FeedbackPending, nil
	}
	if owner != sn {
		s.M.Inc("feedback_denied")
		return "", ErrSNMismatch
	}
	if err := s.Store.UpsertFeedback(ctx, jobID, sn, rating); err != nil {
		s.M.Inc("db_errors")
		return "", err
	}
	s.M.Inc("feedback_ok")
	return FeedbackStored, nil
}

// NeedPurge 纯函数：desired 中 job_feedback_optin 明确为 false 才触发删除。
// 撤回以 desired（用户意图）为触发而不是 reported——显示要保守，删除要积极（INC-4-26）。
func NeedPurge(desired map[string]any) bool {
	v, ok := desired[OptInField]
	if !ok {
		return false
	}
	switch x := v.(type) {
	case bool:
		return !x
	case string:
		b, ok := ParseBool(x)
		return ok && !b
	}
	return false
}

// PurgeOnce 扫描 desired optin=false 的 SN 并删除其加工数据。返回删除总行数。
func (s *Service) PurgeOnce(ctx context.Context) (int64, error) {
	s.M.Inc("purge_runs")
	sns, err := s.Store.OptedOutSNs(ctx)
	if err != nil {
		s.M.Inc("db_errors")
		return 0, err
	}
	var total int64
	for _, sn := range sns {
		n, err := s.Store.PurgeSN(ctx, sn)
		if err != nil {
			s.M.Inc("db_errors")
			slog.Error("job purge failed", "sn", sn, "err", err)
			continue
		}
		if n > 0 {
			slog.Info("job data purged (opt-in withdrawn)", "sn", sn, "rows", n)
		}
		total += n
	}
	s.M.Add("purged_rows", total)
	return total, nil
}

// RunPurger 周期执行 PurgeOnce 直到 ctx 取消。
func (s *Service) RunPurger(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := s.PurgeOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("job purge run", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
