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
	"strings"
	"time"
)

// Dispatcher 是「把任务下发到设备」的抽象：生产是 deviceapi 的 HTTP 客户端，测试注入 fake。
type Dispatcher interface {
	JobStart(ctx context.Context, sn string, params map[string]any) (cmdID string, err error)
}

// HTTPDispatcher 调 deviceapi POST /api/v1/devices/{sn}/cmd，头 X-Source: fleet。
type HTTPDispatcher struct {
	Base string
	HTTP *http.Client
}

// SourceFleet 是 deviceapi 判定 job_start 的唯一合法来源（见 internal/deviceapi/jobstart.go）。
const SourceFleet = "fleet"

func NewHTTPDispatcher(base string) *HTTPDispatcher {
	return &HTTPDispatcher{Base: strings.TrimRight(base, "/"), HTTP: &http.Client{Timeout: 5 * time.Second}}
}

func (d *HTTPDispatcher) JobStart(ctx context.Context, sn string, params map[string]any) (string, error) {
	body, _ := json.Marshal(map[string]any{"action": "job_start", "params": params})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.Base+"/api/v1/devices/"+sn+"/cmd", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Source", SourceFleet)
	resp, err := d.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("deviceapi job_start: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("deviceapi job_start %s: status %d: %s", sn, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Code int `json:"code"`
		Data struct {
			CmdID string `json:"cmd_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("deviceapi job_start decode: %w", err)
	}
	if out.Code != 0 || out.Data.CmdID == "" {
		return "", fmt.Errorf("deviceapi job_start %s: code %d", sn, out.Code)
	}
	return out.Data.CmdID, nil
}

// Scheduler 每 DispatchInterval 一轮：队首 approved 项 → 空闲候选设备 → 抢占 → 下发。
// 抢占（ClaimDispatch 条件 UPDATE）是零重复下发的唯一依据：affected=0 就跳过，不管候选看起来多空闲（INC-5-13）。
type Scheduler struct {
	S *Service
	D Dispatcher
}

func NewScheduler(s *Service, d Dispatcher) *Scheduler { return &Scheduler{S: s, D: d} }

// Run 起两个 ticker：下发与过期清理。阻塞直到 ctx 取消。
func (sc *Scheduler) Run(ctx context.Context) {
	dt := time.NewTicker(sc.S.Opt.DispatchInterval)
	defer dt.Stop()
	et := time.NewTicker(sc.S.Opt.ExpireInterval)
	defer et.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-dt.C:
			if _, err := sc.DispatchOnce(ctx); err != nil {
				slog.Error("dispatch round", "err", err)
			}
		case <-et.C:
			if n, err := sc.ExpireOnce(ctx); err != nil {
				slog.Error("expire round", "err", err)
			} else if n > 0 {
				slog.Info("items expired", "n", n)
			}
		}
	}
}

// DispatchOnce 跑一轮：每个队列最多下发一项，返回成功下发数。
func (sc *Scheduler) DispatchOnce(ctx context.Context) (int, error) {
	s := sc.S
	queues, err := s.Store.AllQueues(ctx)
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, q := range queues {
		ok, err := sc.dispatchQueue(ctx, q)
		if err != nil {
			slog.Warn("dispatch queue", "queue", q.QueueID, "err", err)
			continue
		}
		if ok {
			sent++
		}
	}
	return sent, nil
}

func (sc *Scheduler) dispatchQueue(ctx context.Context, q Queue) (bool, error) {
	s := sc.S
	item, err := s.Store.HeadApproved(ctx, q.QueueID)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	active, err := s.Store.ActiveSNs(ctx, q.QueueID)
	if err != nil {
		return false, err
	}
	for _, sn := range q.DeviceSNs {
		if active[sn] {
			continue
		}
		rep, err := s.Sh.Read(ctx, sn)
		if err != nil {
			slog.Warn("dispatch shadow read", "sn", sn, "err", err)
			continue
		}
		if !Summarize(sn, rep).Idle() {
			continue
		}
		s.M.Inc(MDispatchAttempts)
		token := NewID()
		claimed, err := s.Store.ClaimDispatch(ctx, item.ItemID, sn, token)
		if err != nil {
			return false, err
		}
		if !claimed {
			// 别的副本抢先了（或设备已有进行中任务）：这一轮不再碰这个队列
			return false, nil
		}
		s.M.Inc(MDispatchClaimed)
		cmdID, err := sc.D.JobStart(ctx, sn, jobParams(item))
		if err != nil {
			s.M.Inc(MDispatchFailed)
			slog.Warn("job_start failed, reverting", "item", item.ItemID, "sn", sn, "err", err)
			if rerr := s.Store.RevertDispatch(ctx, item.ItemID, token, s.Opt.MaxRetry); rerr != nil {
				return false, rerr
			}
			return false, nil
		}
		if err := s.Store.ConfirmDispatch(ctx, item.ItemID, token, cmdID); err != nil {
			return false, err
		}
		s.M.Inc(MDispatchOK)
		slog.Info("dispatched", "item", item.ItemID, "sn", sn, "cmd_id", cmdID)
		return true, nil
	}
	return false, nil
}

// jobParams 组装 job_start 的参数（deviceapi 只做来源与空闲判定，不校验内容）。
func jobParams(it *Item) map[string]any {
	p := map[string]any{"job_id": it.JobID, "sha256": it.FileSHA256, "item_id": it.ItemID}
	if it.JobID == "" {
		p["job_id"] = it.ItemID
	}
	if it.FileURL != "" {
		p["url"] = it.FileURL
	}
	if it.MaterialID != "" {
		p["material_id"] = it.MaterialID
	}
	if it.ParamProfileID != "" {
		p["param_profile_id"] = it.ParamProfileID
	}
	return p
}

// ExpireOnce 把无人取走的 approved 与无进展的 dispatched 置 skipped。
func (sc *Scheduler) ExpireOnce(ctx context.Context) (int, error) {
	n, err := sc.S.Store.ExpireItems(ctx, sc.S.Opt.ApprovedTTL, sc.S.Opt.DispatchedTTL, sc.S.now())
	if err != nil {
		return 0, err
	}
	sc.S.M.Add(MItemsExpired, int64(n))
	return n, nil
}
