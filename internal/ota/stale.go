package ota

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// StaleErrorCode 是 sweeper 写入 ota_device_task.error_code 的值。
const StaleErrorCode = "STALE_TIMEOUT"

// StaleStatuses 是"云端下发后等待设备回报"的中间态；停在这里超过 olderThan 视为刷砖/失联（预推演 INC-16 情形 B）。
func StaleStatuses() []string { return []string{TaskNotified, TaskDownloading, TaskVerifying} }

// IsStale 纯判定：status 是中间态且 updatedAt 早于 now-olderThan。olderThan<=0 永不 stale。
func IsStale(status string, updatedAt, now time.Time, olderThan time.Duration) bool {
	if olderThan <= 0 {
		return false
	}
	switch status {
	case TaskNotified, TaskDownloading, TaskVerifying:
		return updatedAt.Before(now.Add(-olderThan))
	}
	return false
}

// SweepStale 把停在中间态超过 olderThan 的任务置为 failed/STALE_TIMEOUT（retry 不变），
// 并按与设备上报 failed 完全相同的事务逻辑给批次计数、评估熔断；paused/fused/completed 批次只落任务状态不计数。
// 返回本轮置失败的任务数。
func (s *Service) SweepStale(ctx context.Context, olderThan time.Duration) (int, error) {
	if olderThan <= 0 {
		return 0, nil
	}
	rows, err := s.DB.Query(ctx,
		`SELECT batch_id, sn FROM iot_shard.ota_device_task
		 WHERE status = ANY($1) AND updated_at < now() - make_interval(secs => $2)
		 ORDER BY updated_at LIMIT 1000`, StaleStatuses(), olderThan.Seconds())
	if err != nil {
		return 0, fmt.Errorf("select stale: %w", err)
	}
	type key struct {
		batchID int64
		sn      string
	}
	var keys []key
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.batchID, &k.sn); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, k)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	n := 0
	for _, k := range keys {
		out, err := s.applyPhase(ctx, phaseChange{
			batchID: k.batchID, sn: k.sn, phase: TaskFailed, errorCode: StaleErrorCode,
			onlyIfStaleFor: olderThan, metricPrefix: "stale",
		})
		if err != nil {
			return n, err
		}
		if out == ProgressTerminal || out == ProgressFused {
			n++
			s.M.Inc("stale_failed")
			slog.Warn("ota task stale → failed", "batch_id", k.batchID, "sn", k.sn, "older_than", olderThan, "outcome", out)
		}
	}
	return n, nil
}

// RunStaleSweeper 周期执行 SweepStale 直到 ctx 取消。
func (s *Service) RunStaleSweeper(ctx context.Context, interval, olderThan time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := s.SweepStale(ctx, olderThan); err != nil && ctx.Err() == nil {
				slog.Error("stale sweep failed", "err", err)
			} else if n > 0 {
				slog.Info("stale sweep", "failed", n)
			}
		}
	}
}
