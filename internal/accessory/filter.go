package accessory

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xtool/xtool-aiot/internal/pkg/shadow"
	"github.com/xtool/xtool-aiot/internal/pkg/tdengine"
)

// 滤芯寿命（技术方案 §8）：每小时批，累计等效风量 → health → 线性外推 EOL。
// 原型近似：档位由 telemetry.fan_rpm 均值按 rpm_levels 分档；运行秒数 = fan_rpm>0 的采样行数 × 采样周期；
// 压差取影子 reported.pressure_diff 当前值（pressure_diff 不在 TDengine 列里）。

// FilterModelCfg 是 filter_model 一行。
type FilterModelCfg struct {
	Model       string
	RatedVolume float64         // 等效风量上限（m³）
	FlowCoeff   map[int]float64 // 档位 → m³/h
	P0, PSpan   float64
	KP          float64
	RPMLevels   []float64 // 升序阈值：rpm >= levels[i] → 档位 i+1
}

func (c *FilterModelCfg) parse(coeff, levels []byte) error {
	raw := map[string]float64{}
	if err := json.Unmarshal(coeff, &raw); err != nil {
		return fmt.Errorf("flow_coeff: %w", err)
	}
	c.FlowCoeff = map[int]float64{}
	for k, v := range raw {
		n, err := strconv.Atoi(k)
		if err != nil {
			return fmt.Errorf("flow_coeff key %q", k)
		}
		c.FlowCoeff[n] = v
	}
	if len(levels) > 0 {
		if err := json.Unmarshal(levels, &c.RPMLevels); err != nil {
			return fmt.Errorf("rpm_levels: %w", err)
		}
		sort.Float64s(c.RPMLevels)
	}
	return nil
}

// DefaultFilterModel 是 HEPA-STD 的内置兜底（表缺行时用）。
func DefaultFilterModel() FilterModelCfg {
	return FilterModelCfg{Model: "HEPA-STD", RatedVolume: 3600000, FlowCoeff: map[int]float64{1: 60, 2: 120, 3: 200, 4: 300},
		P0: 50, PSpan: 100, KP: 0.5, RPMLevels: []float64{800, 1600, 2400, 3200}}
}

// LevelFromRPM 纯函数：rpm ≤ 0 → 0；否则为满足 rpm ≥ 阈值 的最大档（1..len）。
func LevelFromRPM(rpm float64, levels []float64) int {
	if rpm <= 0 || len(levels) == 0 {
		return 0
	}
	lv := 0
	for i, th := range levels {
		if rpm >= th {
			lv = i + 1
		}
	}
	if lv == 0 {
		lv = 1 // 转着但低于最低阈值：按 1 档
	}
	return lv
}

// HourAgg 是一个小时的聚合输入。
type HourAgg struct {
	Hour         time.Time
	RunSeconds   float64
	AvgRPM       float64 // 聚合原始值；Level 由 LevelFromRPM(AvgRPM, cfg.RPMLevels) 得出
	Level        int
	PressureDiff float64
}

// FilterState 是滤芯的累计状态。
type FilterState struct {
	EqAirVolume float64
	RunSeconds  float64
	Health      float64
	LastHour    time.Time
}

// PressureFactor 纯函数：1 + max(0, (p - p0)/p_span) × k_p。
func PressureFactor(pressure float64, cfg FilterModelCfg) float64 {
	if cfg.PSpan <= 0 {
		return 1
	}
	over := (pressure - cfg.P0) / cfg.PSpan
	if over < 0 {
		over = 0
	}
	return 1 + over*cfg.KP
}

// FilterStep 纯函数：应用一个小时的运行数据。Level 0 或 RunSeconds 0 只推进 LastHour。
func FilterStep(prev FilterState, hour HourAgg, cfg FilterModelCfg) FilterState {
	next := prev
	if hour.Level > 0 && hour.RunSeconds > 0 {
		coeff := cfg.FlowCoeff[hour.Level]
		dv := hour.RunSeconds / 3600 * coeff * PressureFactor(hour.PressureDiff, cfg)
		next.EqAirVolume += dv
		next.RunSeconds += hour.RunSeconds
	}
	if cfg.RatedVolume > 0 {
		next.Health = clamp01(1 - next.EqAirVolume/cfg.RatedVolume)
	} else {
		next.Health = 1
	}
	if hour.Hour.After(next.LastHour) {
		next.LastHour = hour.Hour
	}
	return next
}

// PredictEOL 纯函数：按日均等效风量线性外推；日均 ≤ 0 或已耗尽时 ok=false / 立即。
func PredictEOL(st FilterState, dailyAvg float64, cfg FilterModelCfg, now time.Time) (time.Time, bool) {
	remain := cfg.RatedVolume - st.EqAirVolume
	if remain <= 0 {
		return now, true
	}
	if dailyAvg <= 0 {
		return time.Time{}, false
	}
	days := remain / dailyAvg
	return now.Add(time.Duration(days * float64(24*time.Hour))), true
}

// ResetFilter 纯函数：换芯 → 清零。
func ResetFilter(installedAt time.Time) FilterState {
	return FilterState{Health: 1, LastHour: installedAt.Truncate(time.Hour)}
}

func clamp01(v float64) float64 {
	if v < 0 || math.IsNaN(v) {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// TDQuerier 是 FilterBatch 依赖的 TDengine 子集。
type TDQuerier interface {
	Query(ctx context.Context, sql string) (*tdengine.Result, error)
}

// BuildHourlyQuery 生成某配件 [from, to) 的小时聚合 SQL（只统计 fan_rpm>0 的采样行）。
func BuildHourlyQuery(accSN string, from, to time.Time) string {
	return fmt.Sprintf("SELECT _wstart, avg(fan_rpm), count(*) FROM iot.telemetry WHERE sn='%s' AND fan_rpm > 0 AND ts >= %d AND ts < %d INTERVAL(1h)",
		sanitizeSN(accSN), from.UnixMilli(), to.UnixMilli())
}

func sanitizeSN(sn string) string {
	out := make([]byte, 0, len(sn))
	for i := 0; i < len(sn); i++ {
		c := sn[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-' {
			out = append(out, c)
		}
	}
	return string(out)
}

// ParseHourly 把 TDengine 结果解析成 HourAgg（RunSeconds = count × sampleSec；PressureDiff 由调用方填）。
func ParseHourly(res *tdengine.Result, sampleSec float64) ([]HourAgg, error) {
	if res == nil {
		return nil, nil
	}
	var out []HourAgg
	for _, row := range res.Data {
		if len(row) < 3 {
			continue
		}
		ts, ok := parseTDTime(row[0])
		if !ok {
			continue
		}
		rpm, _ := toFloat(row[1])
		cnt, _ := toFloat(row[2])
		out = append(out, HourAgg{Hour: ts.UTC().Truncate(time.Hour), RunSeconds: cnt * sampleSec, AvgRPM: rpm})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hour.Before(out[j].Hour) })
	return out, nil
}

func parseTDTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.000Z07:00", "2006-01-02 15:04:05.000"} {
			if ts, err := time.Parse(layout, t); err == nil {
				return ts, true
			}
		}
	case float64:
		return time.UnixMilli(int64(t)), true
	}
	return time.Time{}, false
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	}
	return 0, false
}

// FilterBatch 每小时把配件运行数据折算成滤芯寿命。
type FilterBatch struct {
	Store     Store
	TD        TDQuerier
	RDB       *redis.Client
	M         *Metrics
	SampleSec float64 // 采样周期（模拟器 -work 默认 5 s）
	Now       func() time.Time
}

// RunOnce 处理全部配件设备（product_key ACC_*）。返回处理设备数与小时数。
func (b *FilterBatch) RunOnce(ctx context.Context) (devices, hours int, err error) {
	b.M.Inc("filter_batch_runs")
	sns, err := b.Store.AccessorySNs(ctx, "ACC_")
	if err != nil {
		b.M.Inc("filter_errors")
		return 0, 0, err
	}
	now := b.Now().UTC()
	curHour := now.Truncate(time.Hour) // 只处理已完整结束的小时
	for _, sn := range sns {
		n, err := b.runDevice(ctx, sn, curHour)
		if err != nil {
			b.M.Inc("filter_errors")
			slog.Warn("filter batch device", "sn", sn, "err", err)
			continue
		}
		devices++
		hours += n
	}
	b.M.Add("filter_devices", int64(devices))
	b.M.Add("filter_hours", int64(hours))
	return devices, hours, nil
}

func (b *FilterBatch) runDevice(ctx context.Context, sn string, curHour time.Time) (int, error) {
	life, err := b.Store.FilterLifeGet(ctx, sn)
	if err != nil && err != ErrNotFound {
		return 0, err
	}
	var rep map[string]string
	if b.RDB != nil {
		rep, _ = shadow.Read(ctx, b.RDB, sn)
	}
	if life == nil {
		installed := curHour
		if v, ok := toFloat(rep["filter_installed_at"]); ok && v > 0 {
			installed = time.Unix(int64(v), 0).UTC()
		}
		life = &FilterLife{AccSN: sn, FilterModel: "HEPA-STD", InstalledAt: installed, Health: 1}
	}
	cfg := DefaultFilterModel()
	if c, err := b.Store.FilterModel(ctx, life.FilterModel); err == nil {
		cfg = *c
	}
	st := FilterState{EqAirVolume: life.EqAirVolume, RunSeconds: life.RunSeconds, Health: life.Health}
	if life.LastHour != nil {
		st.LastHour = *life.LastHour
	} else {
		st.LastHour = life.InstalledAt.Truncate(time.Hour).Add(-time.Hour)
	}
	from := st.LastHour.Add(time.Hour)
	if !from.Before(curHour) {
		return 0, nil // 幂等：没有新完整小时
	}
	if curHour.Sub(from) > 7*24*time.Hour {
		from = curHour.Add(-7 * 24 * time.Hour) // 首次或长期离线只补最近 7 天
	}
	res, err := b.TD.Query(ctx, BuildHourlyQuery(sn, from, curHour))
	if err != nil {
		return 0, err
	}
	aggs, _ := ParseHourly(res, b.SampleSec)
	pressure, _ := toFloat(rep["pressure_diff"])
	var beforeVol = st.EqAirVolume
	for i := range aggs {
		aggs[i].Level = LevelFromRPM(aggs[i].AvgRPM, cfg.RPMLevels)
		aggs[i].PressureDiff = pressure
		st = FilterStep(st, aggs[i], cfg)
	}
	// 即使没有运行数据也推进 last_hour（幂等基准）
	if st.LastHour.Before(curHour.Add(-time.Hour)) {
		st.LastHour = curHour.Add(-time.Hour)
	}
	// 日均：本次窗口的增量按窗口天数折算；窗口不足 1 天按 1 天
	days := curHour.Sub(from).Hours() / 24
	if days < 1 {
		days = 1
	}
	dailyAvg := (st.EqAirVolume - beforeVol) / days
	life.EqAirVolume, life.RunSeconds, life.Health = st.EqAirVolume, st.RunSeconds, st.Health
	lh := st.LastHour
	life.LastHour = &lh
	if eol, ok := PredictEOL(st, dailyAvg, cfg, b.Now()); ok {
		life.PredictedEOLAt = &eol
	}
	if err := b.Store.FilterLifeUpsert(ctx, *life); err != nil {
		return 0, err
	}
	return len(aggs), nil
}

// Run 按 interval 常驻。
func (b *FilterBatch) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if d, h, err := b.RunOnce(ctx); err != nil {
				slog.Warn("filter batch", "err", err)
			} else {
				slog.Info("filter batch", "devices", d, "hours", h)
			}
		}
	}
}
