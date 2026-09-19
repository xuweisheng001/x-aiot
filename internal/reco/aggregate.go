package reco

import (
	"sort"
	"time"
)

// Rules 是候选生成阈值（技术方案 §9.2 与 INC-4-21 反刷偏）。
type Rules struct {
	MinSamples      int     // 去重后样本数下限（默认 30）
	MinDistinctSN   int     // 不同设备数下限（默认 30）
	MinGoodRatio    float64 // good / 样本 下限（默认 0.8）
	Top3Share       float64 // 前三台设备贡献占比上限（默认 0.5，超过排除）
	MaxPerDeviceDay int     // 单设备单日标记数上限（默认 20，超过排除）
}

// DefaultRules 是方案给定的初始值。
func DefaultRules() Rules {
	return Rules{MinSamples: 30, MinDistinctSN: 30, MinGoodRatio: 0.8, Top3Share: 0.5, MaxPerDeviceDay: 20}
}

// GroupKey 是聚合维度。
type GroupKey struct {
	ProductKey  string
	ModuleModel string
	MaterialID  string
	ParamsHash  string
}

// FeedbackRow 是一条标记明细（job_record ⋈ job_feedback 加维表富化后的形态）。
type FeedbackRow struct {
	GroupKey
	SN     string
	Rating string // good | burnt | uncut
	At     time.Time
}

// Candidate 是一个分组的聚合结果。Excluded=true 时 Reason 说明原因，不写候选表。
type Candidate struct {
	GroupKey
	Samples    int     // 去重后样本
	DistinctSN int     // 不同设备
	Good       int     // 去重后 good 数
	GoodRatio  float64 // Good / Samples
	Excluded   bool
	Reason     string
}

// 排除原因（稳定字符串，进日志与计数）。
const (
	ReasonUnknownDims = "unknown_dims"     // product_key / module_model 缺失
	ReasonFewSamples  = "few_samples"      // 样本不足
	ReasonFewDevices  = "few_devices"      // 设备数不足
	ReasonLowGood     = "low_good_ratio"   // good 占比不足
	ReasonTop3Share   = "top3_share"       // 前三台设备贡献过高
	ReasonDeviceFlood = "device_day_flood" // 单设备单日标记过多
)

// Aggregate 纯函数：按 GroupKey 聚合标记明细。
//
// 去重：同 SN 同分组同一天只计一次（取该天最后一条标记）；样本 = 去重后条数。
// 反刷偏：任一设备任一日原始标记数 > MaxPerDeviceDay，或前三台设备（按去重后贡献）占比 > Top3Share → 排除。
// 阈值：Samples ≥ MinSamples 且 DistinctSN ≥ MinDistinctSN 且 GoodRatio ≥ MinGoodRatio 才不排除。
// 输出按 GroupKey 稳定排序。
func Aggregate(rows []FeedbackRow, r Rules) []Candidate {
	type dayKey struct {
		sn  string
		day string
	}
	type group struct {
		latest   map[dayKey]FeedbackRow // 去重：每 SN 每日最后一条
		rawCount map[dayKey]int         // 原始标记数（反刷偏）
	}
	groups := map[GroupKey]*group{}
	for _, row := range rows {
		g, ok := groups[row.GroupKey]
		if !ok {
			g = &group{latest: map[dayKey]FeedbackRow{}, rawCount: map[dayKey]int{}}
			groups[row.GroupKey] = g
		}
		dk := dayKey{row.SN, row.At.UTC().Format("2006-01-02")}
		g.rawCount[dk]++
		if cur, ok := g.latest[dk]; !ok || row.At.After(cur.At) {
			g.latest[dk] = row
		}
	}

	out := make([]Candidate, 0, len(groups))
	for k, g := range groups {
		c := Candidate{GroupKey: k}
		perSN := map[string]int{}
		for dk, row := range g.latest {
			c.Samples++
			perSN[dk.sn]++
			if row.Rating == "good" {
				c.Good++
			}
		}
		c.DistinctSN = len(perSN)
		if c.Samples > 0 {
			c.GoodRatio = float64(c.Good) / float64(c.Samples)
		}
		switch {
		case k.ProductKey == "" || k.ProductKey == "UNKNOWN" || k.ModuleModel == "" || k.ModuleModel == "UNKNOWN":
			c.Excluded, c.Reason = true, ReasonUnknownDims
		case deviceFlood(g.rawCount, r.MaxPerDeviceDay):
			c.Excluded, c.Reason = true, ReasonDeviceFlood
		case c.Samples < r.MinSamples:
			c.Excluded, c.Reason = true, ReasonFewSamples
		case c.DistinctSN < r.MinDistinctSN:
			c.Excluded, c.Reason = true, ReasonFewDevices
		case top3Share(perSN, c.Samples) > r.Top3Share: // 样本够了再看集中度，小组的 top3 恒为 100% 没有意义
			c.Excluded, c.Reason = true, ReasonTop3Share
		case c.GoodRatio < r.MinGoodRatio:
			c.Excluded, c.Reason = true, ReasonLowGood
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].GroupKey, out[j].GroupKey
		if a.ProductKey != b.ProductKey {
			return a.ProductKey < b.ProductKey
		}
		if a.ModuleModel != b.ModuleModel {
			return a.ModuleModel < b.ModuleModel
		}
		if a.MaterialID != b.MaterialID {
			return a.MaterialID < b.MaterialID
		}
		return a.ParamsHash < b.ParamsHash
	})
	return out
}

func deviceFlood[K comparable](raw map[K]int, max int) bool {
	if max <= 0 {
		return false
	}
	for _, n := range raw {
		if n > max {
			return true
		}
	}
	return false
}

// top3Share 返回贡献最多的三台设备占样本的比例；样本为 0 返回 0。
func top3Share(perSN map[string]int, samples int) float64 {
	if samples == 0 {
		return 0
	}
	counts := make([]int, 0, len(perSN))
	for _, n := range perSN {
		counts = append(counts, n)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(counts)))
	sum := 0
	for i := 0; i < len(counts) && i < 3; i++ {
		sum += counts[i]
	}
	return float64(sum) / float64(samples)
}

// PowerCapRatio 是推荐档功率相对同维度官方档的上限（INC-4-19 根治）。
const PowerCapRatio = 1.10

// WithinPowerCap 纯函数：候选功率 ≤ 官方功率 × 1.10 才允许。officialPower ≤ 0 表示无官方档，不设限。
func WithinPowerCap(candidatePower, officialPower float64) bool {
	if officialPower <= 0 {
		return true
	}
	return candidatePower <= officialPower*PowerCapRatio+1e-9
}
