package support

import (
	"context"
	"fmt"
	"time"
)

// 保修数据汇总（§10）：只给数据与依据，不给判定。任何字段名不得出现 verdict / approved / decision 等判定语义。

// WarrantyStats 是从各表汇总的原始数据。
type WarrantyStats struct {
	ActivatedAt      *time.Time     `json:"activated_at,omitempty"`
	LaserHours       float64        `json:"laser_hours"`
	LaserHoursKnown  bool           `json:"laser_hours_known"`
	SafetyEvents30d  map[string]int `json:"safety_events_30d"`
	ErrorCodesTop    []CodeCount    `json:"error_codes_top"`
	OTASuccess       int            `json:"ota_success"`
	OTAFailed        int            `json:"ota_failed"`
	OTARolledBack    int            `json:"ota_rolled_back"`
	FWVersion        string         `json:"fw_version"`
	Jobs             int            `json:"jobs"`
	JobsNonOfficial  int            `json:"jobs_non_official_params"`
	JobsOptInPresent bool           `json:"jobs_opt_in_present"`

	// productKey 只用于查同机型基线，不进入响应（保修数据不含机型以外的归属信息）。
	productKey string
}

type CodeCount struct {
	Code  string `json:"code"`
	Count int    `json:"count"`
}

// Signal 每条带 evidence 与 threshold（INC-6-14）。没有数据时 no_data=true 且不给 value。
type Signal struct {
	Name      string  `json:"name"`
	Triggered bool    `json:"triggered"`
	NoData    bool    `json:"no_data,omitempty"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	Evidence  string  `json:"evidence"`
}

// Baseline 是同机型基线（P90）。
type Baseline struct {
	DailyLaserHoursP90 float64
	HasBaseline        bool
}

// Signals 纯函数：五条使用信号，每条含 evidence / threshold；输入缺失 → no_data。
func Signals(st WarrantyStats, base Baseline, now time.Time) []Signal {
	var out []Signal

	// high_intensity：日均 laser_hours 高于同机型 P90
	{
		s := Signal{Name: "high_intensity", Threshold: base.DailyLaserHoursP90}
		if st.ActivatedAt == nil || !st.LaserHoursKnown || !base.HasBaseline {
			s.NoData = true
			s.Evidence = "缺少激活时间 / 累计工时 / 机型基线之一"
		} else {
			days := now.Sub(*st.ActivatedAt).Hours() / 24
			if days < 1 {
				days = 1
			}
			s.Value = st.LaserHours / days
			s.Triggered = s.Value > s.Threshold
			s.Evidence = fmt.Sprintf("累计发光 %.1f h / 激活 %.0f 天 = 日均 %.2f h，同机型 P90 %.2f h", st.LaserHours, days, s.Value, s.Threshold)
		}
		out = append(out, s)
	}
	// repeated_over_temp：30 天 OVER_TEMP ≥ 5
	out = append(out, countSignal("repeated_over_temp", st.SafetyEvents30d["OVER_TEMP"], 5, "30 天 OVER_TEMP 事件 %d 次，阈值 %d"))
	// frequent_estop：30 天 ESTOP ≥ 10
	out = append(out, countSignal("frequent_estop", st.SafetyEvents30d["ESTOP"], 10, "30 天 ESTOP 事件 %d 次，阈值 %d"))
	// non_official_params：opt-in 用户非官方参数占比 > 50%
	{
		s := Signal{Name: "non_official_params", Threshold: 0.5}
		if !st.JobsOptInPresent || st.Jobs == 0 {
			s.NoData = true
			s.Evidence = "设备未开启加工数据反哺或无加工记录"
		} else {
			s.Value = float64(st.JobsNonOfficial) / float64(st.Jobs)
			s.Triggered = s.Value > s.Threshold
			s.Evidence = fmt.Sprintf("%d / %d 次加工使用非官方参数档，占比 %.0f%%，阈值 50%%", st.JobsNonOfficial, st.Jobs, s.Value*100)
		}
		out = append(out, s)
	}
	return out
}

func countSignal(name string, n, threshold int, format string) Signal {
	return Signal{Name: name, Value: float64(n), Threshold: float64(threshold), Triggered: n >= threshold,
		Evidence: fmt.Sprintf(format, n, threshold)}
}

// WarrantyCase 是 warranty_case 的一行。
type WarrantyCase struct {
	CaseID      string        `json:"case_id"`
	SN          string        `json:"sn"`
	TicketID    string        `json:"ticket_id,omitempty"`
	Summary     WarrantyStats `json:"summary"`
	Signals     []Signal      `json:"signals"`
	Disclaimer  string        `json:"disclaimer"`
	GeneratedAt time.Time     `json:"generated_at"`
}

// Disclaimer 是每份汇总都带的免责声明（INC-6-14）。
const Disclaimer = "以下为设备使用数据与统计信号，仅供参考，不构成保修判定；判定由售后按保修条款作出。"

// ForbiddenWarrantyKeys 是响应里绝不允许出现的判定语义键（单测断言）。
var ForbiddenWarrantyKeys = []string{"verdict", "approved", "approve", "decision", "eligible", "covered", "reject", "rejected", "in_warranty"}

// GenerateWarranty 汇总并落快照。
func (s *Service) GenerateWarranty(ctx context.Context, sn, ticketID string) (WarrantyCase, error) {
	if !identRe.MatchString(sn) {
		return WarrantyCase{}, fmt.Errorf("%w: sn", ErrBadParam)
	}
	st, err := s.Store.WarrantyStats(ctx, sn, s.now())
	if err != nil {
		return WarrantyCase{}, err
	}
	if s.Devices != nil {
		if sh, err := s.Devices.Shadow(ctx, sn); err == nil {
			if v, ok := parseFloat(sh.Reported["laser_hours"]); ok {
				st.LaserHours, st.LaserHoursKnown = v, true
			}
			if st.FWVersion == "" {
				st.FWVersion = sh.Reported["fw_version"]
			}
		}
	}
	base := Baseline{}
	if p90, ok, err := s.Store.Baseline(ctx, st.productKey, "daily_laser_hours"); err == nil && ok {
		base = Baseline{DailyLaserHoursP90: p90, HasBaseline: true}
	}
	c := WarrantyCase{CaseID: NewID(), SN: sn, TicketID: ticketID, Summary: st, Signals: Signals(st, base, s.now()), Disclaimer: Disclaimer, GeneratedAt: s.now()}
	if err := s.Store.InsertWarranty(ctx, c); err != nil {
		return WarrantyCase{}, err
	}
	s.M.Inc(MWarrantyGenerated)
	return c, nil
}

func parseFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	var f float64
	_, err := fmt.Sscanf(s, "%g", &f)
	return f, err == nil
}
