// Package simulator 是虚拟设备：bootstrap → MQTT 连接 → 遥测/心跳/事件上行，指令/OTA/desired 下行处理，重连纪律。
package simulator

import (
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/xtool/xtool-aiot/internal/pkg/backoff"
)

// EventSpec 是 -event CODE@DELAY。
type EventSpec struct {
	Code string
	At   time.Duration
}

func (e EventSpec) Enabled() bool { return e.Code != "" }

// Config 对应命令行 flags。
type Config struct {
	// SeqBase 是上行 seq 的起始值。0 表示按启动时刻推导（UnixMilli×1000），模拟真实设备跨重启单调的 seq（DR-07），
	// 避免 10 分钟内重跑模拟器时与云端 dedupe:{sn}:{seq} 撞键；负数表示从 0 起（仅测试）。
	SeqBase     int64
	N           int
	PK          string
	MQTTURL     string // tcp://host:port（bootstrap 不可达时的兜底）
	Bootstrap   string // http://host:port，空则不走 bootstrap
	HB          time.Duration
	Work        time.Duration
	Event       EventSpec
	BadFirmware float64
	OTAFailRate float64
	SNPrefix    string
	FW          string

	// 物模型 v1.1（BL4）
	Jobs     bool   // 作业周期内发 JOB_START / JOB_DONE
	Module   string // module_model 属性
	JobOptin bool   // 出厂 reported.job_feedback_optin；desired 下发后以 desired 为准并回报

	// 配件模式（BL2）：设备是净化器，只上报净化器属性并响应 desired {power_on, fan_level}
	Accessory    bool
	PairHost     string // 非空时启动后通过 accessory-svc 把全部配件配到该主机
	AccessoryURL string // accessory-svc base url
}

// ParseEvent 解析 "FLAME_DETECTED@30s"；空串表示不发事件。
func ParseEvent(s string) (EventSpec, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return EventSpec{}, nil
	}
	code, at, ok := strings.Cut(s, "@")
	if !ok || code == "" {
		return EventSpec{}, fmt.Errorf("event %q: want CODE@DURATION", s)
	}
	d, err := time.ParseDuration(at)
	if err != nil || d < 0 {
		return EventSpec{}, fmt.Errorf("event %q: bad duration", s)
	}
	return EventSpec{Code: code, At: d}, nil
}

// SNFor 生成第 i（从 1 起）台设备的 SN：PREFIX + 5 位序号。
func SNFor(prefix string, i int) string { return fmt.Sprintf("%s%05d", prefix, i) }

// IsBadFirmware：前 round(frac·n) 台是坏固件（确定性，便于复现）。
func IsBadFirmware(i, n int, frac float64) bool {
	if frac <= 0 || n <= 0 {
		return false
	}
	return i < int(math.Round(frac*float64(n)))
}

// ReconnectDelay 是重连纪律：好固件 backoff.Next(n, 1s)；坏固件固定 1s 不退避（复现风暴用）。
func ReconnectDelay(bad bool, attempt int) time.Duration {
	if bad {
		return time.Second
	}
	return backoff.Next(attempt, time.Second)
}

// HostPort 把 tcp://host:port 或 host:port 归一成 host:port。
func HostPort(s string) (string, error) {
	if !strings.Contains(s, "://") {
		if s == "" {
			return "", fmt.Errorf("empty mqtt address")
		}
		return s, nil
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("mqtt url %q: missing host", s)
	}
	if u.Port() == "" {
		return u.Host + ":1883", nil
	}
	return u.Host, nil
}
