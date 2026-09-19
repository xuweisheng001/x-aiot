// Package ota 实现固件登记、灰度批次圈选/下发、进度回流与熔断。
package ota

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"strconv"
)

// Stages 是灰度档位，累进包含：0.1% ⊂ 1% ⊂ 10% ⊂ 50% ⊂ 100%。
var Stages = []string{"0.1", "1", "10", "50", "100"}

// StagePct 把档位字符串转成百分比；非法档位返回 error。
func StagePct(stage string) (float64, error) {
	for _, s := range Stages {
		if s == stage {
			f, _ := strconv.ParseFloat(s, 64)
			return f, nil
		}
	}
	return 0, fmt.Errorf("invalid stage %q (want one of %v)", stage, Stages)
}

// NextStage 返回下一档；已是 100 时 ok=false。
func NextStage(stage string) (string, bool) {
	for i, s := range Stages {
		if s == stage && i+1 < len(Stages) {
			return Stages[i+1], true
		}
	}
	return "", false
}

// Bucket 返回 md5(sn) 低 32 位 / 2^32 ∈ [0,1)，对同一 SN 稳定。
func Bucket(sn string) float64 {
	sum := md5.Sum([]byte(sn))
	low := binary.BigEndian.Uint32(sum[12:16])
	return float64(low) / 4294967296.0
}

// InStage 报告 sn 是否落入 stagePct(%) 抽样。因为比较的是同一个 Bucket 值与递增阈值，
// 档位天然累进包含：InStage(sn, a) && a<=b ⇒ InStage(sn, b)。
func InStage(sn string, stagePct float64) bool {
	if stagePct >= 100 {
		return true
	}
	if stagePct <= 0 {
		return false
	}
	return Bucket(sn) < stagePct/100
}

// FirstStage 是任何固件的第一个允许档位。
const FirstStage = "0.1"

// ExpectedNextStage 返回某固件下一批次唯一允许的档位：没有任何批次时只能是 FirstStage；
// 否则必须是最新批次的 NextStage。latest 已是 100 时 ok=false（无法再建）。
func ExpectedNextStage(latest string, exists bool) (string, bool) {
	if !exists {
		return FirstStage, true
	}
	return NextStage(latest)
}
