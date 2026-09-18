// Package backoff 是设备端重连纪律：min(2^n·base, 15min) + rand(0,30s)。
// 这是全方案唯一不可事后补救的部分，必须在第一版固件固化（技术方案 §4.2）。
package backoff

import (
	"math/rand/v2"
	"time"
)

const (
	MaxInterval = 15 * time.Minute
	MaxJitter   = 30 * time.Second
)

// Next 返回第 n 次（从 0 起）重连前应等待的时长。
func Next(n int, base time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	d := base
	for i := 0; i < n; i++ {
		d *= 2
		if d >= MaxInterval {
			d = MaxInterval
			break
		}
	}
	if d > MaxInterval {
		d = MaxInterval
	}
	return d + time.Duration(rand.Int64N(int64(MaxJitter)))
}
