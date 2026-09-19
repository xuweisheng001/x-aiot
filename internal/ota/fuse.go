package ota

// MinFuseSamples：样本不足时不熔断（避免前几台失败就把整批熔掉）。
const MinFuseSamples = 50

// ShouldFuse 判定是否触发熔断：samples=ok+fail < minSamples 永不熔断；
// 仅当 fail/(ok+fail) **严格大于** threshold 时熔断（等于阈值不熔断）。
func ShouldFuse(ok, fail int, threshold float64, minSamples int) bool {
	if ok < 0 || fail < 0 {
		return false
	}
	samples := ok + fail
	if samples < minSamples || samples == 0 {
		return false
	}
	return float64(fail)/float64(samples) > threshold
}

// FailRatio 返回 fail/(ok+fail)，无样本时 0。
func FailRatio(ok, fail int) float64 {
	if ok+fail <= 0 {
		return 0
	}
	return float64(fail) / float64(ok+fail)
}

// DefaultMinAbsFail：绝对数熔断默认阈值。样本不足 MinFuseSamples 时比例规则永不触发，
// 但坏固件在 0.1% 档（可能只有几十台）上把每台都刷坏同样是灾难——fail 达到绝对数即熔断（预推演 INC-16）。
const DefaultMinAbsFail = 5

// ShouldFuseAbs = ShouldFuse（比例规则） || fail >= minAbsFail。minAbsFail <= 0 表示关闭绝对数规则，
// 此时行为与 ShouldFuse 完全一致。
func ShouldFuseAbs(ok, fail int, threshold float64, minSamples, minAbsFail int) bool {
	if ok < 0 || fail < 0 {
		return false
	}
	if minAbsFail > 0 && fail >= minAbsFail {
		return true
	}
	return ShouldFuse(ok, fail, threshold, minSamples)
}
