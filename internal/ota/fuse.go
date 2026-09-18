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
