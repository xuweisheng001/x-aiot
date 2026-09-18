// Package cellmap 决定设备归属单元。bridge 与 bootstrap 必须使用同一函数。
package cellmap

import "hash/fnv"

// CellOf 返回 1..n。
func CellOf(sn string, n int) int {
	if n <= 1 {
		return 1
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(sn))
	return int(h.Sum32()%uint32(n)) + 1
}
