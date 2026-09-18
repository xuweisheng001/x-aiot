package cf001

import (
	"fmt"
	"strings"
)

// SNLen：{pk 4}{yymmdd 6}{产线 2}{当日流水 9}{校验 2} = 23。
const SNLen = 23

// Checksum 取字符串全部字节 ASCII 求和 % 256，输出两位大写十六进制。
// 调用方传前 21 位；传入其它长度也按同样算法计算（纯函数，不做长度校验）。
func Checksum(s21 string) string {
	sum := 0
	for i := 0; i < len(s21); i++ {
		sum += int(s21[i])
	}
	return fmt.Sprintf("%02X", sum%256)
}

// Prefix4 取 product_key 前 4 个字符并大写，不足 4 位用 '_' 右补。
func Prefix4(productKey string) string {
	p := strings.ToUpper(productKey)
	if len(p) > 4 {
		p = p[:4]
	}
	for len(p) < 4 {
		p += "_"
	}
	return p
}

// NormalizeLine 把产线号规整为两位数字字符串；空 → "01"。
func NormalizeLine(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return "01"
	}
	if len(line) == 1 {
		return "0" + line
	}
	if len(line) > 2 {
		return line[len(line)-2:]
	}
	return line
}

// BuildSN 拼装 23 位 SN。seq 取 10^9 模（9 位流水），prefix4/line/yymmdd 先规整。
func BuildSN(prefix4, yymmdd, line string, seq int64) string {
	if len(yymmdd) > 6 {
		yymmdd = yymmdd[:6]
	}
	for len(yymmdd) < 6 {
		yymmdd = "0" + yymmdd
	}
	if seq < 0 {
		seq = -seq
	}
	body := Prefix4(prefix4) + yymmdd + NormalizeLine(line) + fmt.Sprintf("%09d", seq%1_000_000_000)
	return body + Checksum(body)
}

// ValidSN 校验长度与校验码是否自洽。
func ValidSN(sn string) bool {
	if len(sn) != SNLen {
		return false
	}
	return Checksum(sn[:21]) == sn[21:]
}
