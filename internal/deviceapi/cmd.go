package deviceapi

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/xtool/xtool-aiot/internal/pkg/httpx"
)

// Decision 是指令白名单判定结果。
type Decision struct {
	Allowed    bool
	HTTPStatus int
	BizCode    int
	Msg        string
}

// job_start 属白名单，但另有一道来源 + 空闲判定（DecideJobStart，BL5 §17.3）。
var whitelist = map[string]bool{"pause": true, "stop": true, "self_check": true, ActionJobStart: true}

// DecideCmd 纯函数：白名单 pause/stop/self_check；remote_restart → 403/10003；其他 → 400/10001。
func DecideCmd(action string) Decision {
	switch {
	case whitelist[action]:
		return Decision{Allowed: true, HTTPStatus: http.StatusOK, BizCode: httpx.CodeOK}
	case action == "remote_restart":
		return Decision{HTTPStatus: http.StatusForbidden, BizCode: httpx.CodeDenied, Msg: "remote_restart is not allowed"}
	case action == "":
		return Decision{HTTPStatus: http.StatusBadRequest, BizCode: httpx.CodeBadParam, Msg: "action required"}
	default:
		return Decision{HTTPStatus: http.StatusBadRequest, BizCode: httpx.CodeBadParam, Msg: "unknown action: " + action}
	}
}

// NewUUIDv4 用 crypto/rand 生成 RFC 4122 v4 UUID 字符串。
func NewUUIDv4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	var dst [36]byte
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], b[10:16])
	return string(dst[:])
}
