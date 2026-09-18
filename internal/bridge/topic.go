// Package bridge 把 EMQX 共享订阅收到的上行消息封装成信封并发布到 JetStream。
package bridge

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// ErrBadTopic 表示 topic 不符合 up/{pk}/{sn}/{kind}。
var ErrBadTopic = errors.New("bridge: bad topic")

// Topic 是解析后的上行 topic。
type Topic struct {
	PK   string
	SN   string
	Kind envelope.Kind
}

var validKinds = map[envelope.Kind]bool{
	envelope.KindTelemetry: true, envelope.KindEvent: true,
	envelope.KindCmdAck: true, envelope.KindOTAProgress: true,
}

// ParseTopic 解析 up/{pk}/{sn}/{kind}（纯函数）。
// 兼容带 "$share/<group>/" 前缀的形态（个别 broker 会把共享订阅前缀带回来）。
func ParseTopic(topic string) (Topic, error) {
	if strings.HasPrefix(topic, "$share/") {
		rest := topic[len("$share/"):]
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			return Topic{}, ErrBadTopic
		}
		topic = rest[i+1:]
	}
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != "up" {
		return Topic{}, ErrBadTopic
	}
	for _, p := range parts[1:] {
		if p == "" || strings.ContainsAny(p, "+#") {
			return Topic{}, ErrBadTopic
		}
	}
	k := envelope.Kind(parts[3])
	if !validKinds[k] {
		return Topic{}, ErrBadTopic
	}
	return Topic{PK: parts[1], SN: parts[2], Kind: k}, nil
}

// ExtractSeq 从 payload JSON 中取 "seq"；缺失或非法返回 0（不阻断转发，pipeline 负责校验）。
func ExtractSeq(payload []byte) int64 {
	var p struct {
		Seq *json.Number `json:"seq"`
	}
	if err := json.Unmarshal(payload, &p); err != nil || p.Seq == nil {
		return 0
	}
	n, err := p.Seq.Int64()
	if err != nil {
		if f, ferr := p.Seq.Float64(); ferr == nil {
			return int64(f)
		}
		return 0
	}
	return n
}
