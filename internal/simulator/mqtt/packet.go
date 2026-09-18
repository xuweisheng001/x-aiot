// Package mqtt 是模拟器自用的最小 MQTT 3.1.1 客户端（仅标准库）：
// CONNECT/CONNACK、PUBLISH(QoS0/1)/PUBACK、SUBSCRIBE/SUBACK、PINGREQ/PINGRESP、DISCONNECT。
// go.mod 未列 paho，规格要求只用已列依赖 + 标准库，故自带实现。
package mqtt

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	typeCONNECT     byte = 1
	typeCONNACK     byte = 2
	typePUBLISH     byte = 3
	typePUBACK      byte = 4
	typeSUBSCRIBE   byte = 8
	typeSUBACK      byte = 9
	typeUNSUBSCRIBE byte = 10
	typePINGREQ     byte = 12
	typePINGRESP    byte = 13
	typeDISCONNECT  byte = 14
)

const maxPacketSize = 4 * 1024 * 1024

var errMalformed = errors.New("mqtt: malformed packet")

// packet 是解出的一个控制报文。
type packet struct {
	typ   byte
	flags byte
	body  []byte
}

func encodeRemainingLength(n int) []byte {
	var out []byte
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			return out
		}
	}
}

func encodeString(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

func readString(b []byte) (string, []byte, error) {
	if len(b) < 2 {
		return "", nil, errMalformed
	}
	n := int(binary.BigEndian.Uint16(b))
	if len(b) < 2+n {
		return "", nil, errMalformed
	}
	return string(b[2 : 2+n]), b[2+n:], nil
}

// frame 组装固定头 + 报文体。
func frame(typ, flags byte, body []byte) []byte {
	out := make([]byte, 0, 1+4+len(body))
	out = append(out, typ<<4|flags&0x0f)
	out = append(out, encodeRemainingLength(len(body))...)
	return append(out, body...)
}

func readPacket(r *bufio.Reader) (packet, error) {
	h, err := r.ReadByte()
	if err != nil {
		return packet{}, err
	}
	n, mult := 0, 1
	for i := 0; i < 4; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return packet{}, err
		}
		n += int(b&0x7f) * mult
		if b&0x80 == 0 {
			break
		}
		mult *= 128
		if i == 3 {
			return packet{}, errMalformed
		}
	}
	if n > maxPacketSize {
		return packet{}, fmt.Errorf("mqtt: packet too large: %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return packet{}, err
	}
	return packet{typ: h >> 4, flags: h & 0x0f, body: body}, nil
}

// connectPacket 编码 CONNECT。
func connectPacket(clientID, username, password string, cleanSession bool, keepAliveSec uint16) []byte {
	var body []byte
	body = append(body, encodeString("MQTT")...)
	body = append(body, 4) // protocol level 3.1.1
	var flags byte
	if cleanSession {
		flags |= 0x02
	}
	if username != "" {
		flags |= 0x80
		if password != "" {
			flags |= 0x40
		}
	}
	body = append(body, flags)
	body = binary.BigEndian.AppendUint16(body, keepAliveSec)
	body = append(body, encodeString(clientID)...)
	if username != "" {
		body = append(body, encodeString(username)...)
		if password != "" {
			body = append(body, encodeString(password)...)
		}
	}
	return frame(typeCONNECT, 0, body)
}

func publishPacket(topic string, qos byte, pid uint16, payload []byte) []byte {
	body := encodeString(topic)
	if qos > 0 {
		body = binary.BigEndian.AppendUint16(body, pid)
	}
	body = append(body, payload...)
	return frame(typePUBLISH, qos<<1, body)
}

func pubackPacket(pid uint16) []byte {
	return frame(typePUBACK, 0, binary.BigEndian.AppendUint16(nil, pid))
}

func subscribePacket(pid uint16, filter string, qos byte) []byte {
	body := binary.BigEndian.AppendUint16(nil, pid)
	body = append(body, encodeString(filter)...)
	body = append(body, qos)
	return frame(typeSUBSCRIBE, 0x02, body)
}

// Message 是收到的 PUBLISH。
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	PID     uint16
}

func parsePublish(p packet) (Message, error) {
	topic, rest, err := readString(p.body)
	if err != nil {
		return Message{}, err
	}
	m := Message{Topic: topic, QoS: (p.flags >> 1) & 0x03}
	if m.QoS > 0 {
		if len(rest) < 2 {
			return Message{}, errMalformed
		}
		m.PID = binary.BigEndian.Uint16(rest)
		rest = rest[2:]
	}
	m.Payload = rest
	return m, nil
}
