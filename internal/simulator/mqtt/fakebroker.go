package mqtt

import (
	"bufio"
	"encoding/binary"
	"net"
	"sync"
)

// FakeBroker 是测试用最小 broker：CONNACK / PUBACK / SUBACK / PINGRESP，记录 PUBLISH，可向所有连接推送下行。
// 仅供单测（simulator 与 mqtt 包共用），不参与生产路径。
type FakeBroker struct {
	ln        net.Listener
	mu        sync.Mutex
	received  []Message
	conns     map[net.Conn]struct{}
	OnConnect func(clientID string)
}

func NewFakeBroker() (*FakeBroker, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	b := &FakeBroker{ln: ln, conns: map[net.Conn]struct{}{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			b.conns[c] = struct{}{}
			b.mu.Unlock()
			go b.serve(c)
		}
	}()
	return b, nil
}

func (b *FakeBroker) Addr() string { return b.ln.Addr().String() }

func (b *FakeBroker) Close() {
	_ = b.ln.Close()
	b.mu.Lock()
	for c := range b.conns {
		_ = c.Close()
	}
	b.mu.Unlock()
}

// Received 返回已收到的 PUBLISH 拷贝。
func (b *FakeBroker) Received() []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Message, len(b.received))
	copy(out, b.received)
	return out
}

// Push 向所有连接推送一条 QoS1 下行。
func (b *FakeBroker) Push(topic string, payload []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for c := range b.conns {
		_, _ = c.Write(publishPacket(topic, 1, 9, payload))
	}
}

// Connections 返回当前连接数。
func (b *FakeBroker) Connections() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.conns)
}

func (b *FakeBroker) serve(c net.Conn) {
	defer func() {
		b.mu.Lock()
		delete(b.conns, c)
		b.mu.Unlock()
		_ = c.Close()
	}()
	r := bufio.NewReader(c)
	for {
		p, err := readPacket(r)
		if err != nil {
			return
		}
		switch p.typ {
		case typeCONNECT:
			if b.OnConnect != nil && len(p.body) > 10 {
				cid, _, _ := readString(p.body[10:])
				b.OnConnect(cid)
			}
			_, _ = c.Write(frame(typeCONNACK, 0, []byte{0, 0}))
		case typePUBLISH:
			m, err := parsePublish(p)
			if err != nil {
				return
			}
			b.mu.Lock()
			b.received = append(b.received, m)
			b.mu.Unlock()
			if m.QoS > 0 {
				_, _ = c.Write(pubackPacket(m.PID))
			}
		case typeSUBSCRIBE:
			pid := binary.BigEndian.Uint16(p.body)
			_, _ = c.Write(frame(typeSUBACK, 0, append(binary.BigEndian.AppendUint16(nil, pid), 1)))
		case typePINGREQ:
			_, _ = c.Write(frame(typePINGRESP, 0, nil))
		case typeDISCONNECT:
			return
		}
	}
}
