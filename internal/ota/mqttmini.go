package ota

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"time"
)

// Publisher 是下行发布抽象；生产可换 paho 客户端，测试用内存实现。
type Publisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
}

// MiniMQTT 是仅用标准库的 MQTT 3.1.1 QoS0 发布客户端（CONNECT/PUBLISH/PINGREQ）。
// 之所以不引 paho：go.mod 未列该依赖且本任务不得改 go.mod；接口一致，随时可替换。
type MiniMQTT struct {
	addr, clientID, user, pass string
	mu                         sync.Mutex
	conn                       net.Conn
	lastIO                     time.Time
}

// NewMiniMQTT 解析 tcp://host:port。
func NewMiniMQTT(rawURL, clientID, user, pass string) (*MiniMQTT, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "tcp" && u.Scheme != "mqtt" {
		return nil, fmt.Errorf("mqtt: unsupported scheme %q", u.Scheme)
	}
	return &MiniMQTT{addr: u.Host, clientID: clientID, user: user, pass: pass}, nil
}

func encLen(n int) []byte {
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

func lenStr(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

func (m *MiniMQTT) connectLocked(ctx context.Context) error {
	d := net.Dialer{Timeout: 5 * time.Second}
	c, err := d.DialContext(ctx, "tcp", m.addr)
	if err != nil {
		return fmt.Errorf("mqtt dial: %w", err)
	}
	var body []byte
	body = append(body, lenStr("MQTT")...)
	body = append(body, 4) // protocol level 3.1.1
	flags := byte(0x02)    // clean session
	if m.user != "" {
		flags |= 0x80
		if m.pass != "" {
			flags |= 0x40
		}
	}
	body = append(body, flags)
	body = append(body, 0, 60) // keepalive 60s
	body = append(body, lenStr(m.clientID)...)
	if m.user != "" {
		body = append(body, lenStr(m.user)...)
		if m.pass != "" {
			body = append(body, lenStr(m.pass)...)
		}
	}
	pkt := append([]byte{0x10}, encLen(len(body))...)
	pkt = append(pkt, body...)
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write(pkt); err != nil {
		c.Close()
		return fmt.Errorf("mqtt connect write: %w", err)
	}
	ack := make([]byte, 4)
	if _, err := io.ReadFull(c, ack); err != nil {
		c.Close()
		return fmt.Errorf("mqtt connack read: %w", err)
	}
	if ack[0] != 0x20 || ack[3] != 0 {
		c.Close()
		return fmt.Errorf("mqtt connack refused: code %d", ack[3])
	}
	_ = c.SetDeadline(time.Time{})
	m.conn = c
	m.lastIO = time.Now()
	return nil
}

// Publish 发布 QoS0 消息；连接断开时自动重连一次。
func (m *MiniMQTT) Publish(ctx context.Context, topic string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn == nil {
		if err := m.connectLocked(ctx); err != nil {
			return err
		}
	}
	var body []byte
	body = append(body, lenStr(topic)...)
	body = append(body, payload...)
	pkt := append([]byte{0x30}, encLen(len(body))...)
	pkt = append(pkt, body...)
	_ = m.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := m.conn.Write(pkt); err != nil {
		m.conn.Close()
		m.conn = nil
		if err2 := m.connectLocked(ctx); err2 != nil {
			return errors.Join(err, err2)
		}
		_ = m.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := m.conn.Write(pkt); err != nil {
			m.conn.Close()
			m.conn = nil
			return fmt.Errorf("mqtt publish: %w", err)
		}
	}
	m.lastIO = time.Now()
	return nil
}

// KeepAlive 每 30s 发 PINGREQ（并丢弃服务端回包），直到 ctx 取消。
func (m *MiniMQTT) KeepAlive(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.Close()
			return
		case <-t.C:
			m.mu.Lock()
			if m.conn != nil {
				_ = m.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := m.conn.Write([]byte{0xC0, 0x00}); err != nil {
					m.conn.Close()
					m.conn = nil
				} else {
					// 读掉 PINGRESP（以及任何服务端下发，QoS0 不订阅所以只会是 PINGRESP）
					_ = m.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
					buf := make([]byte, 64)
					_, _ = m.conn.Read(buf)
					_ = m.conn.SetReadDeadline(time.Time{})
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *MiniMQTT) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		_, _ = m.conn.Write([]byte{0xE0, 0x00}) // DISCONNECT
		m.conn.Close()
		m.conn = nil
	}
}

// MemPublisher 是测试用内存发布器。
type MemPublisher struct {
	mu   sync.Mutex
	Msgs []struct{ Topic, Payload string }
}

func (p *MemPublisher) Publish(_ context.Context, topic string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Msgs = append(p.Msgs, struct{ Topic, Payload string }{topic, string(payload)})
	return nil
}
