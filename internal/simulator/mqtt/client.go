package mqtt

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Options 是连接参数。
type Options struct {
	Addr           string // host:port
	ClientID       string
	Username       string
	Password       string
	CleanSession   bool
	KeepAlive      time.Duration // 默认 60s
	ConnectTimeout time.Duration // 默认 5s
	OnMessage      func(Message) // 在读协程中回调，需快速返回
	OnPubAck       func()        // 每收到一个 PUBACK 调一次
}

// Client 是单条 MQTT 连接。连接断开后 Done() 关闭，Err() 给原因；不自动重连（重连纪律在上层）。
type Client struct {
	opts Options
	conn net.Conn

	wmu sync.Mutex

	pmu     sync.Mutex
	nextPID uint16
	waiters map[uint16]chan error // SUBACK 等待

	hmu       sync.RWMutex
	onMessage func(Message)

	done    chan struct{}
	errOnce sync.Once
	err     atomic.Value // error
	closed  atomic.Bool
}

var ErrClosed = errors.New("mqtt: connection closed")

// Dial 建 TCP 连接并完成 CONNECT/CONNACK。
func Dial(ctx context.Context, opts Options) (*Client, error) {
	if opts.KeepAlive <= 0 {
		opts.KeepAlive = 60 * time.Second
	}
	if opts.ConnectTimeout <= 0 {
		opts.ConnectTimeout = 5 * time.Second
	}
	dctx, cancel := context.WithTimeout(ctx, opts.ConnectTimeout)
	defer cancel()
	d := &net.Dialer{}
	conn, err := d.DialContext(dctx, "tcp", opts.Addr)
	if err != nil {
		return nil, err
	}
	c := &Client{opts: opts, conn: conn, waiters: map[uint16]chan error{}, done: make(chan struct{}), onMessage: opts.OnMessage}
	_ = conn.SetDeadline(time.Now().Add(opts.ConnectTimeout))
	ka := uint16(opts.KeepAlive / time.Second)
	if _, err := conn.Write(connectPacket(opts.ClientID, opts.Username, opts.Password, opts.CleanSession, ka)); err != nil {
		conn.Close()
		return nil, err
	}
	r := bufio.NewReaderSize(conn, 64*1024)
	p, err := readPacket(r)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("mqtt: waiting CONNACK: %w", err)
	}
	if p.typ != typeCONNACK || len(p.body) < 2 {
		conn.Close()
		return nil, fmt.Errorf("mqtt: expected CONNACK, got type %d", p.typ)
	}
	if rc := p.body[1]; rc != 0 {
		conn.Close()
		return nil, fmt.Errorf("mqtt: connection refused, code %d", rc)
	}
	_ = conn.SetDeadline(time.Time{})
	go c.readLoop(r)
	go c.pingLoop()
	return c, nil
}

func (c *Client) fail(err error) {
	c.errOnce.Do(func() {
		if err == nil {
			err = ErrClosed
		}
		c.err.Store(err)
		c.closed.Store(true)
		_ = c.conn.Close()
		close(c.done)
		c.pmu.Lock()
		for pid, ch := range c.waiters {
			ch <- err
			delete(c.waiters, pid)
		}
		c.pmu.Unlock()
	})
}

// SetOnMessage 替换下行回调（可在 Dial 之后设置）。
func (c *Client) SetOnMessage(h func(Message)) {
	c.hmu.Lock()
	c.onMessage = h
	c.hmu.Unlock()
}

// Done 在连接结束时关闭。
func (c *Client) Done() <-chan struct{} { return c.done }

// Err 返回结束原因（未结束为 nil）。
func (c *Client) Err() error {
	if v := c.err.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (c *Client) write(b []byte) error {
	if c.closed.Load() {
		return ErrClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := c.conn.Write(b)
	if err != nil {
		c.fail(err)
	}
	return err
}

func (c *Client) allocPID() uint16 {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	c.nextPID++
	if c.nextPID == 0 {
		c.nextPID = 1
	}
	return c.nextPID
}

// Publish 发布；QoS1 不等待 PUBACK（PUBACK 到达时回调 OnPubAck）。
func (c *Client) Publish(topic string, qos byte, payload []byte) error {
	if qos > 1 {
		qos = 1
	}
	var pid uint16
	if qos == 1 {
		pid = c.allocPID()
	}
	return c.write(publishPacket(topic, qos, pid, payload))
}

// Subscribe 订阅并等待 SUBACK。
func (c *Client) Subscribe(ctx context.Context, filter string, qos byte) error {
	pid := c.allocPID()
	ch := make(chan error, 1)
	c.pmu.Lock()
	c.waiters[pid] = ch
	c.pmu.Unlock()
	if err := c.write(subscribePacket(pid, filter, qos)); err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		c.pmu.Lock()
		delete(c.waiters, pid)
		c.pmu.Unlock()
		return ctx.Err()
	case <-c.done:
		return c.Err()
	}
}

// Disconnect 发 DISCONNECT 并关闭。
func (c *Client) Disconnect() {
	if !c.closed.Load() {
		c.wmu.Lock()
		_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
		_, _ = c.conn.Write(frame(typeDISCONNECT, 0, nil))
		c.wmu.Unlock()
	}
	c.fail(ErrClosed)
}

func (c *Client) readLoop(r *bufio.Reader) {
	for {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.opts.KeepAlive + c.opts.KeepAlive/2))
		p, err := readPacket(r)
		if err != nil {
			c.fail(err)
			return
		}
		switch p.typ {
		case typePUBLISH:
			m, err := parsePublish(p)
			if err != nil {
				c.fail(err)
				return
			}
			if m.QoS > 0 {
				_ = c.write(pubackPacket(m.PID))
			}
			c.hmu.RLock()
			h := c.onMessage
			c.hmu.RUnlock()
			if h != nil {
				h(m)
			}
		case typePUBACK:
			if c.opts.OnPubAck != nil {
				c.opts.OnPubAck()
			}
		case typeSUBACK:
			if len(p.body) < 3 {
				c.fail(errMalformed)
				return
			}
			pid := binary.BigEndian.Uint16(p.body)
			var res error
			if p.body[2] == 0x80 {
				res = errors.New("mqtt: subscribe rejected")
			}
			c.pmu.Lock()
			if ch, ok := c.waiters[pid]; ok {
				ch <- res
				delete(c.waiters, pid)
			}
			c.pmu.Unlock()
		case typePINGRESP:
		default:
			// 忽略未处理类型
		}
	}
}

func (c *Client) pingLoop() {
	t := time.NewTicker(c.opts.KeepAlive / 2)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			if err := c.write(frame(typePINGREQ, 0, nil)); err != nil {
				return
			}
		}
	}
}
