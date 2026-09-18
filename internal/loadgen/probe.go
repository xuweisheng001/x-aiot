// Package loadgen 是压测剧本：conn（限速建连 + RST 假成功识别）、storm（零错峰风暴）、flood（泄洪法测管道吞吐并对账）。
package loadgen

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Outcome 是一次连接探测的结论。
type Outcome int

const (
	OutcomeDialFail Outcome = iota
	OutcomeRST              // Dial 成功但随后被 RST（accept 后拒绝——经典假成功）
	OutcomeAlive            // 1 字节读超时/有数据：连接真活着
)

func (o Outcome) String() string {
	switch o {
	case OutcomeRST:
		return "rst"
	case OutcomeAlive:
		return "alive"
	}
	return "dial_fail"
}

// Classify 把探测读的结果分类：nil 或超时 = alive，其它读错误 = rst。纯函数。
func Classify(readErr error) Outcome {
	if readErr == nil {
		return OutcomeAlive
	}
	var ne net.Error
	if errors.As(readErr, &ne) && ne.Timeout() {
		return OutcomeAlive
	}
	return OutcomeRST
}

// Probe 在已建立的连接上等待 wait 后做 1 字节读（readTimeout 截止）。
func Probe(conn net.Conn, wait, readTimeout time.Duration) Outcome {
	time.Sleep(wait)
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	_, err := conn.Read(make([]byte, 1))
	_ = conn.SetReadDeadline(time.Time{})
	return Classify(err)
}

// DialProbe = Dial + Probe；alive 时返回连接（由调用方保活/关闭）。
func DialProbe(target string, dialTimeout, wait, readTimeout time.Duration) (Outcome, net.Conn) {
	c, err := net.DialTimeout("tcp", target, dialTimeout)
	if err != nil {
		return OutcomeDialFail, nil
	}
	o := Probe(c, wait, readTimeout)
	if o != OutcomeAlive {
		_ = c.Close()
		return o, nil
	}
	return o, c
}

// CSVWriter 是带互斥的简单 CSV 写出。
type CSVWriter struct {
	mu sync.Mutex
	w  *csv.Writer
}

func NewCSVWriter(w io.Writer, header ...string) *CSVWriter {
	cw := &CSVWriter{w: csv.NewWriter(w)}
	if len(header) > 0 {
		_ = cw.w.Write(header)
		cw.w.Flush()
	}
	return cw
}

func (c *CSVWriter) Row(vals ...int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rec := make([]string, len(vals))
	for i, v := range vals {
		rec[i] = strconv.FormatInt(v, 10)
	}
	_ = c.w.Write(rec)
	c.w.Flush()
}

// OpenOut 打开 -out 文件（自动建目录）；空路径返回 io.Discard。
func OpenOut(path string) (io.WriteCloser, error) {
	if path == "" {
		return nopCloser{io.Discard}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return f, nil
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

// connSet 保活连接集合，结束时统一关闭。
type connSet struct {
	mu    sync.Mutex
	conns []net.Conn
}

func (s *connSet) add(c net.Conn) {
	s.mu.Lock()
	s.conns = append(s.conns, c)
	s.mu.Unlock()
}

func (s *connSet) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
}
