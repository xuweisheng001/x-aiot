package bridge

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/xtool/xtool-aiot/internal/pkg/envelope"
)

// DiskBuffer 是 JetStream 不可用时非 telemetry 信封的本地 JSON Lines 缓冲。
// Replay 时先把主文件原子改名为 .replay，新到的追加继续写主文件；失败的行重新追加回主文件。
type DiskBuffer struct {
	path string
	mu   sync.Mutex
}

func NewDiskBuffer(path string) (*DiskBuffer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("buffer dir: %w", err)
	}
	return &DiskBuffer{path: path}, nil
}

func (d *DiskBuffer) Path() string { return d.path }

// Append 追加一行 JSON。
func (d *DiskBuffer) Append(env *envelope.Envelope) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appendLines([][]byte{env.Marshal()})
}

func (d *DiskBuffer) appendLines(lines [][]byte) error {
	f, err := os.OpenFile(d.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, l := range lines {
		w.Write(l)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Len 返回主文件当前行数（用于指标与测试）。
func (d *DiskBuffer) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := os.Open(d.path)
	if err != nil {
		return 0
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) > 0 {
			n++
		}
	}
	return n
}

// ReplayResult 是一次 Replay 的统计。
type ReplayResult struct {
	Replayed int // 成功重发
	Requeued int // 失败后重新写回缓冲
	Skipped  int // 非法行丢弃
}

// Replay 重发缓冲内容。publish 首次失败后停止尝试，余下行按原顺序写回主文件。
func (d *DiskBuffer) Replay(ctx context.Context, publish func(context.Context, *envelope.Envelope) error) (ReplayResult, error) {
	var res ReplayResult
	tmp := d.path + ".replay"

	d.mu.Lock()
	if _, err := os.Stat(tmp); err != nil { // 无残留的半途 replay 文件 → 轮换主文件
		st, err := os.Stat(d.path)
		if err != nil || st.Size() == 0 {
			d.mu.Unlock()
			return res, nil
		}
		if err := os.Rename(d.path, tmp); err != nil {
			d.mu.Unlock()
			return res, fmt.Errorf("buffer rotate: %w", err)
		}
	}
	d.mu.Unlock()

	f, err := os.Open(tmp)
	if err != nil {
		return res, fmt.Errorf("buffer open: %w", err)
	}
	var requeue [][]byte
	var firstErr error
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		cp := append([]byte(nil), line...)
		if firstErr != nil || ctx.Err() != nil {
			requeue = append(requeue, cp)
			continue
		}
		env, uerr := envelope.Unmarshal(cp)
		if uerr != nil {
			res.Skipped++
			continue
		}
		if perr := publish(ctx, env); perr != nil {
			firstErr = perr
			requeue = append(requeue, cp)
			continue
		}
		res.Replayed++
	}
	scanErr := sc.Err()
	f.Close()

	d.mu.Lock()
	defer d.mu.Unlock()
	if len(requeue) > 0 {
		if err := d.appendLines(requeue); err != nil {
			return res, fmt.Errorf("buffer requeue: %w", err) // 保留 .replay，下次继续
		}
		res.Requeued = len(requeue)
	}
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return res, err
	}
	if scanErr != nil {
		return res, scanErr
	}
	return res, firstErr
}
