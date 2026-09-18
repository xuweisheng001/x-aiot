package bootstrap

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// CellCache 把 cell 表镜像在内存中，定时刷新；写操作后可 Refresh 立即生效。
type CellCache struct {
	repo     CellRepo
	interval time.Duration
	mu       sync.RWMutex
	cells    map[int]Cell
}

func NewCellCache(repo CellRepo, interval time.Duration) *CellCache {
	return &CellCache{repo: repo, interval: interval, cells: map[int]Cell{}}
}

// Refresh 从库重新加载一次。
func (c *CellCache) Refresh(ctx context.Context) error {
	list, err := c.repo.ListCells(ctx)
	if err != nil {
		return err
	}
	m := make(map[int]Cell, len(list))
	for _, cl := range list {
		m[cl.ID] = cl
	}
	c.mu.Lock()
	c.cells = m
	c.mu.Unlock()
	return nil
}

// Run 阻塞刷新直到 ctx 结束。
func (c *CellCache) Run(ctx context.Context) {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := c.Refresh(rctx); err != nil {
				slog.Warn("cell cache refresh failed", "err", err)
			}
			cancel()
		}
	}
}

// Snapshot 返回当前镜像的拷贝。
func (c *CellCache) Snapshot() map[int]Cell {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m := make(map[int]Cell, len(c.cells))
	for k, v := range c.cells {
		m[k] = v
	}
	return m
}

// SetStatus 本地立即更新（写库成功后调用，避免等下一轮刷新）。
func (c *CellCache) SetStatus(id int, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.cells[id]; ok {
		cl.Status = status
		c.cells[id] = cl
	}
}
