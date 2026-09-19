package cf001

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// 认证载荷的重放防护（INC-22 / INC-23）：timestamp 新鲜度 + nonce 一次性。
// 生产必须开启；IOT_CF001_SKIP_FRESHNESS=true 仅供开发联调。

var (
	ErrStale       = errors.New("stale timestamp")   // 包装为 ErrBadParam → 400 10001
	ErrReplay      = errors.New("replay")            // 包装为 ErrBadParam → 400 10001
	ErrUnavailable = errors.New("nonce store error") // 503：安全校验做不了就拒绝（fail-closed）
)

const DefaultFreshnessWindow = 5 * time.Minute

// CheckFreshness 校验 ts（Unix 秒或毫秒，按位数判断：≤ 10 位秒，11 到 13 位毫秒）与 now 的偏差不超过 window。
// 未来时间同样受限（设备时钟快也不放行）。非数字 / 位数异常报错。
func CheckFreshness(ts string, now time.Time, window time.Duration) error {
	ts = strings.TrimSpace(ts)
	if ts == "" {
		return fmt.Errorf("%w: empty timestamp", ErrStale)
	}
	n, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || n <= 0 {
		return fmt.Errorf("%w: timestamp not a positive integer", ErrStale)
	}
	var t time.Time
	switch d := len(strings.TrimLeft(ts, "0")); {
	case d <= 10:
		t = time.Unix(n, 0)
	case d <= 13:
		t = time.UnixMilli(n)
	default:
		return fmt.Errorf("%w: timestamp has %d digits (want seconds or milliseconds)", ErrStale, d)
	}
	if window <= 0 {
		window = DefaultFreshnessWindow
	}
	diff := now.Sub(t)
	if diff < 0 {
		diff = -diff
	}
	if diff > window {
		return fmt.Errorf("%w: |now - ts| = %s exceeds %s", ErrStale, diff.Truncate(time.Second), window)
	}
	return nil
}

// NonceStore 记录已见 nonce。Seen 返回 dup=true 表示该 nonce 已被使用过。
type NonceStore interface {
	Seen(ctx context.Context, nonce string, ttl time.Duration) (dup bool, err error)
}

// RedisNonceStore：SETNX cf001:nonce:{nonce} EX ttl。
type RedisNonceStore struct{ RDB *redis.Client }

func NonceKey(nonce string) string { return "cf001:nonce:" + nonce }

func (r *RedisNonceStore) Seen(ctx context.Context, nonce string, ttl time.Duration) (bool, error) {
	ok, err := r.RDB.SetNX(ctx, NonceKey(nonce), 1, ttl).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil
}

// MemNonceStore 是进程内实现（单副本开发 / 测试用）。
type MemNonceStore struct {
	mu   chan struct{}
	seen map[string]time.Time
	Now  func() time.Time
}

func NewMemNonceStore() *MemNonceStore {
	return &MemNonceStore{mu: make(chan struct{}, 1), seen: map[string]time.Time{}, Now: time.Now}
}

func (m *MemNonceStore) Seen(_ context.Context, nonce string, ttl time.Duration) (bool, error) {
	m.mu <- struct{}{}
	defer func() { <-m.mu }()
	now := m.Now()
	for k, exp := range m.seen {
		if now.After(exp) {
			delete(m.seen, k)
		}
	}
	if _, ok := m.seen[nonce]; ok {
		return true, nil
	}
	m.seen[nonce] = now.Add(ttl)
	return false, nil
}

// Counters 是 cf001-svc 的进程内文本计数器（GET /metrics）。
type Counters struct {
	Signed, Existing, NoQuota, Stale, Replay, NonceStoreErr, SkippedFreshness atomic.Int64
}

func (c *Counters) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "sign_ok %d\nsign_existing %d\nsign_no_quota %d\nsign_stale %d\nsign_replay %d\nsign_nonce_store_err %d\nsign_freshness_skipped %d\n",
		c.Signed.Load(), c.Existing.Load(), c.NoQuota.Load(), c.Stale.Load(), c.Replay.Load(), c.NonceStoreErr.Load(), c.SkippedFreshness.Load())
}

// checkReplay 在解密之后、任何数据库操作之前执行：新鲜度 → nonce 去重。
// SkipFreshness 为真时整体跳过（计数）。NonceStore 出错 → ErrUnavailable（fail-closed）。
func (s *Service) checkReplay(ctx context.Context, p Payload) error {
	c := s.C
	if c == nil {
		c = &Counters{} // 未注入计数器时丢弃计数，不 panic
	}
	if s.SkipFreshness {
		c.SkippedFreshness.Add(1)
		return nil
	}
	window := s.FreshnessWindow
	if window <= 0 {
		window = DefaultFreshnessWindow
	}
	if err := CheckFreshness(strconv.FormatInt(p.Timestamp, 10), s.Now(), window); err != nil {
		c.Stale.Add(1)
		return fmt.Errorf("%w: %w", ErrBadParam, err)
	}
	if s.Nonces == nil {
		c.NonceStoreErr.Add(1)
		return fmt.Errorf("%w: nonce store not configured", ErrUnavailable)
	}
	dup, err := s.Nonces.Seen(ctx, p.Nonce, 2*window)
	if err != nil {
		c.NonceStoreErr.Add(1)
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if dup {
		c.Replay.Add(1)
		return fmt.Errorf("%w: %w: nonce already used", ErrBadParam, ErrReplay)
	}
	return nil
}
