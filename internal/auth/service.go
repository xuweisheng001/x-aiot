package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store 抽象 PG 查询，便于 handler 单测。
type Store interface {
	// DeviceStatus 返回设备状态；found=false 表示不存在。
	DeviceStatus(ctx context.Context, sn string) (status string, found bool, err error)
	// Cert 返回证书归属 SN 与状态；found=false 表示不存在。
	Cert(ctx context.Context, fp string) (sn, status string, found bool, err error)
}

type PGStore struct{ Pool *pgxpool.Pool }

func (s *PGStore) DeviceStatus(ctx context.Context, sn string) (string, bool, error) {
	var st string
	err := s.Pool.QueryRow(ctx, `SELECT status FROM iot_shard.device WHERE sn=$1`, sn).Scan(&st)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("device status: %w", err)
	}
	return st, true, nil
}

func (s *PGStore) Cert(ctx context.Context, fp string) (string, string, bool, error) {
	var sn, st string
	err := s.Pool.QueryRow(ctx, `SELECT sn, status FROM iot_shard.device_cert WHERE cert_fp=$1`, fp).Scan(&sn, &st)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("cert: %w", err)
	}
	return strings.TrimSpace(sn), st, true, nil
}

// Authenticate：SN 格式合法 && 设备 status='activated' && (cert_fp 为空 || 证书 active 且归属该 SN)。
// 出错时 fail-closed（deny）并返回 err 供记录。
func Authenticate(ctx context.Context, st Store, clientID, certFP string) (bool, string, error) {
	if !ValidSN(clientID) {
		return false, "invalid sn", nil
	}
	status, found, err := st.DeviceStatus(ctx, clientID)
	if err != nil {
		return false, "store error", err
	}
	if !found {
		return false, "device not found", nil
	}
	if status != "activated" {
		return false, "device status " + status, nil
	}
	if certFP = strings.TrimSpace(certFP); certFP != "" {
		sn, cst, found, err := st.Cert(ctx, certFP)
		if err != nil {
			return false, "store error", err
		}
		if !found {
			return false, "cert not found", nil
		}
		if cst != "active" {
			return false, "cert status " + cst, nil
		}
		if sn != clientID {
			return false, "cert sn mismatch", nil
		}
	}
	return true, "", nil
}

// ---------- fail-open 兜底（INC-02） ----------

// Authenticator 在 Authenticate 之上加进程内缓存与可选的 fail-open：
//   - 认证成功 → 缓存 (clientID|certFP) 的成功时间；
//   - 仅当 Store 出错（PG 不可达等）且 FailOpen 开启且缓存在 TTL 内命中 → allow，reason "cache-failopen"；
//   - 明确拒绝（设备不存在 / 未激活 / 证书不匹配 / 吊销）不受缓存影响：fail-open 只覆盖「查不到答案」，不覆盖「答案是拒绝」。
//
// 开启 FailOpen 属于有意的降级，必须登记并在依赖恢复后关闭（docs/incident-premortem-bl1.md §10）。
type Authenticator struct {
	Store    Store
	Cache    *AuthCache
	FailOpen bool
	TTL      time.Duration
	Now      func() time.Time
	M        *Metrics
}

const (
	DefaultCacheTTL  = 24 * time.Hour
	ReasonFailOpen   = "cache-failopen"
	ReasonStoreError = "store error"
)

func NewAuthenticator(st Store, failOpen bool, ttl time.Duration, m *Metrics) *Authenticator {
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}
	if m == nil {
		m = NewMetrics()
	}
	return &Authenticator{Store: st, Cache: NewAuthCache(), FailOpen: failOpen, TTL: ttl, Now: time.Now, M: m}
}

// Authenticate 语义见类型注释。返回的 err 仅在 Store 出错时非 nil（无论最终 allow 还是 deny），供调用方记录。
func (a *Authenticator) Authenticate(ctx context.Context, clientID, certFP string) (bool, string, error) {
	now := a.now()
	key := CacheKey(clientID, strings.TrimSpace(certFP))
	ok, reason, err := Authenticate(ctx, a.Store, clientID, certFP)
	if err != nil {
		a.M.Inc(MStoreError)
		if a.FailOpen && a.Cache != nil && a.Cache.Recent(key, now, a.TTL) {
			a.M.Inc(MFailOpenAllowed)
			a.M.Inc(MAuthAllow)
			return true, ReasonFailOpen, err
		}
		a.M.Inc(MAuthDeny)
		a.M.IncDenyReason(ReasonStoreError)
		return false, ReasonStoreError, err
	}
	if ok {
		if a.Cache != nil {
			a.Cache.Remember(key, now)
			a.M.Set(MCacheSize, int64(a.Cache.Len()))
		}
		a.M.Inc(MAuthAllow)
		return true, "", nil
	}
	a.M.Inc(MAuthDeny)
	a.M.IncDenyReason(reason)
	return false, reason, nil
}

func (a *Authenticator) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

// RunSweeper 每 interval 清理一次过期缓存，直到 ctx 取消。
func (a *Authenticator) RunSweeper(ctx context.Context, interval time.Duration) {
	if a.Cache == nil {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.Cache.Sweep(a.now(), a.TTL)
			a.M.Set(MCacheSize, int64(a.Cache.Len()))
		}
	}
}
