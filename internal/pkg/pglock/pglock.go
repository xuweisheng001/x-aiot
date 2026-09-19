// Package pglock 用 PostgreSQL 会话级 advisory lock 做单实例选举，
// 让「同一时刻只该有一个副本在跑」的周期任务（对账、批计算、调度器、探针）在多副本下安全。
//
// # 为什么用 PG 而不是 Redis
//
// 锁要和它保护的数据待在同一个故障域。这些任务保护的是 PG 里的行（删加工记录、
// 改配对、下发指令），锁也放 PG 就不存在「锁没了但活还能干」的裂脑：PG 挂了两边都干不了活。
// 换成 Redis 锁就是两个故障域——Redis 主从切换丢锁的那一刻，两个副本会同时删同一批用户数据。
//
// # 为什么用会话级而不是事务级
//
// 事务级（pg_advisory_xact_lock，internal/param 发布流程用的那种）随事务提交自动释放，
// 适合「一个事务内的临界区」。周期任务跨多条语句、跑几秒到几分钟，不是一个事务，
// 所以要会话级：锁跟着连接走，进程崩了连接断了锁自动没，不需要 TTL、不需要续租、
// 不会留下卡死的锁——这正是 Redis 锁最麻烦的部分。
//
// # 代价与注意事项
//
//   - 持锁副本会独占连接池里的一条连接，直到放锁。连接池至少要留 2 条。
//   - pgxpool 的 Conn.Release() 只是把连接还给池子，**不会**释放会话锁。
//     必须先 pg_advisory_unlock 再还连接，否则锁会跟着连接漂到别的查询上永不释放。
//     Guard.Release 按这个顺序做。
//   - 用 TryLock 而不是阻塞版：没抢到就跳过这一轮，下一轮再试。阻塞版会把连接堆死。
package pglock

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// 已登记的锁名。集中放这里有两个用处：一眼能看全「哪些任务是单实例的」，
// 以及用一个单测断言它们两两不撞哈希。
const (
	NameAlarmReconcile   = "alarm-reconcile"        // 安全事件与告警对账（补录告警，重复跑会重复补）
	NameSupportAudit     = "support-audit"          // 指令授权对账
	NameJobOptIn         = "job-optin"              // 反哺 opt-in 对账（会删数据）
	NameJobPurge         = "job-purge"              // 撤回同意后的删除传导
	NameAccessoryOwner   = "accessory-owner"        // 配对归属对账（会改 linkage_enabled）
	NameAccessoryFilter  = "accessory-filter"       // 滤芯寿命批
	NameAccessoryReconc  = "accessory-reconcile"    // 联动对账
	NameAccessoryOffTick = "accessory-offtimer"     // 延时关闭定时器（重复跑会重复下发）
	NameHealthBatch      = "health-batch"           // 健康度批计算（会写 consumable_health）
	NameHealthReminder   = "health-reminder"        // 提醒冷却对账
	NameFleetScheduler   = "fleet-scheduler"        // 队列调度器（技术方案 §17 明确要求 leader）
	NameFleetExpire      = "fleet-scheduler-expire" // 队列项过期清理（与调度器分锁，互不阻塞）
	NameFleetReconcile   = "fleet-reconcile"        // 课表锁对账（会写 desired）
	NameOTAStaleSweep    = "ota-stale-sweep"        // OTA stale 清扫（会改批次计数与熔断）
	NameSupportDefect    = "support-defect"         // 批次缺陷聚合
	NameProbe            = "probe"                  // 合成探针（每副本各发一发就是 N 条合成告警）
)

// Names 是全部已登记锁名，供哈希碰撞单测与运维核对。
var Names = []string{
	NameAlarmReconcile, NameSupportAudit, NameJobOptIn, NameJobPurge,
	NameAccessoryOwner, NameAccessoryFilter, NameAccessoryReconc, NameAccessoryOffTick,
	NameHealthBatch, NameHealthReminder, NameFleetScheduler, NameFleetReconcile,
	NameOTAStaleSweep, NameSupportDefect, NameProbe, NameFleetExpire,
}

// Key 纯函数：锁名 → advisory lock 的 int64 键（FNV-1a 64）。
// 在 Go 侧算而不是用 SQL 的 hashtext()，是为了让键在日志与测试里可见、可断言。
func Key(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("xtool-aiot:" + name))
	return int64(h.Sum64()) //nolint:gosec // 只要稳定且分散，符号不影响 advisory lock
}

// Guard 持有一把会话级 advisory lock。非并发安全，按「一个 Guard 一个后台循环」使用。
type Guard struct {
	pool *pgxpool.Pool
	name string
	key  int64

	mu   sync.Mutex
	conn *pgxpool.Conn // 非 nil 即代表本副本正持锁
}

func New(pool *pgxpool.Pool, name string) *Guard {
	return &Guard{pool: pool, name: name, key: Key(name)}
}

func (g *Guard) Name() string { return g.name }
func (g *Guard) Key() int64   { return g.key }

// Held 报告本副本当前是否持锁（供指标与 /healthz 展示）。
func (g *Guard) Held() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.conn != nil
}

// Ensure 确保本副本持锁：已持锁则探活（连接死了说明锁已随之释放，重新抢），
// 未持锁则尝试抢一次。返回 false 表示锁在别人手上，这一轮跳过即可，不是错误。
func (g *Guard) Ensure(ctx context.Context) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// ctx 已取消时什么都别做：这时 Ping 必然失败，但那是调用方要停了，不是连接坏了。
	// 误判成「连接坏了」会走下面的丢弃分支，把还持着锁的连接处理掉，反而制造抖动。
	if err := ctx.Err(); err != nil {
		return g.conn != nil, err
	}

	if g.conn != nil {
		err := g.conn.Ping(ctx)
		if err == nil {
			return true, nil
		}
		// ctx 在 Ping 期间被取消：这是调用方要停，不是连接坏了。保持持锁状态原样退出，
		// 让 Release 用干净的 context 发 pg_advisory_unlock——那是确定性的释放。
		// 若在这里改成关连接，锁要等 PG 后端进程退出才释放，是异步的，
		// 紧接着接管的副本会抢不到锁，看起来就像锁泄漏。
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return true, err
		}
		// 真的 Ping 不通 → 连接可疑。**不能**直接 Release 还池子：连接可能还活着、
		// 锁还在它身上，还回去就是把一把看不见的锁塞进池子，之后随机一条查询带着它。
		// 这种情况下关连接是唯一选择，异步释放也只能接受。
		slog.Warn("pglock: connection unhealthy, dropping it to force the lock release", "lock", g.name, "err", err)
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = g.conn.Conn().Close(cctx)
		cancel()
		g.conn.Release()
		g.conn = nil
	}

	c, err := g.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("pglock %s: acquire conn: %w", g.name, err)
	}
	var ok bool
	if err := c.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, g.key).Scan(&ok); err != nil {
		c.Release()
		return false, fmt.Errorf("pglock %s: try lock: %w", g.name, err)
	}
	if !ok {
		c.Release() // 没拿到锁，这条连接上没有任何锁，直接还池子
		return false, nil
	}
	g.conn = c
	return true, nil
}

// Release 先解锁再还连接。顺序不能反：pgxpool 的 Release 只还连接，
// 会话锁会留在那条连接上，跟着它漂到别的查询，直到连接被关闭才消失。
func (g *Guard) Release(ctx context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn == nil {
		return
	}
	if err := ctx.Err(); err != nil {
		// 调用方的 ctx 已经取消，解锁语句发不出去。关连接是唯一可靠的释放方式。
		slog.Warn("pglock: context done before unlock, closing the connection instead", "lock", g.name)
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = g.conn.Conn().Close(cctx)
		cancel()
		g.conn.Release()
		g.conn = nil
		return
	}
	if _, err := g.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, g.key); err != nil {
		// 解不掉就别把这条连接还回池子，关掉它——连接一关，会话锁必然释放。
		slog.Warn("pglock: unlock failed, closing the connection to force release", "lock", g.name, "err", err)
		g.conn.Conn().Close(ctx)
	}
	g.conn.Release()
	g.conn = nil
}

// Every 是给周期任务用的封装：每 interval 尝试取锁，持锁副本执行 fn，其余副本跳过。
// 阻塞到 ctx 取消，退出时释放锁（用独立 context，因为 ctx 这时已经取消了）。
//
// 首轮立即执行一次，不等第一个 tick——服务重启后不该白等一个周期。
func Every(ctx context.Context, pool *pgxpool.Pool, name string, interval time.Duration, fn func(context.Context) error) {
	g := New(pool, name)
	defer func() {
		rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		g.Release(rctx)
	}()

	t := time.NewTicker(interval)
	defer t.Stop()
	was := false
	for {
		held, err := g.Ensure(ctx)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				slog.Error("pglock: leadership check failed", "lock", name, "err", err)
			}
		case held && !was:
			slog.Info("pglock: became the singleton runner", "lock", name, "interval", interval.String())
		case !held && was:
			slog.Warn("pglock: lost leadership, standing by", "lock", name)
		}
		if err == nil {
			was = held
		}
		if held {
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				slog.Error("pglock: singleton round failed", "lock", name, "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
