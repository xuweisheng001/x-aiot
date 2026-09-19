package pglock

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtool/xtool-aiot/internal/pkg/config"
)

func TestKeyIsStableAndDistinct(t *testing.T) {
	// 稳定：同名必须永远同键，否则升级一次就换了把锁，新老副本会同时跑
	if Key(NameProbe) != Key(NameProbe) {
		t.Fatal("Key must be deterministic")
	}
	if Key("a") == Key("b") {
		t.Fatal("different names must not share a key")
	}
	// 互不碰撞：撞了就是两个无关任务互相阻塞，而且极难排查
	seen := map[int64]string{}
	for _, n := range Names {
		k := Key(n)
		if prev, ok := seen[k]; ok {
			t.Fatalf("lock key collision: %q and %q both hash to %d", prev, n, k)
		}
		seen[k] = n
	}
	if len(seen) != len(Names) {
		t.Fatalf("registered %d names but only %d distinct keys", len(Names), len(seen))
	}
	// 命名空间前缀：同一个库被别的系统共用时，裸名字很容易撞上（"probe" 谁都可能用），
	// 所以键要对 "xtool-aiot:<name>" 取哈希而不是裸名字
	bare := fnv.New64a()
	_, _ = bare.Write([]byte(NameProbe))
	if Key(NameProbe) == int64(bare.Sum64()) {
		t.Fatal("keys must be namespaced, not hashed from the bare name")
	}
}

func TestNamesAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, n := range Names {
		if n == "" {
			t.Fatal("empty lock name")
		}
		if seen[n] {
			t.Fatalf("duplicate lock name %q", n)
		}
		seen[n] = true
	}
}

// ===== 集成：需要真实 PG =====

func itPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	if !config.Integration() {
		t.Skip("IOT_IT not set")
	}
	ctx := context.Background()
	p := config.MustPG(ctx)
	t.Cleanup(p.Close)
	return p, ctx
}

// 两个副本抢同一把锁：只有一个拿到；持锁方放手后另一个才能拿到。
func TestIntegration_OnlyOneHolder(t *testing.T) {
	pool, ctx := itPool(t)
	name := "it-lock-" + time.Now().Format("150405.000000")

	a, b := New(pool, name), New(pool, name)
	okA, err := a.Ensure(ctx)
	if err != nil || !okA {
		t.Fatalf("first guard must win: ok=%v err=%v", okA, err)
	}
	okB, err := b.Ensure(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if okB {
		t.Fatal("two replicas hold the same lock: this is exactly the split brain the lock exists to prevent")
	}
	if !a.Held() || b.Held() {
		t.Fatalf("Held() disagrees with Ensure(): a=%v b=%v", a.Held(), b.Held())
	}
	// 重复 Ensure 对持锁方是幂等的，不该把自己挤掉
	if ok, err := a.Ensure(ctx); err != nil || !ok {
		t.Fatalf("re-Ensure by the holder must stay true: %v %v", ok, err)
	}

	a.Release(ctx)
	if a.Held() {
		t.Fatal("Release must clear Held()")
	}
	okB, err = b.Ensure(ctx)
	if err != nil || !okB {
		t.Fatalf("after release the other replica must take over: ok=%v err=%v", okB, err)
	}
	b.Release(ctx)
}

// 放锁必须真的把连接干净地还回池子：不能泄漏连接，也不能把锁留在池子里的连接上。
func TestIntegration_ReleaseDoesNotLeakLockIntoPool(t *testing.T) {
	pool, ctx := itPool(t)
	name := "it-leak-" + time.Now().Format("150405.000000")
	key := Key(name)

	g := New(pool, name)
	if ok, err := g.Ensure(ctx); err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	g.Release(ctx)

	// PG 侧不该再有人持有这把锁
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND ((classid::bigint<<32)|objid::bigint)=$1`,
		key).Scan(&n); err != nil {
		// 不同 PG 版本的 pg_locks 字段拼法可能不同，退化成「能再抢到就算干净」
		t.Logf("pg_locks probe unavailable (%v), falling back to re-acquire check", err)
	} else if n != 0 {
		t.Fatalf("%d advisory locks still held after Release", n)
	}
	// 最有力的证明：另一个 Guard 能立刻拿到
	other := New(pool, name)
	if ok, err := other.Ensure(ctx); err != nil || !ok {
		t.Fatalf("lock was not actually released: ok=%v err=%v", ok, err)
	}
	other.Release(ctx)
}

// Every：持锁副本跑，非持锁副本一轮都不跑。
func TestIntegration_EveryRunsOnlyOnHolder(t *testing.T) {
	pool, ctx := itPool(t)
	name := "it-every-" + time.Now().Format("150405.000000")

	var mu sync.Mutex
	runs := map[string]int{}
	count := func(id string) func(context.Context) error {
		return func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			runs[id]++
			return nil
		}
	}

	cctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			Every(cctx, pool, name, 50*time.Millisecond, count(id))
		}(id)
	}
	time.Sleep(400 * time.Millisecond)
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	total := runs["a"] + runs["b"]
	if total == 0 {
		t.Fatal("nobody ran")
	}
	// 两个副本都跑过就说明锁没起作用
	if runs["a"] > 0 && runs["b"] > 0 {
		t.Fatalf("both replicas ran (a=%d b=%d): the lock did not serialise them", runs["a"], runs["b"])
	}
	// 锁在 Every 退出时释放，之后应该能被别人拿到
	g := New(pool, name)
	ok, err := g.Ensure(ctx)
	if err != nil || !ok {
		t.Fatalf("Every must release the lock on exit: ok=%v err=%v", ok, err)
	}
	g.Release(ctx)
}

// fn 报错不能让循环退出，也不能丢掉锁：一次对账失败不该让这个副本从此不干活。
func TestIntegration_EverySurvivesFnError(t *testing.T) {
	pool, ctx := itPool(t)
	name := "it-err-" + time.Now().Format("150405.000000")

	var mu sync.Mutex
	var n int
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Every(cctx, pool, name, 40*time.Millisecond, func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			n++
			return errors.New("boom")
		})
	}()
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if n < 3 {
		t.Fatalf("a failing round must not stop the loop, only ran %d times", n)
	}
}
