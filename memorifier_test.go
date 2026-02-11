package memorifier_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dairlair/memorifier"
	"github.com/dairlair/memorifier/eviction"
)

func TestBasicLoading(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (string, error) {
		calls.Add(1)
		return "value-" + key, nil
	}

	c := memorifier.New[string, string](loader)
	defer c.Close()

	ctx := context.Background()

	// First call: cache miss, loader invoked.
	val, err := c.Get(ctx, "a")
	assertNoError(t, err)
	assertEqual(t, "value-a", val)
	assertEqual(t, int64(1), calls.Load())

	// Second call: cache hit, loader NOT invoked.
	val, err = c.Get(ctx, "a")
	assertNoError(t, err)
	assertEqual(t, "value-a", val)
	assertEqual(t, int64(1), calls.Load())
}

func TestLoaderError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	var calls atomic.Int64
	loader := func(_ context.Context, key string) (string, error) {
		calls.Add(1)
		return "", errBoom
	}

	c := memorifier.New[string, string](loader)
	defer c.Close()

	// Error result must NOT be cached.
	_, err := c.Get(context.Background(), "a")
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected errBoom, got %v", err)
	}

	_, err = c.Get(context.Background(), "a")
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected errBoom on second call, got %v", err)
	}

	// Loader should have been called twice (errors are not cached).
	assertEqual(t, int64(2), calls.Load())
}

func TestTTLExpiration(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (int, error) {
		return int(calls.Add(1)), nil
	}

	c := memorifier.New[string, int](
		loader,
		memorifier.WithTTL[string, int](50*time.Millisecond),
	)
	defer c.Close()

	ctx := context.Background()

	val, err := c.Get(ctx, "k")
	assertNoError(t, err)
	assertEqual(t, 1, val)

	// Still cached.
	val, err = c.Get(ctx, "k")
	assertNoError(t, err)
	assertEqual(t, 1, val)

	// Wait for TTL to expire.
	time.Sleep(100 * time.Millisecond)

	// Must reload.
	val, err = c.Get(ctx, "k")
	assertNoError(t, err)
	assertEqual(t, 2, val)
}

func TestSlidingExpiration(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (int, error) {
		return int(calls.Add(1)), nil
	}

	c := memorifier.New[string, int](
		loader,
		memorifier.WithTTL[string, int](100*time.Millisecond),
		memorifier.WithSlidingExpiration[string, int](true),
	)
	defer c.Close()

	ctx := context.Background()

	val, err := c.Get(ctx, "k")
	assertNoError(t, err)
	assertEqual(t, 1, val)

	// Access repeatedly before TTL expires — each access resets the clock.
	for i := 0; i < 5; i++ {
		time.Sleep(60 * time.Millisecond)
		val, err = c.Get(ctx, "k")
		assertNoError(t, err)
		assertEqual(t, 1, val)
	}

	// Total elapsed: ~300ms with a 100ms TTL. Sliding kept it alive.
	assertEqual(t, int64(1), calls.Load())

	// Stop accessing, wait for expiration.
	time.Sleep(150 * time.Millisecond)

	val, err = c.Get(ctx, "k")
	assertNoError(t, err)
	assertEqual(t, 2, val)
}

func TestLRUEviction(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key int) (string, error) {
		calls.Add(1)
		return fmt.Sprintf("val-%d", key), nil
	}

	c := memorifier.New[int, string](
		loader,
		memorifier.WithMaxSize[int, string](3),
		memorifier.WithEviction[int, string](eviction.NewLRU[int]()),
	)
	defer c.Close()

	ctx := context.Background()

	// Fill cache: 1, 2, 3. LRU order (MRU→LRU): [3, 2, 1]
	for i := 1; i <= 3; i++ {
		_, err := c.Get(ctx, i)
		assertNoError(t, err)
	}

	// Access key 1 → LRU order: [1, 3, 2]
	_, _ = c.Get(ctx, 1)

	// Reset counter.
	calls.Store(0)

	// Add key 4 → evicts key 2 (LRU tail). Order: [4, 1, 3]
	_, err := c.Get(ctx, 4)
	assertNoError(t, err)
	assertEqual(t, int64(1), calls.Load()) // loaded key 4

	calls.Store(0)

	// Key 2 was evicted: must reload.
	val, err := c.Get(ctx, 2)
	assertNoError(t, err)
	assertEqual(t, "val-2", val)
	assertEqual(t, int64(1), calls.Load())

	calls.Store(0)

	// Key 1 should still be cached.
	val, err = c.Get(ctx, 1)
	assertNoError(t, err)
	assertEqual(t, "val-1", val)
	assertEqual(t, int64(0), calls.Load())
}

func TestFIFOEviction(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key int) (string, error) {
		calls.Add(1)
		return fmt.Sprintf("val-%d", key), nil
	}

	c := memorifier.New[int, string](
		loader,
		memorifier.WithMaxSize[int, string](3),
		memorifier.WithEviction[int, string](eviction.NewFIFO[int]()),
	)
	defer c.Close()

	ctx := context.Background()

	// Fill cache: 1, 2, 3. FIFO order: [1, 2, 3]
	for i := 1; i <= 3; i++ {
		_, _ = c.Get(ctx, i)
	}

	// Access key 1 — FIFO ignores access, order unchanged: [1, 2, 3]
	_, _ = c.Get(ctx, 1)

	calls.Store(0)

	// Add key 4 → evicts key 1 (head/oldest). FIFO: [2, 3, 4]
	_, _ = c.Get(ctx, 4)
	assertEqual(t, int64(1), calls.Load())

	calls.Store(0)

	// Key 1 was evicted — must reload. Evicts key 2 (new head). FIFO: [3, 4, 1]
	_, _ = c.Get(ctx, 1)
	assertEqual(t, int64(1), calls.Load())

	calls.Store(0)

	// Key 4 should still be cached (it was added after key 2).
	val, err := c.Get(ctx, 4)
	assertNoError(t, err)
	assertEqual(t, "val-4", val)
	assertEqual(t, int64(0), calls.Load())

	// Key 3 should still be cached as well.
	calls.Store(0)
	val, err = c.Get(ctx, 3)
	assertNoError(t, err)
	assertEqual(t, "val-3", val)
	assertEqual(t, int64(0), calls.Load())
}

func TestMaxSizeEviction(t *testing.T) {
	t.Parallel()

	loader := func(_ context.Context, key int) (int, error) {
		return key * 10, nil
	}

	const maxSize = 5

	c := memorifier.New[int, int](
		loader,
		memorifier.WithMaxSize[int, int](maxSize),
	)
	defer c.Close()

	ctx := context.Background()

	// Insert more keys than maxSize.
	for i := 0; i < 20; i++ {
		val, err := c.Get(ctx, i)
		assertNoError(t, err)
		assertEqual(t, i*10, val)
	}

	// Cache should still function correctly after evictions.
	for i := 15; i < 20; i++ {
		val, err := c.Get(ctx, i)
		assertNoError(t, err)
		assertEqual(t, i*10, val)
	}
}

func TestStampedeProtection(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (string, error) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond) // simulate slow load
		return "value-" + key, nil
	}

	c := memorifier.New[string, string](loader)
	defer c.Close()

	const goroutines = 100
	var wg sync.WaitGroup

	results := make([]string, goroutines)
	errs := make([]error, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = c.Get(context.Background(), "shared")
		}(i)
	}
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		assertNoError(t, errs[i])
		assertEqual(t, "value-shared", results[i])
	}

	// Only ONE loader call despite 100 concurrent goroutines.
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader called %d times, want 1 (stampede protection)", n)
	}
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()

	loader := func(_ context.Context, key int) (int, error) {
		return key * 10, nil
	}

	c := memorifier.New[int, int](
		loader,
		memorifier.WithTTL[int, int](50*time.Millisecond),
		memorifier.WithMaxSize[int, int](50),
		memorifier.WithEviction[int, int](eviction.NewLRU[int]()),
	)
	defer c.Close()

	const goroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := (id*7 + j) % 200
				val, err := c.Get(ctx, key)
				if err != nil {
					t.Errorf("goroutine %d: unexpected error: %v", id, err)
					return
				}
				if val != key*10 {
					t.Errorf("goroutine %d: got %d, want %d", id, val, key*10)
					return
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestWarmUp(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (string, error) {
		calls.Add(1)
		return "val-" + key, nil
	}

	c := memorifier.New[string, string](
		loader,
		memorifier.WithWarmUpConcurrency[string, string](4),
	)
	defer c.Close()

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	err := c.WarmUp(context.Background(), keys)
	assertNoError(t, err)
	assertEqual(t, int64(len(keys)), calls.Load())

	// All keys should be cached — no additional loads.
	calls.Store(0)
	for _, key := range keys {
		val, err := c.Get(context.Background(), key)
		assertNoError(t, err)
		assertEqual(t, "val-"+key, val)
	}
	assertEqual(t, int64(0), calls.Load())
}

func TestWarmUpFailFast(t *testing.T) {
	t.Parallel()

	errFail := errors.New("load failed")
	var calls atomic.Int64
	loader := func(_ context.Context, key int) (string, error) {
		n := calls.Add(1)
		if n >= 3 {
			return "", errFail
		}
		// Slow enough that fail-fast triggers before all keys are sent.
		time.Sleep(10 * time.Millisecond)
		return fmt.Sprintf("val-%d", key), nil
	}

	c := memorifier.New[int, string](
		loader,
		memorifier.WithWarmUpConcurrency[int, string](2),
		memorifier.WithWarmUpFailFast[int, string](true),
	)
	defer c.Close()

	keys := make([]int, 100)
	for i := range keys {
		keys[i] = i
	}

	err := c.WarmUp(context.Background(), keys)
	if err == nil {
		t.Fatal("expected error from WarmUp with fail-fast")
	}

	// Fail-fast should have stopped early — not all 100 keys loaded.
	if n := calls.Load(); n >= int64(len(keys)) {
		t.Fatalf("fail-fast did not stop early: loader called %d times", n)
	}
}

func TestInvalidate(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (int, error) {
		return int(calls.Add(1)), nil
	}

	c := memorifier.New[string, int](loader)
	defer c.Close()

	ctx := context.Background()

	val, _ := c.Get(ctx, "k")
	assertEqual(t, 1, val)

	c.Invalidate("k")

	// Must reload after invalidation.
	val, _ = c.Get(ctx, "k")
	assertEqual(t, 2, val)

	// Invalidating a non-existent key is a no-op.
	c.Invalidate("nonexistent")
}

func TestInvalidateAll(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (int, error) {
		return int(calls.Add(1)), nil
	}

	c := memorifier.New[string, int](loader)
	defer c.Close()

	ctx := context.Background()

	_, _ = c.Get(ctx, "a")
	_, _ = c.Get(ctx, "b")
	_, _ = c.Get(ctx, "c")
	assertEqual(t, int64(3), calls.Load())

	c.InvalidateAll()

	// All keys must reload.
	_, _ = c.Get(ctx, "a")
	_, _ = c.Get(ctx, "b")
	_, _ = c.Get(ctx, "c")
	assertEqual(t, int64(6), calls.Load())
}

func TestCleanupGoroutine(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (int, error) {
		return int(calls.Add(1)), nil
	}

	c := memorifier.New[string, int](
		loader,
		memorifier.WithTTL[string, int](30*time.Millisecond),
		memorifier.WithCleanupInterval[string, int](20*time.Millisecond),
	)

	ctx := context.Background()

	// Load entries.
	for _, k := range []string{"a", "b", "c"} {
		_, _ = c.Get(ctx, k)
	}
	assertEqual(t, int64(3), calls.Load())

	// Wait for TTL expiration + cleanup cycle.
	time.Sleep(120 * time.Millisecond)

	// Close stops the background goroutine.
	assertNoError(t, c.Close())

	// All entries should have been cleaned up; re-access triggers reload.
	for _, k := range []string{"a", "b", "c"} {
		_, _ = c.Get(ctx, k)
	}
	assertEqual(t, int64(6), calls.Load())
}

func TestCloseIdempotent(t *testing.T) {
	t.Parallel()

	c := memorifier.New[string, string](
		func(_ context.Context, key string) (string, error) {
			return key, nil
		},
		memorifier.WithCleanupInterval[string, string](10*time.Millisecond),
	)

	// Close must be safe to call multiple times.
	assertNoError(t, c.Close())
	assertNoError(t, c.Close())
	assertNoError(t, c.Close())
}

func TestNilLoaderPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil loader")
		}
	}()

	memorifier.New[string, string](nil)
}

func TestNoTTL(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (int, error) {
		return int(calls.Add(1)), nil
	}

	c := memorifier.New[string, int](loader)
	defer c.Close()

	ctx := context.Background()

	_, _ = c.Get(ctx, "k")

	// Without TTL, entry should never expire.
	time.Sleep(50 * time.Millisecond)

	_, _ = c.Get(ctx, "k")
	assertEqual(t, int64(1), calls.Load())
}

func TestWarmUpWithDuplicateKeys(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	loader := func(_ context.Context, key string) (string, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond) // slow enough for dedup
		return "val-" + key, nil
	}

	c := memorifier.New[string, string](
		loader,
		memorifier.WithWarmUpConcurrency[string, string](4),
	)
	defer c.Close()

	// Duplicate keys: stampede protection should deduplicate loads.
	keys := []string{"a", "a", "a", "b", "b", "c"}
	err := c.WarmUp(context.Background(), keys)
	assertNoError(t, err)

	// At most 3 unique loads (a, b, c). May be fewer due to singleflight.
	if n := calls.Load(); n > 3 {
		t.Fatalf("expected at most 3 loads for 3 unique keys, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func assertEqual[T comparable](t *testing.T, want, got T) {
	t.Helper()
	if want != got {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
