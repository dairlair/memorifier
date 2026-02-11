package memorifier

import (
	"context"
	"sync"
	"time"

	"github.com/dairlair/memorifier/eviction"
	"github.com/dairlair/memorifier/internal"
)

// cache is the concrete implementation of the Cache interface.
type cache[K comparable, V any] struct {
	mu     sync.Mutex
	items  map[K]*entry[V]
	loader LoaderFunc[K, V]
	flight *internal.Group[K, V]

	ttl             time.Duration
	sliding         bool
	maxSize         int
	policy          eviction.Policy[K]
	cleanupInterval time.Duration
	cleanupStop     chan struct{}
	closeOnce       sync.Once
	warmUpWorkers   int
	warmUpFailFast  bool
}

func newCache[K comparable, V any](loader LoaderFunc[K, V], opts ...Option[K, V]) *cache[K, V] {
	c := &cache[K, V]{
		items:         make(map[K]*entry[V]),
		loader:        loader,
		flight:        &internal.Group[K, V]{},
		warmUpWorkers: 1,
	}
	for _, opt := range opts {
		opt(c)
	}

	// Default eviction policy: LRU when max size is set, Noop otherwise.
	if c.policy == nil {
		if c.maxSize > 0 {
			c.policy = eviction.NewLRU[K]()
		} else {
			c.policy = eviction.NewNoop[K]()
		}
	}

	if c.cleanupInterval > 0 {
		c.cleanupStop = make(chan struct{})
		go c.cleanup()
	}

	return c
}

// Get retrieves or loads the value for the given key.
func (c *cache[K, V]) Get(ctx context.Context, key K) (V, error) {
	if val, ok := c.get(key); ok {
		return val, nil
	}
	return c.load(ctx, key)
}

// get looks up a key in the cache. It returns the value and true if the entry
// exists and has not expired, updating eviction metadata and sliding TTL.
func (c *cache[K, V]) get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}

	if e.isExpired() {
		// Lazy expiration: remove the expired entry immediately.
		c.policy.OnDelete(key)
		delete(c.items, key)
		var zero V
		return zero, false
	}

	c.policy.OnGet(key)
	if c.sliding && c.ttl > 0 {
		e.expiresAt = time.Now().Add(c.ttl)
	}

	return e.value, true
}

// load calls the loader through singleflight and stores the result on success.
func (c *cache[K, V]) load(ctx context.Context, key K) (V, error) {
	return c.flight.Do(key, func() (V, error) {
		// Re-check: another caller may have populated the cache while
		// this goroutine was waiting for the singleflight slot.
		if val, ok := c.get(key); ok {
			return val, nil
		}

		val, err := c.loader(ctx, key)
		if err != nil {
			return val, err
		}

		c.mu.Lock()
		c.store(key, val)
		c.mu.Unlock()

		return val, nil
	})
}

// store adds or updates a cache entry. Must be called with c.mu held.
func (c *cache[K, V]) store(key K, val V) {
	e := &entry[V]{
		value: val,
	}
	if c.ttl > 0 {
		e.expiresAt = time.Now().Add(c.ttl)
	}

	c.items[key] = e
	c.policy.OnSet(key)
	c.evictExcess()
}

// evictExcess removes entries until the cache is within max size.
// Must be called with c.mu held.
func (c *cache[K, V]) evictExcess() {
	if c.maxSize <= 0 {
		return
	}
	for len(c.items) > c.maxSize {
		key, ok := c.policy.Evict()
		if !ok {
			break
		}
		delete(c.items, key)
	}
}

// Invalidate removes a single key from the cache.
func (c *cache[K, V]) Invalidate(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; ok {
		c.policy.OnDelete(key)
		delete(c.items, key)
	}
}

// InvalidateAll removes all entries from the cache.
func (c *cache[K, V]) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.items {
		c.policy.OnDelete(key)
	}
	c.items = make(map[K]*entry[V])
}

// WarmUp preloads the cache with the given keys concurrently.
func (c *cache[K, V]) WarmUp(ctx context.Context, keys []K) error {
	workers := c.warmUpWorkers
	if workers < 1 {
		workers = 1
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan K, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range ch {
				if _, err := c.Get(ctx, key); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					if c.warmUpFailFast {
						cancel()
						return
					}
				}
			}
		}()
	}

	go func() {
		defer close(ch)
		for _, key := range keys {
			select {
			case ch <- key:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	return firstErr
}

// Close stops background goroutines and releases resources.
func (c *cache[K, V]) Close() error {
	c.closeOnce.Do(func() {
		if c.cleanupStop != nil {
			close(c.cleanupStop)
		}
	})
	return nil
}

// cleanup periodically removes expired entries in the background.
func (c *cache[K, V]) cleanup() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-c.cleanupStop:
			return
		}
	}
}

// deleteExpired removes all expired entries from the cache.
func (c *cache[K, V]) deleteExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, e := range c.items {
		if e.isExpired() {
			c.policy.OnDelete(key)
			delete(c.items, key)
		}
	}
}
