// Package memorifier provides a generic, read-through, in-memory cache with
// stampede protection, TTL support, and pluggable eviction.
//
// Values are loaded on demand through a mandatory [LoaderFunc]. There is no
// Set method; the cache is strictly read-through, ensuring a single source of
// truth for how values are obtained.
//
// Usage:
//
//	cache := memorifier.New[string, *User](
//	    loadUser,
//	    memorifier.WithTTL[string, *User](5*time.Minute),
//	    memorifier.WithMaxSize[string, *User](10_000),
//	    memorifier.WithEviction[string, *User](eviction.NewLRU[string]()),
//	)
//	defer cache.Close()
//
//	user, err := cache.Get(ctx, "user-123")
package memorifier

import (
	"context"
)

// LoaderFunc is a function that loads a value for the given key. It is called
// on cache misses to populate the cache. The provided context should be
// respected for cancellation and timeouts.
type LoaderFunc[K comparable, V any] func(ctx context.Context, key K) (V, error)

// Cache is a generic, read-through, in-memory cache.
//
// Values are loaded on demand via a LoaderFunc. There is no Set method;
// the cache is strictly read-through, ensuring a single source of truth
// for how values are obtained.
//
// All methods are safe for concurrent use by multiple goroutines.
type Cache[K comparable, V any] interface {
	// Get retrieves a value for the given key. If the key is not cached
	// or has expired, the loader function is called to populate it.
	// Concurrent calls for the same key are deduplicated via stampede
	// protection — only one loader invocation occurs.
	Get(ctx context.Context, key K) (V, error)

	// Invalidate removes a single key from the cache. The next Get for
	// this key will trigger a fresh load.
	Invalidate(key K)

	// InvalidateAll removes all entries from the cache.
	InvalidateAll()

	// WarmUp preloads the cache with the given keys using the internal
	// loader. Loading is performed concurrently with configurable
	// parallelism and deduplicated via stampede protection.
	WarmUp(ctx context.Context, keys []K) error

	// Close releases resources held by the cache, including stopping any
	// background goroutines. It is safe to call multiple times.
	Close() error
}

// New creates a new Cache with the given loader and options.
//
// The loader is mandatory and must not be nil. It is called whenever a cache
// miss occurs.
//
// Options can be used to configure TTL, eviction, max size, and more.
// See the With* functions for available options.
func New[K comparable, V any](loader LoaderFunc[K, V], opts ...Option[K, V]) Cache[K, V] {
	if loader == nil {
		panic("memorifier: loader must not be nil")
	}
	return newCache(loader, opts...)
}
