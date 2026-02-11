package memorifier

import (
	"time"

	"github.com/dairlair/memorifier/eviction"
)

// Option configures a [Cache] instance. Options are applied in order
// and can be composed freely.
type Option[K comparable, V any] func(*cache[K, V])

// WithTTL sets the time-to-live for cache entries. Entries that exceed their
// TTL are treated as missing and trigger a fresh load.
//
// A zero or negative TTL means entries never expire.
func WithTTL[K comparable, V any](ttl time.Duration) Option[K, V] {
	return func(c *cache[K, V]) {
		c.ttl = ttl
	}
}

// WithSlidingExpiration enables or disables sliding expiration. When enabled,
// each successful [Cache.Get] resets the entry's TTL, keeping frequently
// accessed entries alive longer.
//
// Requires a positive TTL (set via [WithTTL]) to have any effect.
func WithSlidingExpiration[K comparable, V any](enabled bool) Option[K, V] {
	return func(c *cache[K, V]) {
		c.sliding = enabled
	}
}

// WithMaxSize sets the maximum number of entries in the cache. When the limit
// is reached, entries are evicted according to the configured eviction policy.
//
// A zero or negative value means unbounded.
//
// If no eviction policy is explicitly set via [WithEviction], LRU is used by
// default when MaxSize is configured.
func WithMaxSize[K comparable, V any](size int) Option[K, V] {
	return func(c *cache[K, V]) {
		c.maxSize = size
	}
}

// WithEviction sets the eviction policy for the cache. See the [eviction]
// package for built-in policies ([eviction.NewLRU], [eviction.NewFIFO],
// [eviction.NewNoop]).
//
// Implement [eviction.Policy] to create a custom eviction strategy.
func WithEviction[K comparable, V any](policy eviction.Policy[K]) Option[K, V] {
	return func(c *cache[K, V]) {
		c.policy = policy
	}
}

// WithCleanupInterval sets the interval for background cleanup of expired
// entries. When configured, a background goroutine periodically scans the
// cache and removes entries whose TTL has elapsed.
//
// A zero or negative value disables background cleanup. When disabled,
// expired entries are only removed lazily on access.
//
// The background goroutine is stopped when [Cache.Close] is called.
func WithCleanupInterval[K comparable, V any](interval time.Duration) Option[K, V] {
	return func(c *cache[K, V]) {
		c.cleanupInterval = interval
	}
}

// WithWarmUpConcurrency sets the number of concurrent workers used by
// [Cache.WarmUp]. Defaults to 1. Values less than 1 are treated as 1.
func WithWarmUpConcurrency[K comparable, V any](workers int) Option[K, V] {
	return func(c *cache[K, V]) {
		c.warmUpWorkers = workers
	}
}

// WithWarmUpFailFast configures [Cache.WarmUp] to stop on the first loader
// error. When disabled (default), WarmUp attempts to load all keys and
// returns the first error encountered, if any.
func WithWarmUpFailFast[K comparable, V any](enabled bool) Option[K, V] {
	return func(c *cache[K, V]) {
		c.warmUpFailFast = enabled
	}
}
