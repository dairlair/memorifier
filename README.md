# 🧠 Memorifier

[![Go Reference](https://pkg.go.dev/badge/github.com/dairlair/memorifier.svg)](https://pkg.go.dev/github.com/dairlair/memorifier)
[![Go Report Card](https://goreportcard.com/badge/github.com/dairlair/memorifier)](https://goreportcard.com/report/github.com/dairlair/memorifier)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A production-grade, generic, read-through in-memory cache for Go.

---

## Features

- **Generic** — fully type-safe with Go generics (`K comparable, V any`)
- **Read-through** — values are loaded on demand via a mandatory loader function
- **Stampede protection** — concurrent requests for the same key are deduplicated (internal singleflight)
- **TTL** — global time-to-live with optional sliding expiration
- **Pluggable eviction** — built-in LRU, FIFO, and Noop policies; bring your own with a simple interface
- **Background cleanup** — optional goroutine for proactive expired-entry removal
- **Warm-up** — bulk preload with configurable concurrency and fail-fast
- **Zero external dependencies** — only the Go standard library
- **Concurrency-safe** — designed for read-heavy, multi-goroutine workloads

## Why Memorifier?

Most Go caching libraries are either:

- Not generic (rely on `interface{}` / `any` assertions)
- Not read-through (require manual `Set` calls)
- Missing stampede protection
- Overloaded with features and dependencies

Memorifier is a focused, minimal-surface cache that does one thing well: **load and cache values safely, efficiently, and idiomatically**.

## Installation

```bash
go get github.com/dairlair/memorifier
```

Requires **Go 1.22** or later.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/dairlair/memorifier"
    "github.com/dairlair/memorifier/eviction"
)

type User struct {
    ID   string
    Name string
}

func main() {
    // Define a loader function.
    loadUser := func(ctx context.Context, id string) (*User, error) {
        // Fetch from database, API, etc.
        return &User{ID: id, Name: "Alice"}, nil
    }

    // Create a cache.
    cache := memorifier.New[string, *User](
        loadUser,
        memorifier.WithTTL[string, *User](5*time.Minute),
        memorifier.WithMaxSize[string, *User](10_000),
        memorifier.WithEviction[string, *User](eviction.NewLRU[string]()),
    )
    defer cache.Close()

    // Get a value — loads on first access, cached thereafter.
    user, err := cache.Get(context.Background(), "123")
    if err != nil {
        panic(err)
    }
    fmt.Println(user.Name) // Alice
}
```

## Configuration

All configuration is done through composable functional options:

```go
cache := memorifier.New[string, *User](
    loadUser,

    // Time-to-live for cache entries.
    memorifier.WithTTL[string, *User](5*time.Minute),

    // Reset TTL on every access.
    memorifier.WithSlidingExpiration[string, *User](true),

    // Maximum number of cached entries.
    memorifier.WithMaxSize[string, *User](10_000),

    // Eviction policy (LRU, FIFO, or Noop).
    memorifier.WithEviction[string, *User](eviction.NewLRU[string]()),

    // Background cleanup of expired entries.
    memorifier.WithCleanupInterval[string, *User](1*time.Minute),

    // Warm-up worker pool size.
    memorifier.WithWarmUpConcurrency[string, *User](8),

    // Stop warm-up on first error.
    memorifier.WithWarmUpFailFast[string, *User](true),
)
defer cache.Close()
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithTTL` | 0 (no expiry) | Time-to-live for entries |
| `WithSlidingExpiration` | `false` | Reset TTL on each access |
| `WithMaxSize` | 0 (unbounded) | Maximum number of entries |
| `WithEviction` | auto (LRU if MaxSize set, Noop otherwise) | Eviction policy |
| `WithCleanupInterval` | 0 (disabled) | Background cleanup interval |
| `WithWarmUpConcurrency` | 1 | Number of warm-up worker goroutines |
| `WithWarmUpFailFast` | `false` | Stop warm-up on first error |

## Eviction Policies

Memorifier ships with three eviction policies:

| Policy | Constructor | Description |
|--------|-------------|-------------|
| **LRU** | `eviction.NewLRU[K]()` | Least Recently Used — evicts the entry that hasn't been accessed for the longest time |
| **FIFO** | `eviction.NewFIFO[K]()` | First-In-First-Out — evicts the entry that was added earliest |
| **Noop** | `eviction.NewNoop[K]()` | No eviction — the cache grows without bound |

When `WithMaxSize` is set and no explicit policy is provided, **LRU** is used by default.

### Custom Eviction

Implement the `eviction.Policy[K]` interface:

```go
type Policy[K comparable] interface {
    OnSet(key K)
    OnGet(key K)
    OnDelete(key K)
    Evict() (key K, ok bool)
}
```

## Warm-Up

Preload frequently accessed keys at startup:

```go
keys := []string{"user:1", "user:2", "user:3"}
if err := cache.WarmUp(ctx, keys); err != nil {
    log.Printf("warm-up error: %v", err)
}
```

- Uses the same loader and stampede protection as `Get`
- Configurable concurrency via `WithWarmUpConcurrency`
- Optional fail-fast via `WithWarmUpFailFast`
- Duplicate keys are deduplicated automatically

## Performance Philosophy

Memorifier is designed for backend and microservice workloads where:

- Reads vastly outnumber writes
- Multiple goroutines may request the same key simultaneously
- Memory is bounded and entries have a natural lifetime

**Key design decisions:**

- **Singleflight deduplication** prevents thundering herd on cache misses
- **Short critical sections** minimize lock contention
- **No reflection** — pure generics for type safety and zero-cost abstractions
- **Lazy expiration** plus optional background cleanup
- **No external dependencies** — auditable, predictable, easy to vendor

## Concurrency Guarantees

- All methods are safe for concurrent use by multiple goroutines
- `Get` calls for the same missing key trigger exactly **one** loader invocation
- Loader errors are **not cached** — subsequent calls will retry
- `Close` is safe to call multiple times (idempotent)

## API Reference

```go
// Constructor
func New[K comparable, V any](loader LoaderFunc[K, V], opts ...Option[K, V]) Cache[K, V]

// Cache interface
type Cache[K comparable, V any] interface {
    Get(ctx context.Context, key K) (V, error)
    Invalidate(key K)
    InvalidateAll()
    WarmUp(ctx context.Context, keys []K) error
    Close() error
}

// Loader
type LoaderFunc[K comparable, V any] func(ctx context.Context, key K) (V, error)
```

## Roadmap

- [ ] Metrics and observability hooks (hit rate, miss rate, eviction count)
- [ ] Per-key TTL override
- [ ] Stale-while-revalidate pattern
- [ ] Benchmarks and performance guide
- [ ] Sharded storage for reduced lock contention

## Contributing

Contributions are welcome! Please open an issue or pull request.

## License

MIT — see [LICENSE](LICENSE) for details.
