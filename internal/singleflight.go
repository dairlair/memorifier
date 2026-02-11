// Package internal provides internal utilities for the memorifier cache.
package internal

import (
	"sync"
)

// call represents an in-flight or completed loader invocation.
type call[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

// Group deduplicates concurrent function calls by key. It ensures that only
// one execution of a function is in-flight for a given key at any time.
// Subsequent callers for the same key block until the first call completes
// and then receive the same result.
type Group[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*call[V]
}

// Do function executes fn if no in-flight call exists for key. If a call is already
// in-flight, Do blocks until it completes and returns the same result.
func (g *Group[K, V]) Do(key K, fn func() (V, error)) (V, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[V])
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call[V])
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
