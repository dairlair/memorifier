// Package eviction provides pluggable cache eviction policies.
//
// Implementations are not required to be safe for concurrent use;
// synchronization is handled by the cache layer.
package eviction

// Policy defines the interface for cache eviction strategies.
type Policy[K comparable] interface {
	// OnSet is called when a key is added or updated in the cache.
	OnSet(key K)

	// OnGet is called when a key is accessed in the cache.
	OnGet(key K)

	// OnDelete is called when a key is removed from the cache.
	OnDelete(key K)

	// Evict selects and removes the next candidate for eviction.
	// It returns the evicted key and true, or the zero value and false
	// if there is nothing to evict.
	Evict() (key K, ok bool)
}
