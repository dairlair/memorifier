package eviction

// Noop implements a no-op eviction policy that never evicts entries.
//
// Use Noop for unbounded caches where eviction is not desired.
type Noop[K comparable] struct{}

// NewNoop creates a new no-op eviction policy.
func NewNoop[K comparable]() *Noop[K] {
	return &Noop[K]{}
}

// OnSet is a no-op.
func (*Noop[K]) OnSet(K) {}

// OnGet is a no-op.
func (*Noop[K]) OnGet(K) {}

// OnDelete is a no-op.
func (*Noop[K]) OnDelete(K) {}

// Evict always reports that there is nothing to evict.
func (*Noop[K]) Evict() (K, bool) {
	var zero K
	return zero, false
}
