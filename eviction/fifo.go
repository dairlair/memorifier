package eviction

// FIFO implements a First-In-First-Out eviction policy.
//
// Keys are evicted in the order they were first added. Subsequent accesses
// do not change a key's position in the queue.
type FIFO[K comparable] struct {
	items map[K]*node[K]
	head  *node[K] // oldest entry (evict from here)
	tail  *node[K] // newest entry (append here)
}

// NewFIFO creates a new FIFO eviction policy.
func NewFIFO[K comparable]() *FIFO[K] {
	return &FIFO[K]{
		items: make(map[K]*node[K]),
	}
}

// OnSet records a key being set. If the key already exists its position
// in the queue is preserved; otherwise it is appended at the tail.
func (f *FIFO[K]) OnSet(key K) {
	if _, ok := f.items[key]; ok {
		return
	}
	n := &node[K]{key: key}
	f.items[key] = n
	f.pushBack(n)
}

// OnGet is a no-op for FIFO; access order does not affect eviction.
func (f *FIFO[K]) OnGet(K) {}

// OnDelete removes a key from the eviction queue.
func (f *FIFO[K]) OnDelete(key K) {
	if n, ok := f.items[key]; ok {
		f.unlink(n)
		delete(f.items, key)
	}
}

// Evict removes and returns the oldest key in the queue.
func (f *FIFO[K]) Evict() (K, bool) {
	if f.head == nil {
		var zero K
		return zero, false
	}
	key := f.head.key
	f.unlink(f.head)
	delete(f.items, key)
	return key, true
}

// pushBack appends n at the tail of the list.
func (f *FIFO[K]) pushBack(n *node[K]) {
	n.next = nil
	n.prev = f.tail
	if f.tail != nil {
		f.tail.next = n
	}
	f.tail = n
	if f.head == nil {
		f.head = n
	}
}

// unlink removes n from the list without deleting it from the map.
func (f *FIFO[K]) unlink(n *node[K]) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		f.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		f.tail = n.prev
	}
	n.prev = nil
	n.next = nil
}
