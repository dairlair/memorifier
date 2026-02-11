package eviction

// node is a doubly-linked list element used by ordered eviction policies.
type node[K comparable] struct {
	key  K
	prev *node[K]
	next *node[K]
}

// LRU implements a Least Recently Used eviction policy.
//
// It maintains a doubly-linked list ordered by access time. The most recently
// accessed key is at the head; the least recently accessed key is at the tail.
// Eviction removes the tail.
type LRU[K comparable] struct {
	items map[K]*node[K]
	head  *node[K]
	tail  *node[K]
}

// NewLRU creates a new LRU eviction policy.
func NewLRU[K comparable]() *LRU[K] {
	return &LRU[K]{
		items: make(map[K]*node[K]),
	}
}

// OnSet records a key being set. If the key already exists it is promoted
// to the most recently used position; otherwise a new entry is created.
func (l *LRU[K]) OnSet(key K) {
	if n, ok := l.items[key]; ok {
		l.moveToFront(n)
		return
	}
	n := &node[K]{key: key}
	l.items[key] = n
	l.pushFront(n)
}

// OnGet promotes a key to the most recently used position.
func (l *LRU[K]) OnGet(key K) {
	if n, ok := l.items[key]; ok {
		l.moveToFront(n)
	}
}

// OnDelete removes a key from the access-order list.
func (l *LRU[K]) OnDelete(key K) {
	if n, ok := l.items[key]; ok {
		l.unlink(n)
		delete(l.items, key)
	}
}

// Evict removes and returns the least recently used key.
func (l *LRU[K]) Evict() (K, bool) {
	if l.tail == nil {
		var zero K
		return zero, false
	}
	key := l.tail.key
	l.unlink(l.tail)
	delete(l.items, key)
	return key, true
}

// pushFront inserts n at the head of the list.
func (l *LRU[K]) pushFront(n *node[K]) {
	n.prev = nil
	n.next = l.head
	if l.head != nil {
		l.head.prev = n
	}
	l.head = n
	if l.tail == nil {
		l.tail = n
	}
}

// unlink removes n from the list without deleting it from the map.
func (l *LRU[K]) unlink(n *node[K]) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		l.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		l.tail = n.prev
	}
	n.prev = nil
	n.next = nil
}

// moveToFront moves an existing node to the head of the list.
func (l *LRU[K]) moveToFront(n *node[K]) {
	if l.head == n {
		return
	}
	l.unlink(n)
	l.pushFront(n)
}
