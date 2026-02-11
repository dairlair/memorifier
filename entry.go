package memorifier

import "time"

// entry holds a cached value along with its expiration metadata.
type entry[V any] struct {
	value     V
	expiresAt time.Time
}

// isExpired reports whether the entry has exceeded its TTL.
// Entries with a zero expiresAt never expire.
func (e *entry[V]) isExpired() bool {
	if e.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(e.expiresAt)
}
