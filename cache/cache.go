package cache

import (
	"sync"
	"time"
)

type item struct {
	value     interface{}
	expiresAt time.Time
	hasTTL    bool
}

func (i item) isExpired() bool {
	return i.hasTTL && time.Now().After(i.expiresAt)
}

// Cache is a thread-safe in-memory key-value store with optional per-item TTL.
type Cache struct {
	mu    sync.RWMutex
	items map[string]item
}

func New() *Cache {
	return &Cache{items: make(map[string]item)}
}

// Get returns the value for the given key, or nil if not found or expired.
func (c *Cache) Get(key string) interface{} {
	c.mu.RLock()
	entry, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || entry.isExpired() {
		return nil
	}
	return entry.value
}

// Put stores a value under the given key. A zero ttl means no expiration.
func (c *Cache) Put(key string, value interface{}, ttl time.Duration) {
	entry := item{value: value}
	if ttl > 0 {
		entry.hasTTL = true
		entry.expiresAt = time.Now().Add(ttl)
	}
	c.mu.Lock()
	c.items[key] = entry
	c.mu.Unlock()
}

// Delete removes the value for the given key.
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}
