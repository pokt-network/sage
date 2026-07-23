// Package responsecache provides a thread-safe in-memory LRU response cache
// with TTL-based expiry for relay responses.
package responsecache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
)

// CacheStats holds aggregate statistics for the cache.
type CacheStats struct {
	Hits      uint64
	Misses    uint64
	Size      int
	Evictions uint64
}

type entry struct {
	response *domain.Response
	expiry   time.Time
	key      string // own key, for map removal on eviction
}

// Cache is a thread-safe LRU cache for relay responses with TTL expiry.
// Entries are evicted in least-recently-used order when the cache is at
// capacity, and lazily on Get when they have expired. LRU bookkeeping is a
// doubly-linked list so promote/evict are O(1) under the lock.
type Cache struct {
	mu      sync.Mutex
	entries map[string]*list.Element // values are *entry
	// order holds entries in LRU order: front is the oldest (least recently used).
	order   *list.List
	maxSize int

	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// NewCache creates a new Cache with the given maximum number of entries.
// maxSize must be greater than zero; if it is not, it defaults to 1024.
func NewCache(maxSize int) *Cache {
	if maxSize <= 0 {
		maxSize = 1024
	}
	return &Cache{
		entries: make(map[string]*list.Element, maxSize),
		order:   list.New(),
		maxSize: maxSize,
	}
}

// Get returns the cached response for the given key. Returns (nil, false) if
// the key is not present or the entry has expired. Expired entries are evicted
// lazily on access.
func (c *Cache) Get(key string) (*domain.Response, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	e := elem.Value.(*entry)

	if time.Now().After(e.expiry) {
		// Lazy expiry eviction.
		c.removeElement(elem)
		c.evictions.Add(1)
		c.misses.Add(1)
		return nil, false
	}

	// Promote to most-recently-used.
	c.order.MoveToBack(elem)
	c.hits.Add(1)
	return e.response, true
}

// Set stores a response under the given key with the given TTL. If the cache
// is at capacity, the least-recently-used entry is evicted first.
func (c *Cache) Set(key string, resp *domain.Response, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Overwrite existing entry without changing capacity.
	if elem, exists := c.entries[key]; exists {
		elem.Value = &entry{
			response: resp,
			expiry:   time.Now().Add(ttl),
			key:      key,
		}
		c.order.MoveToBack(elem)
		return
	}

	// Evict LRU entries until under maxSize.
	for len(c.entries) >= c.maxSize {
		oldest := c.order.Front()
		if oldest == nil {
			break
		}
		c.removeElement(oldest)
		c.evictions.Add(1)
	}

	c.entries[key] = c.order.PushBack(&entry{
		response: resp,
		expiry:   time.Now().Add(ttl),
		key:      key,
	})
}

// Key builds a deterministic cache key from a service ID and a list of
// payloads. The key is a hex-encoded SHA-256 hash of: serviceID + each
// payload's method name + each payload's raw bytes, all concatenated.
func Key(serviceID domain.ServiceID, payloads []domain.Payload) string {
	h := sha256.New()
	_, _ = h.Write([]byte(serviceID))
	for _, p := range payloads {
		_, _ = h.Write([]byte(p.Method()))
		_, _ = h.Write(p.Bytes())
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Stats returns a snapshot of the cache statistics.
func (c *Cache) Stats() CacheStats {
	c.mu.Lock()
	size := len(c.entries)
	c.mu.Unlock()
	return CacheStats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Size:      size,
		Evictions: c.evictions.Load(),
	}
}

// removeElement removes an element from both the map and the order list.
// Must be called with c.mu held.
func (c *Cache) removeElement(elem *list.Element) {
	e := c.order.Remove(elem).(*entry)
	delete(c.entries, e.key)
}
