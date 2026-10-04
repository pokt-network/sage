// Package responsecache provides a thread-safe in-memory LRU response cache
// with TTL-based expiry for relay responses.
package responsecache

import (
	"container/list"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

type entry struct {
	response *domain.Response
	expiry   time.Time
	key      string // own key, for map removal on eviction
	size     int    // len(response.Body), counted against maxBytes
}

// maxBytes bounds the response bodies the cache holds.
//
// The entry cap alone bounded nothing in bytes: a cached block or receipt is
// whatever size the chain made it, and an expired entry stayed until LRU
// eviction reached it. On mainnet (2026-10-04) the busiest pod held about
// 10,000 bodies, 189MB and 46% of its heap, for 0.06% of requests answered
// from the cache.
const maxBytes = 64 << 20

// Cache is a thread-safe LRU cache for relay responses with TTL expiry.
// Entries are evicted in least-recently-used order when the cache is at
// capacity in entries or in bytes, lazily on Get when they have expired, and
// from the LRU end on Set when they have expired. LRU bookkeeping is a
// doubly-linked list so promote/evict are O(1) under the lock.
type Cache struct {
	mu      sync.Mutex
	entries map[string]*list.Element // values are *entry
	// order holds entries in LRU order: front is the oldest (least recently used).
	order    *list.List
	maxSize  int
	bytes    int
	maxBytes int
}

// NewCache creates a new Cache with the given maximum number of entries,
// whose response bodies total at most maxBytes. maxSize must be greater than
// zero; if it is not, it defaults to 1024.
func NewCache(maxSize int) *Cache {
	if maxSize <= 0 {
		maxSize = 1024
	}
	return &Cache{
		entries:  make(map[string]*list.Element, maxSize),
		order:    list.New(),
		maxSize:  maxSize,
		maxBytes: maxBytes,
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
		return nil, false
	}
	e := elem.Value.(*entry)

	if time.Now().After(e.expiry) {
		// Lazy expiry eviction.
		c.removeElement(elem)
		return nil, false
	}

	// Promote to most-recently-used.
	c.order.MoveToBack(elem)
	return e.response, true
}

// Set stores a response under the given key with the given TTL. Expired
// entries at the least-recently-used end go first, then live ones in LRU
// order until the new entry fits both caps. A body larger than the whole
// byte cap is not stored.
func (c *Cache) Set(key string, resp *domain.Response, ttl time.Duration) {
	size := len(resp.Body)
	if ttl <= 0 || size > c.maxBytes {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.entries[key]; exists {
		c.removeElement(elem)
	}

	// ponytail: only the expired run at the LRU end is swept; an expired
	// entry behind a live one waits for eviction or a Get. The byte cap
	// bounds what waits.
	now := time.Now()
	for oldest := c.order.Front(); oldest != nil && now.After(oldest.Value.(*entry).expiry); oldest = c.order.Front() {
		c.removeElement(oldest)
	}
	for len(c.entries) >= c.maxSize || c.bytes+size > c.maxBytes {
		oldest := c.order.Front()
		if oldest == nil {
			break
		}
		c.removeElement(oldest)
	}

	c.entries[key] = c.order.PushBack(&entry{
		response: resp,
		expiry:   now.Add(ttl),
		key:      key,
		size:     size,
	})
	c.bytes += size
}

// removeElement removes an element from both the map and the order list.
// Must be called with c.mu held.
func (c *Cache) removeElement(elem *list.Element) {
	e := c.order.Remove(elem).(*entry)
	delete(c.entries, e.key)
	c.bytes -= e.size
}
