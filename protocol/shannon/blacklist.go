package shannon

import (
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

const defaultBlacklistDuration = 15 * time.Minute

// blacklist tracks suppliers that should be temporarily excluded from selection
// due to validation or signature errors. Entries expire after a configurable duration.
type blacklist struct {
	mu       sync.RWMutex
	blocked  map[blacklistKey]time.Time // key → expiry
	duration time.Duration
}

type blacklistKey struct {
	serviceID    domain.ServiceID
	supplierAddr string
}

// newBlacklist creates a blacklist with the default expiry duration.
func newBlacklist() *blacklist {
	return &blacklist{
		blocked:  make(map[blacklistKey]time.Time),
		duration: defaultBlacklistDuration,
	}
}

// BlacklistSupplier adds a supplier to the blacklist for a specific service.
//
// Expired entries are pruned here, the only place the map grows. A periodic
// cleanup existed and nothing ever called it, so every supplier ever
// blacklisted stayed in the map for the life of the process.
func (b *blacklist) BlacklistSupplier(serviceID domain.ServiceID, addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	for key, expiry := range b.blocked {
		if now.After(expiry) {
			delete(b.blocked, key)
		}
	}
	b.blocked[blacklistKey{serviceID, addr}] = now.Add(b.duration)
}

// UnblacklistSupplier removes a supplier from the blacklist.
// Returns true if the supplier was present and removed.
func (b *blacklist) UnblacklistSupplier(serviceID domain.ServiceID, addr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := blacklistKey{serviceID, addr}
	if _, exists := b.blocked[key]; exists {
		delete(b.blocked, key)
		return true
	}
	return false
}

// IsBlacklisted returns true if the supplier is currently blacklisted for the service.
// Expired entries are treated as not blacklisted.
func (b *blacklist) IsBlacklisted(serviceID domain.ServiceID, addr string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	expiry, exists := b.blocked[blacklistKey{serviceID, addr}]
	if !exists {
		return false
	}
	return time.Now().Before(expiry)
}

