package blocklist

import (
	"context"
	"sync"
)

// MemoryBackend keeps admin entries in process memory: one replica, gone on
// restart. The fallback when Redis is not configured.
type MemoryBackend struct {
	mu      sync.Mutex
	entries map[string]Entry
}

// NewMemoryBackend returns an empty MemoryBackend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{entries: make(map[string]Entry)}
}

// Save implements Backend.
func (b *MemoryBackend) Save(_ context.Context, e Entry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries[e.Domain] = e
	return nil
}

// Delete implements Backend.
func (b *MemoryBackend) Delete(_ context.Context, domain string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.entries, domain)
	return nil
}

// Load implements Backend.
func (b *MemoryBackend) Load(_ context.Context) ([]Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Entry, 0, len(b.entries))
	for _, e := range b.entries {
		out = append(out, e)
	}
	return out, nil
}
