package blocklist

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/internal/safego"
)

// Entry is one admin-set ban.
type Entry struct {
	// Domain is the registrable domain (or operator label) to ban, lower-cased.
	Domain string `json:"domain"`
	// RPCTypes limits the ban to these RPC types. Empty means every type.
	RPCTypes []string `json:"rpc_types,omitempty"`
	// Reason is free text for the next operator.
	Reason string `json:"reason,omitempty"`
	// Since is when the ban was set.
	Since time.Time `json:"since"`
}

// ErrPropagation reports that a change applied to this replica but did not
// reach the shared backend, so peers do not have it. The local change stands.
var ErrPropagation = errors.New("blocked domain did not propagate to redis")

// ErrNotFound is returned by Release for a domain no admin entry covers.
var ErrNotFound = errors.New("no admin-set entry for that domain")

// Backend persists admin entries. Save and Delete are the writes an admin
// request makes; Load is what every replica polls to learn what the others
// wrote.
type Backend interface {
	Save(ctx context.Context, e Entry) error
	Delete(ctx context.Context, domain string) error
	Load(ctx context.Context) ([]Entry, error)
}

// Applier is where the union goes: the protocol's SetBlockedDomains.
type Applier interface {
	SetBlockedDomains(entries []config.BlockedDomain) error
}

// Manager owns the union of the config base and the admin entries.
type Manager struct {
	apply   Applier
	backend Backend
	// pollInterval is how often Load runs to pick up peers' writes. Zero
	// means never — the memory backend has no peers.
	pollInterval time.Duration

	mu      sync.Mutex
	base    []config.BlockedDomain
	entries map[string]Entry
	// shared is true when the backend is reachable by other replicas.
	shared bool
}

// Option configures a Manager.
type Option func(*Manager)

// WithPollInterval sets how often the backend is re-read for peers' changes.
func WithPollInterval(d time.Duration) Option {
	return func(m *Manager) { m.pollInterval = d }
}

// WithShared marks the backend as fleet-wide, for the admin API's benefit.
func WithShared(shared bool) Option {
	return func(m *Manager) { m.shared = shared }
}

// New returns a Manager over apply and backend, with base as the config
// entries. Nothing is applied until SetBlockedDomains or Start.
func New(apply Applier, backend Backend, base []config.BlockedDomain, opts ...Option) *Manager {
	m := &Manager{
		apply:   apply,
		backend: backend,
		base:    append([]config.BlockedDomain(nil), base...),
		entries: make(map[string]Entry),
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// Start loads the backend once, applies the union, and — when a poll
// interval is set — keeps re-reading it so a peer's ban lands here too.
func (m *Manager) Start(ctx context.Context) error {
	if err := m.reload(ctx); err != nil {
		return err
	}
	if m.pollInterval <= 0 {
		return nil
	}
	safego.GoCtx(ctx, nil, "blocklist.poll", func(ctx context.Context) {
		ticker := time.NewTicker(m.pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				safego.Run(nil, "blocklist.reload", func() { _ = m.reload(ctx) })
			}
		}
	})
	return nil
}

// SetBlockedDomains replaces the config base and re-applies the union. It is
// the seam a config reload writes through, so Manager satisfies the same
// interface the protocol did before it sat in between.
func (m *Manager) SetBlockedDomains(base []config.BlockedDomain) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev := m.base
	m.base = append([]config.BlockedDomain(nil), base...)
	if err := m.applyLocked(); err != nil {
		m.base = prev
		return err
	}
	return nil
}

// Set adds or replaces an admin entry, applies it here, then persists it.
// A union the protocol rejects (an unknown rpc_type, say) is not stored and
// the error is the protocol's. A backend failure after a successful apply is
// ErrPropagation, wrapped.
func (m *Manager) Set(ctx context.Context, e Entry) error {
	e.Domain = strings.ToLower(strings.TrimSpace(e.Domain))
	if e.Domain == "" {
		return errors.New("domain is required")
	}
	for i, t := range e.RPCTypes {
		e.RPCTypes[i] = strings.ToLower(strings.TrimSpace(t))
	}
	if e.Since.IsZero() {
		e.Since = time.Now().UTC()
	}

	m.mu.Lock()
	prev, had := m.entries[e.Domain]
	m.entries[e.Domain] = e
	if err := m.applyLocked(); err != nil {
		if had {
			m.entries[e.Domain] = prev
		} else {
			delete(m.entries, e.Domain)
		}
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()

	if m.backend == nil {
		return nil
	}
	if err := m.backend.Save(ctx, e); err != nil {
		return fmt.Errorf("%w: %v", ErrPropagation, err)
	}
	return nil
}

// Release removes an admin entry. A domain only the config lists is
// ErrNotFound: the file owns it.
func (m *Manager) Release(ctx context.Context, domain string) error {
	domain = strings.ToLower(strings.TrimSpace(domain))

	m.mu.Lock()
	prev, had := m.entries[domain]
	if !had {
		m.mu.Unlock()
		return ErrNotFound
	}
	delete(m.entries, domain)
	if err := m.applyLocked(); err != nil {
		m.entries[domain] = prev
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()

	if m.backend == nil {
		return nil
	}
	if err := m.backend.Delete(ctx, domain); err != nil {
		return fmt.Errorf("%w: %v", ErrPropagation, err)
	}
	return nil
}

// Entries returns the admin-set entries, sorted by domain.
func (m *Manager) Entries() []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entriesLocked()
}

// Base returns the config entries currently in force.
func (m *Manager) Base() []config.BlockedDomain {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]config.BlockedDomain(nil), m.base...)
}

// Shared reports whether admin entries reach other replicas.
func (m *Manager) Shared() bool { return m.shared }

// Len returns the number of admin-set entries.
func (m *Manager) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// reload replaces the admin entries with the backend's and re-applies when
// they differ. A backend read failure keeps what is here.
func (m *Manager) reload(ctx context.Context) error {
	if m.backend == nil {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.applyLocked()
	}
	loaded, err := m.backend.Load(ctx)
	if err != nil {
		return err
	}
	next := make(map[string]Entry, len(loaded))
	for _, e := range loaded {
		e.Domain = strings.ToLower(strings.TrimSpace(e.Domain))
		if e.Domain != "" {
			next[e.Domain] = e
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if sameEntries(m.entries, next) {
		return nil
	}
	prev := m.entries
	m.entries = next
	if err := m.applyLocked(); err != nil {
		m.entries = prev
		return err
	}
	return nil
}

// applyLocked hands the union to the protocol. Config first, admin entries
// after: the protocol's compiler treats a later "all types" entry as widening
// an earlier typed one and never narrows, so order only decides that.
func (m *Manager) applyLocked() error {
	union := make([]config.BlockedDomain, 0, len(m.base)+len(m.entries))
	union = append(union, m.base...)
	for _, e := range m.entriesLocked() {
		union = append(union, config.BlockedDomain{Domain: e.Domain, RPCTypes: e.RPCTypes})
	}
	return m.apply.SetBlockedDomains(union)
}

func (m *Manager) entriesLocked() []Entry {
	out := make([]Entry, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Domain < out[j].Domain })
	return out
}

func sameEntries(a, b map[string]Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for k, ea := range a {
		eb, ok := b[k]
		if !ok || ea.Reason != eb.Reason || !ea.Since.Equal(eb.Since) || len(ea.RPCTypes) != len(eb.RPCTypes) {
			return false
		}
		for i := range ea.RPCTypes {
			if ea.RPCTypes[i] != eb.RPCTypes[i] {
				return false
			}
		}
	}
	return true
}
