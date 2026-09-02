package reputation

import (
	"context"
	"strings"
	"time"

	"github.com/pokt-network/sage/domain"
)

// HydrateResult is what one warm-up read loaded.
type HydrateResult struct {
	// Keys is how many states were placed in the in-memory cache.
	Keys int
	// Services are the service IDs those states belong to. The health-check
	// executor seeds its readiness coverage from this: a service whose scores
	// are loaded is a service this pod can already steer traffic for.
	Services []domain.ServiceID
	// Skipped counts states read but not loaded — stale, unparseable, or over
	// the per-shard bound.
	Skipped int
}

// Hydrator is the optional half of Service that loads persisted state back
// into memory at startup.
//
// It is optional because the memory-backed service satisfies it trivially
// (there is nothing to load) and because a caller that does not want a cold
// pod adopting another pod's scores can simply not call it.
type Hydrator interface {
	Hydrate(ctx context.Context) (HydrateResult, error)
}

var _ Hydrator = (*serviceImpl)(nil)

// Hydrate loads persisted reputation state into the in-memory cache.
//
// Without it a restarted or newly rolled pod starts every key at InitialScore
// and re-learns the whole pool from probes — minutes during which it either
// selects blind or, behind the readiness gate, is not serving at all. The
// state is already in Storage: the write-behind has been writing it on every
// signal, and until this existed nothing ever read it back (the mainnet canary
// hit exactly this on 2026-09-02, a rolled pod covering 26 of 73 services
// after six minutes while the gate held it out of rotation).
//
// Three bounds keep an adopted score honest:
//
//   - Freshness. A state older than the idle TTL is skipped, as is one with no
//     UpdatedAt stamp at all — the same rule the storage sweep applies, so a
//     hydrating pod never adopts what the sweep is about to delete. The worst
//     case is therefore TTL-old knowledge, against a cold pod's alternative of
//     assuming every endpoint in the network is perfect.
//   - Existing state wins. A signal recorded while the read was in flight is
//     newer than anything in storage, so hydration never overwrites a key that
//     is already present.
//   - The per-shard cap still applies, so a hash left oversized by an older
//     deployment cannot reintroduce the memory growth that bounding the
//     timeline fixed.
//
// Keys are stored at the writer's granularity. A pod configured with a
// different key_granularity than the pod that wrote them loads keys nothing
// will ever look up; they are inert and get pruned like any other
// uninformative entry, but the coverage they imply is real, so the granularity
// should match across a fleet sharing one store.
//
// A storage error is returned, not fatal: the caller logs it and runs cold.
func (s *serviceImpl) Hydrate(ctx context.Context) (HydrateResult, error) {
	states, err := s.storage.GetStates(ctx, "")
	if err != nil {
		return HydrateResult{}, err
	}

	var (
		result HydrateResult
		seen   = make(map[domain.ServiceID]struct{})
		cutoff = time.Now().Add(-s.stateIdleTTL())
	)

	for field, st := range states {
		serviceID, repKey, ok := splitScoreKey(field)
		if !ok || !s.fresh(st, cutoff) {
			result.Skipped++
			continue
		}
		if !s.loadState(serviceID, repKey, st) {
			result.Skipped++
			continue
		}
		result.Keys++
		seen[serviceID] = struct{}{}
	}

	result.Services = make([]domain.ServiceID, 0, len(seen))
	for svc := range seen {
		result.Services = append(result.Services, svc)
	}
	return result, nil
}

// fresh reports whether a stored state is recent enough to adopt. An unstamped
// state predates the UpdatedAt field and is stale by the same definition the
// storage sweep uses. A non-positive TTL disables the sweep, and with nothing
// deleting stale entries there is no age at which one becomes safe to adopt —
// so nothing is loaded rather than loading unbounded history.
func (s *serviceImpl) fresh(st State, cutoff time.Time) bool {
	if st.UpdatedAt <= 0 {
		return false
	}
	return time.Unix(st.UpdatedAt, 0).After(cutoff)
}

// stateIdleTTL is the age at which storage entries are swept, and so the age
// beyond which one must not be adopted.
func (s *serviceImpl) stateIdleTTL() time.Duration {
	if s.cfg.StateIdleTTL == 0 {
		return DefaultIdleTTL
	}
	return s.cfg.StateIdleTTL
}

// loadState places one state in the cache, reporting whether it landed. It
// does not overwrite: a key already present was written by a signal this
// process saw, which is newer than storage by construction.
func (s *serviceImpl) loadState(serviceID domain.ServiceID, repKey string, st State) bool {
	if s.cfg.StateIdleTTL < 0 {
		return false
	}
	sh := s.shard(repKey)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	svcStates := sh.cache[serviceID]
	if svcStates == nil {
		svcStates = make(map[string]State)
		sh.cache[serviceID] = svcStates
	}
	if _, exists := svcStates[repKey]; exists {
		return false
	}
	if len(svcStates) >= maxScoresPerServiceShard {
		s.pruneUninformative(svcStates)
		if len(svcStates) >= maxScoresPerServiceShard {
			return false
		}
	}
	// LatencyMS is reporting-only and deliberately not persisted (see State),
	// so it stays zero until this pod measures its own.
	svcStates[repKey] = st
	return true
}

// splitScoreKey reverses scoreKey. The service ID is everything before the
// first colon; a reputation key can itself contain colons (a URL's port), so
// the split is on the first one only. A field with no colon was not written by
// scoreKey and is not ours to interpret.
func splitScoreKey(field string) (domain.ServiceID, string, bool) {
	serviceID, repKey, found := strings.Cut(field, ":")
	if !found || serviceID == "" || repKey == "" {
		return "", "", false
	}
	return domain.ServiceID(serviceID), repKey, true
}
