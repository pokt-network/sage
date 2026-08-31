package shannon

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
)

const (
	// blockPollInterval is how often we check the full node for the current block height.
	// Blocks on Shannon are ~1 minute, so polling every 10 seconds is plenty responsive
	// for detecting session boundaries while avoiding unnecessary gRPC traffic.
	blockPollInterval = 10 * time.Second
)

// sessionManager caches sessions and their extracted endpoints.
// Sessions are cached by (serviceID, appAddr) and automatically refreshed
// when the current block height crosses the session's end block height.
type sessionManager struct {
	fullNode fullNodeIface
	// sessionCache is keyed on (serviceID, appAddr), a set that does not grow:
	// a rollover overwrites the value rather than adding one.
	sessionCache sync.Map // "serviceID:appAddr" → *sessiontypes.Session
	// endpointCache is keyed on sessionID, which is a NEW value at every
	// rollover — so unlike sessionCache it accumulates, and must be evicted.
	// See evictStaleEndpointsOnRollover.
	endpointCache      sync.Map // sessionID (string) → cachedEndpoints
	configuredServices map[domain.ServiceID]struct{}
	logger             *slog.Logger

	// highestSessionEnd is the newest session end height observed, and
	// rolloverMu serialises the eviction sweep that crossing it triggers. Same
	// shape as the signer's ring cache — see evictStaleRingsOnRollover, which
	// solved this for the other per-session cache.
	highestSessionEnd atomic.Uint64
	rolloverMu        sync.Mutex

	// latestBlockHeight is updated by a background poller.
	// Used to decide when a cached session has expired.
	latestBlockHeight atomic.Int64
	stopPoller        chan struct{}

	// lastFetchErr remembers, per (serviceID, appAddr), the text of the last
	// failed session fetch, so the same failure is reported once rather than
	// every cycle. A service with no suppliers on the network fails every
	// refresh for as long as that lasts; the first failure is an error, the
	// repeats are debug, recovery is info and clears the entry.
	lastFetchErr sync.Map // "serviceID:appAddr" → string
}

// newSessionManager creates a session manager for the given services and full node.
func newSessionManager(fullNode fullNodeIface, services map[domain.ServiceID]struct{}, logger *slog.Logger) *sessionManager {
	return &sessionManager{
		fullNode:           fullNode,
		configuredServices: services,
		logger:             logger.With("component", "session_manager"),
		stopPoller:         make(chan struct{}),
	}
}

// LatestBlockHeight returns the newest chain head the block poller has seen,
// or 0 before the first successful poll. A failed poll leaves the previous
// height in place, so 0 only ever means "not yet polled".
//
// Consumers that need to know whether a specific session has ended compare
// this against their own SessionEndBlockHeight. There is deliberately no
// expiry broadcast: see WSRelayer.watchSessionExpiry.
func (sm *sessionManager) LatestBlockHeight() int64 {
	return sm.latestBlockHeight.Load()
}

// StartBlockPoller starts a background goroutine that polls the full node for
// the current block height. This drives session cache invalidation without
// adding a gRPC call to the relay hot path.
func (sm *sessionManager) StartBlockPoller(ctx context.Context) {
	// Fetch once synchronously so the first relay doesn't hit a cold cache.
	sm.pollBlockHeight(ctx)

	go func() {
		ticker := time.NewTicker(blockPollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				safego.Run(sm.logger, "shannon.blockpoll", func() { sm.pollBlockHeight(ctx) })
			case <-sm.stopPoller:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	sm.logger.Info("block height poller started",
		"interval", blockPollInterval.String(),
	)
}

// StopBlockPoller stops the background block height poller.
func (sm *sessionManager) StopBlockPoller() {
	close(sm.stopPoller)
}

// pollBlockHeight fetches the current block height and stores it. On error the
// previous height is retained rather than zeroed: a stale height degrades to
// "sessions look live for another tick", whereas a zeroed one would read as
// "not yet polled" to every consumer at once.
func (sm *sessionManager) pollBlockHeight(ctx context.Context) {
	height, err := sm.fullNode.GetCurrentBlockHeight(ctx)
	if err != nil {
		sm.logger.Warn("block poller: failed to get block height",
			"error", err,
		)
		return
	}

	prev := sm.latestBlockHeight.Swap(height)
	if height != prev {
		sm.logger.Debug("block poller: updated height",
			"height", height,
			"previous", prev,
		)
	}
}

// evictStaleEndpointsOnRollover bounds endpointCache.
//
// The key is the session ID, which is a fresh value every rollover, so nothing
// ever overwrites anything: each rollover adds a permanent entry holding that
// session's whole endpoint map, per service, per app. Nothing fails — memory
// simply climbs for as long as the process lives, which is why no test catches
// it and why the symptom shows up days later as an OOM.
//
// On observing a session end height newer than any seen, drop entries older
// than the previous session. Current plus previous is kept deliberately: a
// relay that picked its endpoint just before the boundary is still in flight,
// and the grace period means the old session is briefly still valid.
//
// An entry with sessionEnd 0 (a session whose header did not carry a height)
// is never evicted by height, since 0 < anything would drop it immediately —
// it is dropped only if it is not the current session.
func (sm *sessionManager) evictStaleEndpointsOnRollover(sessionEnd uint64) {
	// Fast path: no newer session, nothing to do. Atomic load only.
	if sessionEnd == 0 || sessionEnd <= sm.highestSessionEnd.Load() {
		return
	}

	sm.rolloverMu.Lock()
	defer sm.rolloverMu.Unlock()

	// Re-check under the lock; another goroutine may have handled this rollover.
	prevHighest := sm.highestSessionEnd.Load()
	if sessionEnd <= prevHighest {
		return
	}
	sm.highestSessionEnd.Store(sessionEnd)

	var dropped int
	sm.endpointCache.Range(func(k, v any) bool {
		entry, ok := v.(cachedEndpoints)
		if !ok {
			return true
		}
		if entry.sessionEnd < prevHighest {
			sm.endpointCache.Delete(k)
			dropped++
		}
		return true
	})

	if dropped > 0 {
		sm.logger.Debug("evicted endpoint caches for expired sessions",
			"dropped", dropped,
			"session_end", sessionEnd,
		)
	}
}

// sessionCacheKey returns the key for caching sessions by (serviceID, appAddr).
func sessionCacheKey(serviceID, appAddr string) string {
	return serviceID + ":" + appAddr
}

// getSession returns the cached session for (serviceID, appAddr), refreshing
// from the full node when the current block height has crossed the session's
// end block height.
func (sm *sessionManager) getSession(ctx context.Context, serviceID string, appAddr string) (*sessiontypes.Session, error) {
	key := sessionCacheKey(serviceID, appAddr)

	if cached, ok := sm.sessionCache.Load(key); ok {
		session := cached.(*sessiontypes.Session)
		currentHeight := sm.latestBlockHeight.Load()

		// Session is still valid if current height hasn't reached its end.
		if currentHeight < session.Header.SessionEndBlockHeight {
			return session, nil
		}

		sm.logger.Info("session expired, refreshing",
			"service_id", serviceID,
			"session_id", session.SessionId,
			"session_end_height", session.Header.SessionEndBlockHeight,
			"current_height", currentHeight,
		)
	}

	return sm.refreshSession(ctx, serviceID, appAddr)
}

// refreshSession fetches a fresh session from the full node and updates the cache.
func (sm *sessionManager) refreshSession(ctx context.Context, serviceID string, appAddr string) (*sessiontypes.Session, error) {
	session, err := sm.fullNode.GetSession(ctx, serviceID, appAddr)
	key := sessionCacheKey(serviceID, appAddr)
	if err != nil {
		if prev, seen := sm.lastFetchErr.Load(key); seen && prev.(string) == err.Error() {
			sm.logger.Debug("getSession: full node returned error (repeat)",
				"service_id", serviceID, "app_addr", appAddr, "error", err)
		} else {
			sm.lastFetchErr.Store(key, err.Error())
			sm.logger.Error("getSession: full node returned error",
				"service_id", serviceID,
				"app_addr", appAddr,
				"error", err,
			)
		}
		return nil, fmt.Errorf("getSession: %w", err)
	}
	if _, failed := sm.lastFetchErr.LoadAndDelete(key); failed {
		sm.logger.Info("getSession: full node answers again",
			"service_id", serviceID, "app_addr", appAddr)
	}

	sm.logger.Info("session fetched",
		"service_id", serviceID,
		"session_id", session.SessionId,
		"start_height", session.Header.SessionStartBlockHeight,
		"end_height", session.Header.SessionEndBlockHeight,
		"supplier_count", len(session.Suppliers),
	)

	sm.sessionCache.Store(sessionCacheKey(serviceID, appAddr), session)
	return session, nil
}

// getEndpoints returns the endpoint map for (serviceID, appAddr).
// Endpoints are cached by session ID to avoid redundant extraction.
func (sm *sessionManager) getEndpoints(ctx context.Context, serviceID string, appAddr string) (map[domain.EndpointAddr]*endpoint, error) {
	session, err := sm.getSession(ctx, serviceID, appAddr)
	if err != nil {
		return nil, err
	}

	return sm.getOrCreateEndpoints(session), nil
}

// cachedEndpoints is one session's endpoint set plus the height that session
// ends at. The height is what makes eviction possible: a session ID alone says
// nothing about whether it is still current.
type cachedEndpoints struct {
	endpoints  map[domain.EndpointAddr]*endpoint
	sessionEnd uint64
}

// getOrCreateEndpoints returns cached endpoints for the session, or creates and caches them.
func (sm *sessionManager) getOrCreateEndpoints(session *sessiontypes.Session) map[domain.EndpointAddr]*endpoint {
	var sessionEnd uint64
	if session.Header != nil && session.Header.SessionEndBlockHeight > 0 {
		sessionEnd = uint64(session.Header.SessionEndBlockHeight)
	}

	if cached, ok := sm.endpointCache.Load(session.SessionId); ok {
		return cached.(cachedEndpoints).endpoints
	}

	endpoints := endpointsFromSession(session)
	if endpoints == nil {
		endpoints = make(map[domain.EndpointAddr]*endpoint)
	}

	actual, loaded := sm.endpointCache.LoadOrStore(session.SessionId,
		cachedEndpoints{endpoints: endpoints, sessionEnd: sessionEnd})
	if !loaded {
		sm.logger.Debug("endpoints extracted from session",
			"session_id", session.SessionId,
			"session_end", sessionEnd,
			"endpoint_count", len(endpoints),
		)
		sm.evictStaleEndpointsOnRollover(sessionEnd)
	}
	return actual.(cachedEndpoints).endpoints
}

// ConfiguredServices returns the set of service IDs this session manager is configured for.
func (sm *sessionManager) ConfiguredServices() map[domain.ServiceID]struct{} {
	return sm.configuredServices
}

// IsReady returns true if the full node is reachable (block height > 0).
func (sm *sessionManager) IsReady(ctx context.Context) bool {
	height, err := sm.fullNode.GetCurrentBlockHeight(ctx)
	if err != nil {
		sm.logger.Warn("IsReady: failed to get block height",
			"error", err,
		)
		return false
	}
	return height > 0
}
