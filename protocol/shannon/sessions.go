package shannon

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	"golang.org/x/sync/singleflight"

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

	// graceBlocks is the protocol grace period (GracePeriodEndOffsetBlocks from
	// on-chain shared params): the number of blocks after a session's end
	// during which relays for it are still valid. SAGE keeps serving a session
	// through end+graceBlocks and refreshes the next one in the background, so
	// no relay blocks at the boundary and every relay is signed against a
	// session the chain still honours. Zero disables grace (refresh at end).
	graceBlocks atomic.Int64
	// bgRefreshing marks a (service, app) whose next session is already being
	// fetched in the background during grace, so getSession schedules one
	// goroutine per boundary rather than one per request.
	bgRefreshing sync.Map // "serviceID:appAddr" → struct{}

	// refreshGroup coalesces concurrent refreshes of the same session. At a
	// boundary every in-flight relay for a service sees the cached session as
	// expired, and because num_blocks_per_session aligns every service to one
	// boundary, without this they stampede the full node with one GetSession
	// per relay across all services at once — which overruns the node and
	// hangs relays to the relay timeout. With it, one GetSession per
	// (service, app) runs and its result is shared with everyone waiting.
	refreshGroup singleflight.Group

	// failing marks each (serviceID, appAddr) whose last session fetch failed,
	// so a failure is reported once rather than every cycle. A service with no
	// suppliers on the network fails every refresh for as long as that lasts,
	// and the full node's message carries the block height, so comparing the
	// text would re-report it at every session boundary. The first failure is
	// an error, the repeats are debug (with the current message), recovery is
	// info and clears the mark.
	failing sync.Map // "serviceID:appAddr" → struct{}
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

// SetGraceBlocks installs the protocol grace period. Wire time, from the
// on-chain shared params (GracePeriodEndOffsetBlocks).
func (sm *sessionManager) SetGraceBlocks(n int64) {
	if n < 0 {
		n = 0
	}
	sm.graceBlocks.Store(n)
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
	sm.loadGracePeriod(ctx)

	go func() {
		defer safego.Recover(sm.logger, "shannon.blockpoll.loop")
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

// loadGracePeriod reads the on-chain grace period (GracePeriodEndOffsetBlocks)
// once at startup. On failure it stays at zero — the coalesced hard cutover,
// which is correct if less smooth. The value changes rarely (a governance
// param), so it is not re-polled.
func (sm *sessionManager) loadGracePeriod(ctx context.Context) {
	params, err := sm.fullNode.GetSharedParams(ctx)
	if err != nil || params == nil {
		sm.logger.Warn("session grace period: shared params unavailable, grace disabled",
			"error", err)
		return
	}
	grace := int64(params.GetGracePeriodEndOffsetBlocks())
	sm.SetGraceBlocks(grace)
	sm.logger.Info("session grace period loaded", "grace_period_end_offset_blocks", grace)
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
		end := session.Header.SessionEndBlockHeight
		graceEnd := end + sm.graceBlocks.Load()

		switch {
		case currentHeight <= end:
			// Up to and including the session's last block, this IS the current
			// session — nothing newer exists to switch to yet.
			return session, nil

		case currentHeight <= graceEnd:
			// Past the end, within the protocol grace period: the next session
			// now exists and relays for this one are still valid
			// (GracePeriodEndOffsetBlocks). Switch to the new session as soon as
			// it is fetched — refresh it in the background — while serving this
			// one so nothing blocks at the boundary. Once the refresh lands, the
			// cache holds the new session and later calls take the case above.
			sm.scheduleBackgroundRefresh(serviceID, appAddr)
			return session, nil

		default:
			// Past grace: the session is truly expired, refresh synchronously.
			sm.logger.Info("session expired past grace, refreshing",
				"service_id", serviceID,
				"session_id", session.SessionId,
				"session_end_height", end,
				"grace_end_height", graceEnd,
				"current_height", currentHeight,
			)
		}
	}

	return sm.refreshSession(ctx, serviceID, appAddr)
}

// scheduleBackgroundRefresh fetches the next session off the request path
// during the grace period. One goroutine per (service, app) per boundary: the
// bgRefreshing marker drops requests that arrive while a refresh is already in
// flight, and refreshSession itself coalesces the actual GetSession.
func (sm *sessionManager) scheduleBackgroundRefresh(serviceID, appAddr string) {
	key := sessionCacheKey(serviceID, appAddr)
	if _, inFlight := sm.bgRefreshing.LoadOrStore(key, struct{}{}); inFlight {
		return
	}
	safego.Go(sm.logger, "shannon.session.bg_refresh", func() {
		defer sm.bgRefreshing.Delete(key)
		if _, err := sm.refreshSession(context.Background(), serviceID, appAddr); err != nil {
			sm.logger.Debug("session background refresh failed",
				"service_id", serviceID, "app_addr", appAddr, "error", err)
		}
	})
}

// sessionFetchTimeout bounds one coalesced GetSession. It is detached from the
// caller's context so one relay hitting its deadline does not abort the shared
// fetch that its peers are waiting on.
const sessionFetchTimeout = 15 * time.Second

// refreshSession fetches a fresh session from the full node and updates the
// cache, coalescing concurrent callers for the same (service, app) so the full
// node sees one GetSession per boundary rather than one per relay.
func (sm *sessionManager) refreshSession(ctx context.Context, serviceID string, appAddr string) (*sessiontypes.Session, error) {
	key := sessionCacheKey(serviceID, appAddr)
	v, err, _ := sm.refreshGroup.Do(key, func() (interface{}, error) {
		// A relay that reaches its deadline while waiting must not cancel the
		// shared fetch, so the fetch runs on a detached, independently bounded
		// context. The winner populates the cache for everyone.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionFetchTimeout)
		defer cancel()
		return sm.doRefreshSession(fetchCtx, serviceID, appAddr)
	})
	if err != nil {
		return nil, err
	}
	return v.(*sessiontypes.Session), nil
}

// doRefreshSession is the uncoalesced fetch-and-store, run once per boundary
// under refreshGroup.
func (sm *sessionManager) doRefreshSession(ctx context.Context, serviceID string, appAddr string) (*sessiontypes.Session, error) {
	session, err := sm.fullNode.GetSession(ctx, serviceID, appAddr)
	key := sessionCacheKey(serviceID, appAddr)
	if err != nil {
		if _, seen := sm.failing.LoadOrStore(key, struct{}{}); seen {
			sm.logger.Debug("getSession: full node returned error (still failing)",
				"service_id", serviceID, "app_addr", appAddr, "error", err)
		} else {
			sm.logger.Error("getSession: full node returned error",
				"service_id", serviceID,
				"app_addr", appAddr,
				"error", err,
			)
		}
		return nil, fmt.Errorf("getSession: %w", err)
	}
	if _, failed := sm.failing.LoadAndDelete(key); failed {
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
