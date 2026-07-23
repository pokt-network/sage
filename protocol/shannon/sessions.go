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
	fullNode           fullNodeIface
	sessionCache       sync.Map // "serviceID:appAddr" → *sessiontypes.Session
	endpointCache      sync.Map // sessionID (string) → map[domain.EndpointAddr]*endpoint
	configuredServices map[domain.ServiceID]struct{}
	logger             *slog.Logger

	// latestBlockHeight is updated by a background poller.
	// Used to decide when a cached session has expired.
	latestBlockHeight atomic.Int64
	stopPoller        chan struct{}
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
				sm.pollBlockHeight(ctx)
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
	if err != nil {
		sm.logger.Error("getSession: full node returned error",
			"service_id", serviceID,
			"app_addr", appAddr,
			"error", err,
		)
		return nil, fmt.Errorf("getSession: %w", err)
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

// getOrCreateEndpoints returns cached endpoints for the session, or creates and caches them.
func (sm *sessionManager) getOrCreateEndpoints(session *sessiontypes.Session) map[domain.EndpointAddr]*endpoint {
	if cached, ok := sm.endpointCache.Load(session.SessionId); ok {
		return cached.(map[domain.EndpointAddr]*endpoint)
	}

	endpoints := endpointsFromSession(session)
	if endpoints == nil {
		endpoints = make(map[domain.EndpointAddr]*endpoint)
	}

	actual, loaded := sm.endpointCache.LoadOrStore(session.SessionId, endpoints)
	if !loaded {
		sm.logger.Debug("endpoints extracted from session",
			"session_id", session.SessionId,
			"endpoint_count", len(endpoints),
		)
	}
	return actual.(map[domain.EndpointAddr]*endpoint)
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
