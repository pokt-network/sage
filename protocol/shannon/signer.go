package shannon

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	"github.com/pokt-network/poktroll/pkg/crypto/rings"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	ring "github.com/pokt-network/ring-go"
	sdk "github.com/pokt-network/shannon-sdk"
)

// ringCacheKey uniquely identifies a cached ring by app address and session end height.
type ringCacheKey struct {
	appAddress       string
	sessionEndHeight uint64
}

// relaySigner signs relay requests using ring signatures.
// The SDK signer is created once at construction and reused across requests.
// Ring instances are cached per (appAddress, sessionEndHeight) to enable
// SignerContext cache hits within a session, since ring composition only
// changes at session boundaries (when delegations may change).
type relaySigner struct {
	sdkSigner     *sdk.Signer
	accountClient *sdk.AccountClient
	ringCache     sync.Map // map[ringCacheKey]*ring.Ring
	logger        *slog.Logger

	// highestSessionEnd is the newest session end height observed. Used to
	// detect session rollover and evict stale per-session cache entries.
	highestSessionEnd atomic.Uint64
	// rolloverMu serializes rollover eviction so a single goroutine prunes
	// per new session.
	rolloverMu sync.Mutex
}

// newRelaySigner creates a relaySigner from a private key hex string.
func newRelaySigner(accountClient *sdk.AccountClient, privateKeyHex string, logger *slog.Logger) (*relaySigner, error) {
	sdkSigner, err := sdk.NewSignerFromHex(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("newRelaySigner: failed to create SDK signer: %w", err)
	}
	return &relaySigner{
		sdkSigner:     sdkSigner,
		accountClient: accountClient,
		logger:        logger.With("component", "signer"),
	}, nil
}

// signRelayRequest signs the relay request using the application's ring signature.
// Returns the signed relay request.
func (rs *relaySigner) signRelayRequest(
	ctx context.Context,
	unsignedReq *servicetypes.RelayRequest,
	app *apptypes.Application,
) (*servicetypes.RelayRequest, error) {
	sessionEndHeight := uint64(unsignedReq.Meta.SessionHeader.SessionEndBlockHeight)

	rs.logger.Debug("signRelayRequest: getting ring",
		"component", "shannon",
		"app_addr", app.Address,
		"session_end_height", sessionEndHeight,
	)

	appRing, err := rs.getOrCreateRing(ctx, app, sessionEndHeight)
	if err != nil {
		rs.logger.Error("signRelayRequest: failed to get ring",
			"component", "shannon",
			"app_addr", app.Address,
			"error", err,
		)
		return nil, fmt.Errorf("signRelayRequest: error getting ring for app %s: %w", app.Address, err)
	}

	// SignOffChainWithRing uses ring-go's hash-cache fast path (cached SignerContext
	// per ring pointer). Safe here because SAGE relay signing is off-chain / not
	// consensus-critical; the signature still verifies identically at the relay miner.
	// The deterministic Signer.Sign path is intentionally not used on this hot path.
	signed, err := rs.sdkSigner.SignOffChainWithRing(ctx, unsignedReq, appRing)
	if err != nil {
		rs.logger.Error("signRelayRequest: failed to sign",
			"component", "shannon",
			"app_addr", app.Address,
			"error", err,
		)
		return nil, fmt.Errorf("signRelayRequest: error signing relay request: %w", err)
	}

	return signed, nil
}

// getOrCreateRing returns a cached ring for the (appAddress, sessionEndHeight) pair,
// or creates and caches a new one if none exists.
//
// Ring composition depends on the app's delegated gateway addresses at the session end
// height. When a new session starts (new sessionEndHeight), a new cache key is created,
// forcing a fresh ring build that picks up any delegation changes.
// Note: the *app object itself may be cached (see Protocol.appCache in relayer.go),
// so mid-session delegation changes won't be reflected until the app cache is refreshed.
func (rs *relaySigner) getOrCreateRing(ctx context.Context, app *apptypes.Application, sessionEndHeight uint64) (*ring.Ring, error) {
	// Bound the per-session caches (ringCache + the SDK's SignerContext cache)
	// by evicting stale entries when a new session is observed.
	rs.evictStaleRingsOnRollover(sessionEndHeight)

	key := ringCacheKey{
		appAddress:       app.Address,
		sessionEndHeight: sessionEndHeight,
	}

	if cached, ok := rs.ringCache.Load(key); ok {
		rs.logger.Debug("getOrCreateRing: cache hit",
			"component", "shannon",
			"app_addr", app.Address,
			"session_end_height", sessionEndHeight,
		)
		return cached.(*ring.Ring), nil
	}

	rs.logger.Debug("getOrCreateRing: cache miss, building ring",
		"component", "shannon",
		"app_addr", app.Address,
		"session_end_height", sessionEndHeight,
	)

	// Determine ring addresses: the app plus any gateway delegates.
	gatewayAddresses := rings.GetRingAddressesAtSessionEndHeight(app, sessionEndHeight)

	ringAddresses := make([]string, 0, 1+len(gatewayAddresses))
	ringAddresses = append(ringAddresses, app.Address)
	if len(gatewayAddresses) == 0 {
		// Self-delegation: use app address twice to form a valid ring.
		ringAddresses = append(ringAddresses, app.Address)
	} else {
		ringAddresses = append(ringAddresses, gatewayAddresses...)
	}

	// Fetch public keys for all ring addresses.
	pubKeys := make([]cryptotypes.PubKey, 0, len(ringAddresses))
	for _, addr := range ringAddresses {
		pubKey, err := rs.accountClient.GetPubKeyFromAddress(ctx, addr)
		if err != nil {
			return nil, fmt.Errorf("getOrCreateRing: failed to get pubkey for %s: %w", addr, err)
		}
		pubKeys = append(pubKeys, pubKey)
	}

	newRing, err := rings.GetRingFromPubKeys(pubKeys)
	if err != nil {
		return nil, fmt.Errorf("getOrCreateRing: failed to create ring: %w", err)
	}

	// Use LoadOrStore to handle concurrent construction: only the winner's ring is kept.
	actual, _ := rs.ringCache.LoadOrStore(key, newRing)
	return actual.(*ring.Ring), nil
}

// evictStaleRingsOnRollover bounds the per-session caches. Both rs.ringCache and
// the SDK's SignerContext cache are keyed per session/ring and would otherwise
// grow without bound as sessions roll over (~hourly). On observing a session end
// height newer than any seen, it drops rings older than the previous session
// (keeping current + previous for in-flight/grace-period requests) and clears the
// SDK's SignerContext cache. The SDK exposes only a full clear; rings still in
// ringCache rebuild their context lazily on the next sign.
func (rs *relaySigner) evictStaleRingsOnRollover(sessionEndHeight uint64) {
	// Fast path (hot): no newer session, nothing to evict. Atomic load only.
	if sessionEndHeight <= rs.highestSessionEnd.Load() {
		return
	}

	rs.rolloverMu.Lock()
	defer rs.rolloverMu.Unlock()

	// Re-check under the lock; another goroutine may have handled this rollover.
	prevHighest := rs.highestSessionEnd.Load()
	if sessionEndHeight <= prevHighest {
		return
	}
	rs.highestSessionEnd.Store(sessionEndHeight)

	// Drop rings older than the previous session (keep current + previous).
	rs.ringCache.Range(func(k, _ any) bool {
		if k.(ringCacheKey).sessionEndHeight < prevHighest {
			rs.ringCache.Delete(k)
		}
		return true
	})

	// Release the SDK's per-ring SignerContexts. Only a full clear is exposed;
	// rings kept in ringCache rebuild their context on next sign.
	rs.sdkSigner.ClearSignerContextCache()
}
