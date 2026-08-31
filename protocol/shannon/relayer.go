package shannon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sdk "github.com/pokt-network/shannon-sdk"
	sdktypes "github.com/pokt-network/shannon-sdk/types"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
	"github.com/pokt-network/sage/protocol"
)

// Compile-time interface assertions.
var (
	_ protocol.Relayer          = (*Protocol)(nil)
	_ protocol.EndpointProvider = (*Protocol)(nil)
	_ protocol.SessionManager   = (*Protocol)(nil)
	_ protocol.SupplierManager  = (*Protocol)(nil)
)

// fullNodeIface is the internal interface used by Protocol.
// Using an interface here enables testing without a live full node.
type fullNodeIface interface {
	GetSession(ctx context.Context, serviceID string, appAddr string) (*sessiontypes.Session, error)
	GetApp(ctx context.Context, appAddr string) (*apptypes.Application, error)
	GetCurrentBlockHeight(ctx context.Context) (int64, error)
	GetSharedParams(ctx context.Context) (*sharedtypes.Params, error)
	ValidateRelayResponse(supplierAddr string, responseBz []byte) (*servicetypes.RelayResponse, error)
	AccountClient() *sdk.AccountClient
}

// relaySignerIface is the signing interface, enabling mock injection in tests.
type relaySignerIface interface {
	signRelayRequest(ctx context.Context, req *servicetypes.RelayRequest, app *apptypes.Application) (*servicetypes.RelayRequest, error)
}

// Protocol implements the Shannon relay protocol for SAGE.
// It handles session management, relay signing, HTTP transport, and response validation.
//
// See apps.go for app management (pickApp, getApp, buildOwnedApps) and transport.go
// for HTTP transport details.
type Protocol struct {
	fullNode    fullNodeIface
	sessions    *sessionManager
	signer      relaySignerIface
	bl          *blacklist
	gatewayAddr string
	// ownedApps maps serviceID → list of app addresses for centralized gateway mode.
	ownedApps map[domain.ServiceID][]string
	// appCache caches Application objects by address to avoid gRPC lookups per relay.
	// See getApp in apps.go for the delegation-lifecycle caveat.
	appCache   sync.Map // appAddr (string) → *apptypes.Application
	httpClient *http.Client
	// grpc carries gRPC relays, which cannot ride the HTTP path.
	grpc *grpcRelayTransport
	// blockedDomains is the operator domain ban. An atomic.Pointer so
	// SetBlockedDomains can swap in a rebuilt list without a lock on the
	// AvailableEndpoints/SendRelay read path. A nil *domainBlocklist (the zero
	// value, and what Load returns before anything is ever Stored) blocks
	// nothing and is nil-safe throughout — see domain_blocklist.go.
	blockedDomains atomic.Pointer[domainBlocklist]
	// drains is the operator-drain store consulted next to blockedDomains in
	// AvailableEndpoints. Nil until SetDrains is called, and nil-safe at the
	// call site — see drain.go.
	drains drain.Store
	// metrics records supplier-attributable events (blacklists, relay miner
	// errors). Never nil — see SetMetrics.
	metrics supplierMetrics
	// rpcFallbacks is the per-service rpc_type_fallbacks mapping, consulted by
	// endpointURL wherever a relay is addressed. Nil when no service sets one.
	rpcFallbacks rpcFallbackTable
	logger       *slog.Logger
}

// New constructs a Protocol from the given configuration.
// Only centralized gateway mode (owned apps) is supported for MVP.
func New(cfg config.Config, logger *slog.Logger) (*Protocol, error) {
	fullNode, err := NewFullNode(cfg.FullNode, logger)
	if err != nil {
		return nil, fmt.Errorf("shannon.New: failed to create full node: %w", err)
	}

	ownedApps, err := buildOwnedApps(fullNode, cfg.Gateway.OwnedAppsPrivateKeys, logger)
	if err != nil {
		return nil, fmt.Errorf("shannon.New: failed to build owned apps map: %w", err)
	}

	configuredServices := make(map[domain.ServiceID]struct{})
	for svcID := range ownedApps {
		configuredServices[svcID] = struct{}{}
	}

	sm := newSessionManager(fullNode, configuredServices, logger)

	signer, err := newRelaySigner(fullNode.pubKeys, cfg.Gateway.GatewayPrivateKeyHex, logger)
	if err != nil {
		return nil, fmt.Errorf("shannon.New: failed to create relay signer: %w", err)
	}

	relayTimeout := cfg.Gateway.Defaults.Timeout.RelayTimeout
	if relayTimeout == 0 {
		relayTimeout = 30 * time.Second
	}

	httpClient := &http.Client{Timeout: relayTimeout}

	blockedDomains, err := newDomainBlocklist(cfg.Gateway.BlockedDomains)
	if err != nil {
		return nil, fmt.Errorf("shannon.New: %w", err)
	}
	for _, e := range blockedDomains.entries() {
		logger.Warn("blocked domain configured: no endpoint here will be selected or health-checked",
			"component", "shannon", "domain", e[0], "rpc_type", e[1])
	}

	p := &Protocol{
		fullNode:     fullNode,
		sessions:     sm,
		signer:       signer,
		bl:           newBlacklist(),
		gatewayAddr:  cfg.Gateway.GatewayAddress,
		ownedApps:    ownedApps,
		httpClient:   httpClient,
		grpc:         newGRPCRelayTransport(cfg.Protocol.GRPCMode, httpClient, logger.With("component", "shannon_grpc")),
		metrics:      noopSupplierMetrics{},
		rpcFallbacks: buildRPCFallbacks(cfg.Gateway.AllServices()),
		logger:       logger.With("component", "shannon_protocol"),
	}
	p.blockedDomains.Store(blockedDomains)
	return p, nil
}

// SendRelay sends a relay for the given service to the specified endpoint.
// Flow: fetch session → look up endpoint → build request → sign → POST → validate → deserialize.
func (p *Protocol) SendRelay(
	ctx context.Context,
	serviceID domain.ServiceID,
	endpointAddr domain.EndpointAddr,
	payload domain.Payload,
) (*domain.Response, error) {
	start := time.Now()

	appAddr, err := p.pickApp(serviceID)
	if err != nil {
		p.logger.Error("SendRelay: no app available for service",
			"component", "shannon",
			"service_id", serviceID,
			"error", err,
		)
		return nil, domain.NewRelayError(domain.ErrValidation, "no app available for service", err, false)
	}

	session, err := p.sessions.getSession(ctx, string(serviceID), appAddr)
	if err != nil {
		p.logger.Error("SendRelay: failed to get session",
			"component", "shannon",
			"service_id", serviceID,
			"app_addr", appAddr,
			"error", err,
		)
		return nil, domain.NewRelayError(domain.ErrTransport, "failed to get session", err, true)
	}

	endpoints := p.sessions.getOrCreateEndpoints(session)

	ep, ok := endpoints[endpointAddr]
	if !ok {
		// The session rolled over between selection and send: the endpoint was
		// picked from the previous session's list and this one no longer has
		// it. Not a client or supplier fault — no relay was sent — so it is
		// retryable and carries ErrEndpointsStale, which tells Retry to
		// reselect from the fresh session rather than try this list's other
		// (equally stale) members. Warn, not Error: it is an expected race at
		// every session boundary, roughly one per second under load.
		p.logger.Warn("SendRelay: endpoint not in current session (rollover), reselecting",
			"component", "shannon",
			"service_id", serviceID,
			"endpoint_addr", endpointAddr,
			"session_id", session.SessionId,
		)
		return nil, domain.NewRelayError(domain.ErrTransport,
			fmt.Sprintf("endpoint %s not in current session after rollover", endpointAddr),
			domain.ErrEndpointsStale, true)
	}

	url, err := p.endpointURL(serviceID, ep, payload.RPCType())
	if err != nil {
		p.logger.Error("SendRelay: endpoint does not support RPC type",
			"component", "shannon",
			"service_id", serviceID,
			"endpoint_addr", endpointAddr,
			"rpc_type", payload.RPCType(),
			"error", err,
		)
		return nil, domain.NewRelayError(domain.ErrCapability, "endpoint does not support requested RPC type", err, false)
	}

	// A banned domain is refused here as well as at selection. Selection is
	// where the ban does its work; this is the guarantee — anything holding an
	// endpoint address from before the ban, or reaching SendRelay by a path
	// that never consulted AvailableEndpoints, still cannot send to it. Not
	// retryable: another attempt at the same endpoint has the same answer.
	if p.blockedDomains.Load().IsBlocked(url, payload.RPCType()) {
		p.logger.Warn("SendRelay: refusing a blocked domain",
			"component", "shannon",
			"service_id", serviceID,
			"endpoint_addr", endpointAddr,
			"rpc_type", payload.RPCType(),
		)
		return nil, domain.NewRelayError(domain.ErrValidation,
			"endpoint is at a blocked domain", nil, false)
	}

	// Build the HTTP request to embed in the relay payload. The SDK serializes
	// the URL and verb verbatim, and the relay miner replays both against its
	// backend — so a REST or CometBFT path dropped here is a request the
	// backend never sees.
	httpReq, err := http.NewRequestWithContext(ctx, payloadHTTPMethod(payload), payloadURL(url, payload), bytes.NewReader(payload.Bytes()))
	if err != nil {
		return nil, domain.NewRelayError(domain.ErrTransport, "failed to build HTTP request", err, false)
	}
	httpReq.Header.Set("Content-Type", payloadContentType(payload))

	// Serialize the HTTP request into the relay payload format.
	_, payloadBz, err := sdktypes.SerializeHTTPRequest(httpReq)
	if err != nil {
		return nil, domain.NewRelayError(domain.ErrProtocol, "failed to serialize HTTP request", err, false)
	}

	// Build the unsigned relay request with session metadata.
	unsignedReq := &servicetypes.RelayRequest{
		Meta: servicetypes.RelayRequestMetadata{
			SessionHeader:           session.Header,
			SupplierOperatorAddress: ep.Supplier(),
		},
		Payload: payloadBz,
	}

	// Get the app for ring signing (cached to avoid gRPC per relay).
	app, err := p.getApp(ctx, appAddr)
	if err != nil {
		p.logger.Error("SendRelay: failed to fetch app for signing",
			"component", "shannon",
			"service_id", serviceID,
			"app_addr", appAddr,
			"error", err,
		)
		return nil, domain.NewRelayError(domain.ErrProtocol, "failed to fetch app for signing", err, true)
	}

	signedReq, err := p.signer.signRelayRequest(ctx, unsignedReq, app)
	if err != nil {
		p.logger.Error("SendRelay: failed to sign relay request",
			"component", "shannon",
			"service_id", serviceID,
			"endpoint_addr", endpointAddr,
			"error", err,
		)
		return nil, domain.NewRelayError(domain.ErrProtocol, "failed to sign relay request", err, false)
	}

	// Marshal the signed relay request to wire format.
	reqBz, err := signedReq.Marshal()
	if err != nil {
		return nil, domain.NewRelayError(domain.ErrProtocol, "failed to marshal relay request", err, false)
	}

	// Send the relay. gRPC does not go over the miner's HTTP path: that one
	// rebuilds the request as HTTP/1.1, which a gRPC backend refuses. Only the
	// miner's relay *service* reaches its HTTP/2 backend client.
	// Kept for the validation-failure log below: a supplier returning a
	// non-RelayResponse body is usually relaying a backend failure verbatim,
	// and without the status that error is unattributable. gRPC has no such
	// status of its own — a failure there is already a grpc-status error.
	var respBz []byte
	httpStatus := 0
	if payload.RPCType() == domain.RPCTypeGRPC {
		respBz, err = p.grpc.send(ctx, url, reqBz, payload.RPCType())
		if err != nil {
			p.logger.Error("SendRelay: gRPC relay failed",
				"component", "shannon",
				"service_id", serviceID,
				"endpoint_addr", endpointAddr,
				"url", url,
				"error", err,
			)
			return nil, domain.NewRelayError(domain.ErrTransport, "gRPC relay failed", err, true)
		}
	} else {
		httpResp, err := p.sendHTTP(ctx, url, reqBz, payload.RPCType())
		if err != nil {
			p.logger.Error("SendRelay: HTTP relay failed",
				"component", "shannon",
				"service_id", serviceID,
				"endpoint_addr", endpointAddr,
				"url", url,
				"error", err,
			)
			return nil, domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", err, true)
		}
		defer httpResp.Body.Close()
		httpStatus = httpResp.StatusCode

		respBz, err = io.ReadAll(httpResp.Body)
		if err != nil {
			return nil, domain.NewRelayError(domain.ErrTransport, "failed to read relay response body", err, true)
		}

		// A non-2xx status is the relay MINER erroring before it produced a
		// signed RelayResponse (overload -> 503, its own 500, payload too large
		// -> 413): the body is an error page, not a RelayResponse. Do not try to
		// unmarshal it — that fails and blacklisted the supplier for 15 minutes
		// over a transient miner error. Grade it as a retryable endpoint error
		// so retry reaches another supplier and the score records a recoverable
		// penalty (via the score middleware), while the supplier stays in the
		// pool. The client sees the kind, not the status.
		if httpStatus < 200 || httpStatus >= 300 {
			p.logger.Debug("SendRelay: relay miner returned HTTP error",
				"component", "shannon",
				"service_id", serviceID,
				"endpoint_addr", endpointAddr,
				"http_status", httpStatus,
			)
			return nil, domain.NewRelayError(domain.ErrEndpoint, "upstream endpoint unavailable", nil, true)
		}
	}

	// Verify the supplier's signature over the response. See
	// FullNode.ValidateRelayResponse: the key is the one belonging to the
	// supplier we selected, which is what makes these bytes attributable.
	relayResp, err := p.fullNode.ValidateRelayResponse(ep.Supplier(), respBz)

	// Read the miner's error report first — it survives a validation failure,
	// and branching on err would discard it.
	p.trackRelayMinerError(serviceID, endpointAddr, ep.Supplier(), relayResp)

	if err != nil {
		return nil, p.handleValidationFailure(serviceID, endpointAddr, ep.Supplier(), err, "http_status", httpStatus)
	}

	// Deserialize the relay response payload into an HTTP response.
	poktHTTPResp, err := sdktypes.DeserializeHTTPResponse(relayResp.Payload)
	if err != nil {
		return nil, domain.NewRelayError(domain.ErrProtocol, "failed to deserialize relay response", err, false)
	}

	latency := time.Since(start)
	// Guard: even disabled slog.Debug boxes its variadic args (~6 interface
	// allocations per call) — skip entirely unless Debug is active.
	if p.logger.Enabled(ctx, slog.LevelDebug) {
		p.logger.Debug("SendRelay: complete",
			"component", "shannon",
			"service_id", serviceID,
			"endpoint_addr", endpointAddr,
			"http_status", poktHTTPResp.StatusCode,
			"latency_ms", latency.Milliseconds(),
		)
	}

	return &domain.Response{
		Body:           poktHTTPResp.BodyBz,
		HTTPStatusCode: int(poktHTTPResp.StatusCode),
		Latency:        latency,
		EndpointAddr:   endpointAddr,
		Headers:        grpcResponseHeaders(payload.RPCType(), poktHTTPResp),
	}, nil
}

// LatestBlockHeight returns the newest chain head the block poller has seen,
// or 0 before the first successful poll — and likewise 0 when no session
// manager is wired, so a heightless Protocol reads as "never expires" rather
// than panicking.
func (p *Protocol) LatestBlockHeight() int64 {
	if p.sessions == nil {
		return 0
	}
	return p.sessions.LatestBlockHeight()
}

// StartBlockPoller starts the background block height poller for session cache invalidation.
func (p *Protocol) StartBlockPoller(ctx context.Context) {
	p.sessions.StartBlockPoller(ctx)
}

// StopBlockPoller stops the background block height poller.
func (p *Protocol) StopBlockPoller() {
	p.sessions.StopBlockPoller()
}

// AvailableEndpoints returns the endpoint addresses for the service/rpcType
// combination that may be handed traffic: registrations that serve the RPC
// type, minus the operator domain ban, the operator drain and the supplier
// blacklist.
func (p *Protocol) AvailableEndpoints(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType) (domain.EndpointAddrList, error) {
	return p.endpoints(ctx, serviceID, rpcType, true)
}

// RegisteredEndpoints returns every registration for the service/rpcType
// combination that serves the RPC type, whether or not it is currently banned,
// drained or blacklisted. It is what a dry-run drain counts against: an
// operator already excluded for another reason would otherwise report zero
// matches, which reads as "no such operator" rather than "already out".
func (p *Protocol) RegisteredEndpoints(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType) (domain.EndpointAddrList, error) {
	return p.endpoints(ctx, serviceID, rpcType, false)
}

// endpoints is AvailableEndpoints and RegisteredEndpoints: the session's
// registrations for the RPC type, with the exclusions applied when filtered.
func (p *Protocol) endpoints(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType, filtered bool) (domain.EndpointAddrList, error) {
	appAddr, err := p.pickApp(serviceID)
	if err != nil {
		p.logger.Error("AvailableEndpoints: no app for service",
			"component", "shannon",
			"service_id", serviceID,
			"error", err,
		)
		return nil, fmt.Errorf("AvailableEndpoints: %w", err)
	}

	endpoints, err := p.sessions.getEndpoints(ctx, string(serviceID), appAddr)
	if err != nil {
		// Debug, not error: the session manager already reported the cause
		// once, and this path runs every health-check cycle for every service
		// — a service with no suppliers on the network would otherwise log
		// the same failure twice every 30 seconds for as long as it lasts.
		p.logger.Debug("AvailableEndpoints: failed to get session endpoints",
			"component", "shannon",
			"service_id", serviceID,
			"app_addr", appAddr,
			"error", err,
		)
		return nil, fmt.Errorf("AvailableEndpoints: %w", err)
	}

	// Filter by RPC type support, operator domain ban, operator drain and
	// blacklist in one pass — no intermediate map.
	//
	// The ban and the drain are applied here, at the one place endpoints are
	// handed out, so they cover relay selection (and therefore retry, hedge
	// and batch), WebSocket bind and health checks without each of them
	// knowing either exists.
	// rpc_type_fallbacks is a pool-level switch, as in PATH: the fallback
	// type is used only when no supplier in the session staked the requested
	// one. Applied per supplier it would add REST-only suppliers to a
	// json_rpc pool that has plenty of json_rpc ones — which on mainnet
	// answered tron JSON-RPC with 405 from their REST root.
	lookupType := rpcType
	if rpcType != domain.RPCTypeUnknown && !anyStakes(endpoints, rpcType) {
		if fallback := p.rpcFallbacks.resolve(serviceID, rpcType); fallback != "" {
			lookupType = fallback
		}
	}

	blockedDomains := p.blockedDomains.Load()
	result := make(domain.EndpointAddrList, 0, len(endpoints))
	var blacklisted, blocked, drained int
	for addr, ep := range endpoints {
		url := ""
		if rpcType != domain.RPCTypeUnknown {
			u, err := ep.GetURL(lookupType)
			if err != nil {
				continue
			}
			url = u
		} else {
			url = ep.PublicURL()
		}
		if !filtered {
			result = append(result, addr)
			continue
		}
		if blockedDomains.IsBlocked(url, rpcType) {
			blocked++
			continue
		}
		if d := p.drains; d != nil && d.Drained(serviceID, operatorOf(url), rpcType) {
			drained++
			continue
		}
		if p.bl.IsBlacklisted(serviceID, ep.Supplier()) {
			blacklisted++
			continue
		}
		result = append(result, addr)
	}

	if p.logger.Enabled(ctx, slog.LevelDebug) {
		p.logger.Debug("AvailableEndpoints: result",
			"component", "shannon",
			"service_id", serviceID,
			"rpc_type", rpcType,
			"available", len(result),
			"blacklisted", blacklisted,
			"blocked_domain", blocked,
			"drained", drained,
		)
	}

	return result, nil
}

// ConfiguredServices returns the set of service IDs this protocol is configured for.
func (p *Protocol) ConfiguredServices() map[domain.ServiceID]struct{} {
	return p.sessions.ConfiguredServices()
}

// IsReady returns true if the protocol is ready to handle requests.
func (p *Protocol) IsReady(ctx context.Context) bool {
	return p.sessions.IsReady(ctx)
}

// BlacklistSupplier adds a supplier to the blacklist for a service.
func (p *Protocol) BlacklistSupplier(serviceID domain.ServiceID, addr string) {
	p.logger.Debug("blacklisting supplier",
		"component", "shannon",
		"service_id", serviceID,
		"supplier_addr", addr,
	)
	p.bl.BlacklistSupplier(serviceID, addr)
}

// UnblacklistSupplier removes a supplier from the blacklist.
func (p *Protocol) UnblacklistSupplier(serviceID domain.ServiceID, addr string) bool {
	removed := p.bl.UnblacklistSupplier(serviceID, addr)
	if removed {
		p.logger.Debug("supplier removed from blacklist",
			"component", "shannon",
			"service_id", serviceID,
			"supplier_addr", addr,
		)
	}
	return removed
}

// IsBlacklisted returns true if the supplier is currently blacklisted for the service.
func (p *Protocol) IsBlacklisted(serviceID domain.ServiceID, addr string) bool {
	return p.bl.IsBlacklisted(serviceID, addr)
}
