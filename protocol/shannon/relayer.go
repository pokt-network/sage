package shannon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sdk "github.com/pokt-network/shannon-sdk"
	sdktypes "github.com/pokt-network/shannon-sdk/types"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
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
	// metrics records supplier-attributable events (blacklists, relay miner
	// errors). Never nil — see SetMetrics.
	metrics supplierMetrics
	logger  *slog.Logger
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

	signer, err := newRelaySigner(fullNode.AccountClient(), cfg.Gateway.GatewayPrivateKeyHex, logger)
	if err != nil {
		return nil, fmt.Errorf("shannon.New: failed to create relay signer: %w", err)
	}

	relayTimeout := cfg.Gateway.Defaults.Timeout.RelayTimeout
	if relayTimeout == 0 {
		relayTimeout = 30 * time.Second
	}

	httpClient := &http.Client{Timeout: relayTimeout}

	return &Protocol{
		fullNode:    fullNode,
		sessions:    sm,
		signer:      signer,
		bl:          newBlacklist(),
		gatewayAddr: cfg.Gateway.GatewayAddress,
		ownedApps:   ownedApps,
		httpClient:  httpClient,
		grpc:        newGRPCRelayTransport(cfg.Protocol.GRPCMode, httpClient, logger.With("component", "shannon_grpc")),
		metrics:     noopSupplierMetrics{},
		logger:      logger.With("component", "shannon_protocol"),
	}, nil
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
		p.logger.Error("SendRelay: endpoint not found in session",
			"component", "shannon",
			"service_id", serviceID,
			"endpoint_addr", endpointAddr,
			"session_id", session.SessionId,
		)
		return nil, domain.NewRelayError(domain.ErrValidation,
			fmt.Sprintf("endpoint %s not found in session", endpointAddr), nil, false)
	}

	url, err := ep.GetURL(payload.RPCType())
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

// AvailableEndpoints returns the list of endpoint addresses for the service/rpcType combination,
// excluding any blacklisted suppliers.
func (p *Protocol) AvailableEndpoints(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType) (domain.EndpointAddrList, error) {
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
		p.logger.Error("AvailableEndpoints: failed to get session endpoints",
			"component", "shannon",
			"service_id", serviceID,
			"app_addr", appAddr,
			"error", err,
		)
		return nil, fmt.Errorf("AvailableEndpoints: %w", err)
	}

	// Filter by RPC type support and blacklist in one pass — no intermediate map.
	result := make(domain.EndpointAddrList, 0, len(endpoints))
	var blacklisted int
	for addr, ep := range endpoints {
		if rpcType != domain.RPCTypeUnknown {
			if _, err := ep.GetURL(rpcType); err != nil {
				continue
			}
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
