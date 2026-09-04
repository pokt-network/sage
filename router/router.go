// Package router wires HTTP routes to the relay middleware chain and admin API.
// It uses the Go 1.22+ enhanced net/http.ServeMux with method+pattern routing.
package router

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/relay"
)

// WebSocketOpener is the minimal contract the router needs from a WS relayer.
// The concrete implementation in production is *shannon.WSRelayer; tests
// substitute a fake. Keeping this as an interface avoids an import cycle
// (router → shannon) and lets the router remain protocol-agnostic.
type WebSocketOpener interface {
	Open(ctx context.Context, serviceID domain.ServiceID, req *http.Request, w http.ResponseWriter) error
}

// ClientMetrics records the client-facing HTTP status of each relay request —
// what the caller/edge sees, distinct from sage_relay_total's per-attempt view.
// metrics.Recorder satisfies it; nil disables recording.
type ClientMetrics interface {
	RecordClientRequest(serviceID domain.ServiceID, status int)
}

// Warmup reports whether the gateway can steer endpoint selection yet — i.e.
// health-check results have populated reputation for the configured services.
// A nil Warmup on the Router means not gated (always ready).
type Warmup interface {
	Warm() bool
}

// Router is the main HTTP server. It dispatches requests to the relay chain
// or admin API and owns the http.Server lifecycle.
type Router struct {
	mux       *http.ServeMux
	server    *http.Server
	chain     relay.Handler
	sessions  protocol.SessionManager
	wsRelayer WebSocketOpener // optional; if nil, WS upgrade requests 503
	// warmup gates /ready: nil means not gated. Set via SetWarmup at wire time.
	warmup Warmup
	// clientMetrics records the client-facing status per relay request. Nil
	// disables recording. Set via SetClientMetrics at wire time.
	clientMetrics ClientMetrics
	// serviceRPCTypes reports what a service declares in config, for the
	// WebSocket path, which bypasses the middleware chain and so the Validate
	// gate. Nil means ungated. Set via SetServiceRPCTypes at wire time.
	serviceRPCTypes func(domain.ServiceID) (rpcTypes []string, configured bool)
	logger          *slog.Logger
}

// EndpointLister is the optional capability behind per-service readiness:
// a session with at least one endpoint. protocol.Protocol satisfies it; a
// session manager that does not makes /ready/{service} answer on
// configuration alone.
type EndpointLister interface {
	AvailableEndpoints(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType) (domain.EndpointAddrList, error)
}

// SetClientMetrics installs the client-facing request-status recorder. Wire
// time only; nil leaves client-status recording off.
func (r *Router) SetClientMetrics(m ClientMetrics) { r.clientMetrics = m }

// SetWarmup installs the readiness warm-up gate consulted by /ready. Wire time
// only; nil leaves /ready ungated (session readiness alone).
func (r *Router) SetWarmup(w Warmup) { r.warmup = w }

// SetServiceRPCTypes installs the config lookup the WebSocket path uses to
// refuse, before upgrading, a service that is not configured or does not
// declare websocket. Wire time only; nil leaves the path ungated.
func (r *Router) SetServiceRPCTypes(fn func(domain.ServiceID) ([]string, bool)) {
	r.serviceRPCTypes = fn
}

// New creates a Router and registers all routes.
// wsRelayer may be nil — in that case, WebSocket upgrade requests receive a
// 503 and the normal HTTP relay path is preserved.
//
// The admin API is deliberately not mounted here. It has no authentication, so
// serving it from the same mux published an unauthenticated control plane on
// whatever port relays arrive on. It gets its own listener; see cmd/sagegw and
// config.AdminConfig.
func New(
	cfg config.RouterConfig,
	chain relay.Handler,
	sessions protocol.SessionManager,
	wsRelayer WebSocketOpener,
	logger *slog.Logger,
) *Router {
	mux := http.NewServeMux()

	r := &Router{
		mux:       mux,
		chain:     chain,
		sessions:  sessions,
		wsRelayer: wsRelayer,
		logger:    logger,
	}

	// Relay endpoints — the main traffic path. Registered without a method,
	// as PATH's are: a REST service is addressed with whatever verb its API
	// uses, and a method-qualified pattern answered PUT and DELETE with 405.
	mux.HandleFunc("/v1", r.handleV1)
	mux.HandleFunc("/v1/{path...}", r.handleV1)

	// Health / readiness. The split follows PATH's and Kubernetes' meanings:
	// /health and /livez are liveness (200 whenever the process serves),
	// /healthz and /ready are readiness (503 until the gateway can relay).
	// /health used to be readiness here, which made a liveness probe written
	// for PATH restart pods during a full-node outage — the one dependency's
	// outage became a restart loop, which is exactly what /livez exists to
	// prevent.
	mux.HandleFunc("GET /health", r.handleLive)
	mux.HandleFunc("GET /livez", r.handleLive)
	mux.HandleFunc("GET /healthz", r.handleHealthz)
	mux.HandleFunc("GET /ready/{service}", r.handleReadyService)
	mux.HandleFunc("GET /ready", r.handleReadyAll)

	addr := fmt.Sprintf(":%d", cfg.Port)
	r.server = &http.Server{
		Addr:    addr,
		Handler: mux,
		// PATH's defaults, so a config that sets none behaves the same behind
		// either gateway. The 30s write timeout this used to carry cut off
		// slow archival calls PATH would have served.
		ReadTimeout:    withDefault(cfg.ReadTimeout, 60*time.Second),
		WriteTimeout:   withDefault(cfg.WriteTimeout, 120*time.Second),
		IdleTimeout:    withDefault(cfg.IdleTimeout, 180*time.Second),
		MaxHeaderBytes: withDefaultInt(cfg.MaxRequestHeaderBytes, defaultMaxHeaderBytes),
	}

	return r
}

// defaultMaxHeaderBytes is PATH's default request header cap.
const defaultMaxHeaderBytes = 2_000_000

// corsAllowHeaders is what a browser may send to /v1: the JSON-RPC content
// type, the two headers SAGE itself reads, and the two PATH allows so a dapp
// written against it keeps working (Authorization for portal keys,
// solana-client for the Solana web3 library).
const corsAllowHeaders = "Content-Type, Target-Service-Id, RPC-Type, Authorization, solana-client"

// setCORS grants the request's own origin, as PATH does: this gateway has no
// notion of a client, so it has no basis on which to refuse one. The grant is
// only stamped when there is an Origin to mirror, and Vary says so, so a
// shared cache never serves one origin's grant to another.
func setCORS(w http.ResponseWriter, req *http.Request) {
	origin := req.Header.Get("Origin")
	if origin == "" {
		return
	}
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
	h.Set("Access-Control-Allow-Headers", corsAllowHeaders)
	h.Add("Vary", "Origin")
}

// handleV1 is the front door for everything under /v1: a CORS preflight is
// answered here, a WebSocket upgrade goes to the WS relayer, and everything
// else — any verb — goes down the relay chain.
func (r *Router) handleV1(w http.ResponseWriter, req *http.Request) {
	setCORS(w, req)
	if req.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if isWebSocketUpgrade(req) {
		r.handleWebSocket(w, req)
		return
	}
	r.handleRelay(w, req)
}

// Start binds the HTTP server and blocks until it stops.
// It returns http.ErrServerClosed on graceful shutdown.
func (r *Router) Start() error {
	r.logger.Info("router listening", "addr", r.server.Addr)
	return r.server.ListenAndServe()
}

// Shutdown performs a graceful shutdown, waiting for in-flight requests.
func (r *Router) Shutdown(ctx context.Context) error {
	return r.server.Shutdown(ctx)
}

// handleWebSocket hands an upgrade request to the WebSocket relayer once the
// gate the HTTP path gets from Validate has been applied here too: the service
// must be configured and must declare websocket. Refusing before the upgrade
// is what PATH does; upgrading first and failing with a 502 from inside the
// bridge told the client nothing about what it got wrong. If no WS relayer is
// configured, upgrade attempts return 503.
func (r *Router) handleWebSocket(w http.ResponseWriter, req *http.Request) {
	if r.wsRelayer == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "websocket relays not configured")
		return
	}
	serviceID := domain.ServiceID(req.Header.Get("Target-Service-Id"))
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing Target-Service-Id header")
		return
	}
	if r.serviceRPCTypes != nil {
		types, configured := r.serviceRPCTypes(serviceID)
		if !configured {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("service %q is not configured", serviceID))
			return
		}
		if !slices.Contains(types, string(domain.RPCTypeWebSocket)) {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("websocket not supported for service %q", serviceID))
			return
		}
	}
	if err := r.wsRelayer.Open(req.Context(), serviceID, req, w); err != nil {
		// Post-upgrade errors are surfaced via WS close codes by the bridge;
		// pre-upgrade errors (e.g. flag off, no endpoints) have already
		// written an HTTP response via wsRelayer.Open.
		r.logger.Error("ws open failed", "service_id", serviceID, "err", err)
	}
}

// isWebSocketUpgrade returns true when the request's headers indicate a
// WebSocket upgrade handshake.
func isWebSocketUpgrade(req *http.Request) bool {
	if !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") {
		return false
	}
	// "Connection" may be a comma-separated list; check for the "upgrade" token.
	conn := strings.ToLower(req.Header.Get("Connection"))
	return strings.Contains(conn, "upgrade")
}

// stripMountPrefix rewrites the request URL to the path the *service* should
// see, dropping the gateway's own "/v1" mount point. The wildcard capture is
// authoritative, so this stays correct if the mount point ever moves.
//
// This matters beyond cosmetics: JSON-RPC ignores the path, but REST and
// CometBFT requests are *addressed* by it, and relaying "/v1/status" instead
// of "/status" is a 404 at the supplier's backend. RPC type detection reads
// the path too, so an un-stripped prefix also mis-detects the RPC type.
func stripMountPrefix(req *http.Request) *http.Request {
	servicePath := "/" + strings.TrimPrefix(req.PathValue("path"), "/")
	if req.URL.Path == servicePath {
		return req
	}
	// Copy the URL rather than mutating the caller's: the query string and
	// everything else carry over untouched. RawPath is cleared so the escaped
	// form is re-derived from the new Path.
	u := *req.URL
	u.Path = servicePath
	u.RawPath = ""
	shallow := *req
	shallow.URL = &u
	return &shallow
}

// handleRelay is the main relay handler. It:
//  1. Wraps the http.ResponseWriter with HTTPResponseWriter.
//  2. Creates a relay.Context.
//  3. Invokes the middleware chain.
//  4. On success the chain (or a Write middleware) commits the response.
//  5. On error, writes an appropriate error response.
func (r *Router) handleRelay(w http.ResponseWriter, req *http.Request) {
	req = stripMountPrefix(req)

	rw := relay.NewHTTPResponseWriter(w)
	ctx := relay.NewContext(req.Context(), req, r.logger, rw)

	// Record the client-facing status once, whichever path answers — this is
	// what an edge dashboard sees, unlike sage_relay_total's per-attempt view.
	if r.clientMetrics != nil {
		defer func() { r.clientMetrics.RecordClientRequest(ctx.ServiceID, rw.Status()) }()
	}

	if err := r.chain.HandleRelay(ctx); err != nil {
		// A retry verdict with a response in hand is not a failure to
		// deliver: no further attempt did better, so the upstream's own
		// answer stands — the chain's `execution reverted`, the node's
		// `block not found`. Replacing it with a gateway-made -32603 hid the
		// real error from the client on ~1% of a canary's requests.
		if ctx.Response != nil && errors.Is(err, domain.ErrRetryVerdict) {
			r.logger.Info("relay: delivering the last upstream response after a retry verdict",
				"service", ctx.ServiceID, "endpoint", ctx.Endpoint, "verdict", err)
		} else {
			r.logger.Error("relay chain error", "service", ctx.ServiceID, "endpoint", ctx.Endpoint, "error", err)
			r.writeRelayError(rw, ctx, err)
			return
		}
	}

	// If the chain wrote the response itself (via ctx.Writer.Write) we are done.
	// If ctx.Response is set but not yet written, write it now.
	if ctx.Response == nil {
		r.logger.Warn("relay chain returned nil response", "service", ctx.ServiceID, "endpoint", ctx.Endpoint, "degraded", ctx.Degraded)
	}
	if ctx.Response != nil {
		status := ctx.Response.HTTPStatusCode
		if status == 0 {
			status = http.StatusOK
		}
		// Emitted here, not where the degradation happens: SelectEndpoint runs
		// inside the batch and hedge fan-outs, so it cannot know whether the
		// attempt it degraded is the one being answered with. By now every merge
		// has run and ctx.Degraded is settled.
		if ctx.Degraded {
			rw.SetHeader(relay.HeaderDegraded, "true")
		}

		body := ctx.Response.Body
		if ctx.RPCType == domain.RPCTypeGRPC {
			body = finishGRPCResponse(rw, ctx)
		} else if ct := responseContentType(ctx.RPCType, body); ct != "" {
			rw.SetHeader("Content-Type", ct)
		}

		rw.SetStatusCode(status)
		if writeErr := rw.Write(body); writeErr != nil {
			r.logger.Error("failed to write relay response", "error", writeErr)
		}
	}
}

// responseContentType names the body's media type. JSON-RPC and CometBFT are
// JSON by definition; a REST reply is JSON when it looks like it and is left
// for net/http to sniff otherwise, since the upstream's own header is not
// carried through the relay. Without this every relay response was sniffed
// as text/plain, which PATH never did.
func responseContentType(rpcType domain.RPCType, body []byte) string {
	switch rpcType {
	case domain.RPCTypeJSONRPC, domain.RPCTypeCometBFT:
		return "application/json"
	case domain.RPCTypeGRPC, domain.RPCTypeWebSocket:
		return ""
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		return "application/json"
	}
	return ""
}

// finishGRPCResponse types the reply and, for a gRPC-Web client, appends the
// trailer frame carrying grpc-status.
//
// The framing matters to the client, not to us: a gRPC-Web client reads its
// status from a final in-body frame and rejects a reply that has none, while a
// native gRPC client reads HTTP trailers and must not be handed an extra frame
// it would decode as another message. The relay carries only a body, so
// whichever the client asked for has to be rebuilt here.
func finishGRPCResponse(rw relay.ResponseWriter, ctx *relay.Context) []byte {
	requestType := ctx.HTTPRequest.Header.Get("Content-Type")
	rw.SetHeader("Content-Type", requestType)

	code, message, _ := ctx.Response.GRPCStatus()
	// Mirror the status as headers too: harmless for gRPC-Web, and the only
	// channel a native client behind an HTTP/1.1 hop has left.
	rw.SetHeader("Grpc-Status", strconv.Itoa(code))
	if message != "" {
		rw.SetHeader("Grpc-Message", message)
	}

	if !strings.HasPrefix(strings.ToLower(requestType), "application/grpc-web") {
		return ctx.Response.Body
	}
	return append(ctx.Response.Body, encodeGRPCWebTrailers(code, message)...)
}

// encodeGRPCWebTrailers builds the gRPC-Web trailer frame: flag 0x80, then a
// big-endian length, then an HTTP-header-shaped block.
func encodeGRPCWebTrailers(code int, message string) []byte {
	trailers := "grpc-status:" + strconv.Itoa(code) + "\r\n"
	if message != "" {
		trailers += "grpc-message:" + message + "\r\n"
	}

	frame := make([]byte, 5+len(trailers))
	frame[0] = 0x80
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(trailers)))
	copy(frame[5:], trailers)
	return frame
}

// writeRelayError writes an error response appropriate for the request type.
//
// It writes through the relay response writer, not the raw http.ResponseWriter,
// so it shares the write-once guard with any middleware that already answered.
// A middleware rejecting a request (parse, validate, batch) writes its own terse
// body and returns the error; this then renders as a no-op rather than
// concatenating a second JSON object onto the first. When nothing in the chain
// wrote — a deep failure like a send error — this is the write that answers the
// client.
func (r *Router) writeRelayError(rw relay.ResponseWriter, ctx *relay.Context, err error) {
	status := statusForError(err)
	// domain.ClientMessage, not err.Error(): the cause chain carries the
	// operator's own infrastructure (a dial failure names the fullnode's
	// host and port), and this gateway authenticates no one. The chain is
	// already in the log line above, which is where it is useful.
	message := domain.ClientMessage(err)

	if ctx.RPCType == domain.RPCTypeJSONRPC || isJSONRPCRequest(ctx) {
		var id json.RawMessage = []byte("null")
		if len(ctx.Payloads) > 0 {
			id = ctx.Payloads[0].JSONRPCID()
		}
		renderJSONRPCError(rw, status, -32603, message, id)
		return
	}

	renderJSONError(rw, status, message)
}

// statusForError maps a gateway-made failure to the HTTP status a client
// sees. The body carries the JSON-RPC code either way; the status is what a
// load balancer, a dashboard and a client's retry policy branch on, and a
// 200 on every failure — which is what this used to be for JSON-RPC — hid
// them from all three. PATH answers 500; SAGE keeps the standard -32603
// rather than PATH's -31001 (docs/path-compat.md, gateway errors).
func statusForError(err error) int {
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	var re *domain.RelayError
	if errors.As(err, &re) {
		switch re.Kind {
		case domain.ErrValidation:
			return http.StatusBadRequest
		case domain.ErrRateLimit:
			return http.StatusTooManyRequests
		}
	}
	return http.StatusInternalServerError
}

// handleHealthz is readiness in PATH's spelling: 200 when the protocol layer
// has a session, 503 otherwise. A load-balancer rule written for PATH's
// /healthz takes a pod out on the same condition here.
func (r *Router) handleHealthz(w http.ResponseWriter, req *http.Request) {
	if r.sessions.IsReady(req.Context()) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
}

// handleReadyService answers whether one service can be served: configured,
// and — when the session manager can say — holding a session with at least
// one endpoint. 503 otherwise, as on PATH; it used to be 200 for anything
// configured, which made a per-service readiness probe unable to fail.
func (r *Router) handleReadyService(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("service"))
	services := r.sessions.ConfiguredServices()
	if _, ok := services[serviceID]; !ok {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("service %q not found", serviceID))
		return
	}
	ready, count, reason := r.serviceReady(req.Context(), serviceID)
	body := map[string]any{
		"ready":          ready,
		"service":        string(serviceID),
		"endpoint_count": count,
	}
	if reason != "" {
		body["error"] = reason
	}
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, body)
}

// serviceReady reports a configured service's readiness. With no
// EndpointLister the answer is the session layer's, which is all that used
// to be known.
func (r *Router) serviceReady(ctx context.Context, serviceID domain.ServiceID) (ready bool, count int, reason string) {
	lister, ok := r.sessions.(EndpointLister)
	if !ok {
		return r.sessions.IsReady(ctx), 0, ""
	}
	endpoints, err := lister.AvailableEndpoints(ctx, serviceID, domain.RPCTypeUnknown)
	if err != nil {
		return false, 0, "no session"
	}
	if len(endpoints) == 0 {
		return false, 0, "no endpoints"
	}
	return true, len(endpoints), ""
}

// handleLive answers 200 unconditionally: the process is up and serving.
// Readiness (sessions, full node) is /healthz and /ready.
func (r *Router) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyAll is the readiness endpoint. Unlike /healthz (a session exists)
// it reflects the ability to SERVE: it is 503 until the session layer is ready
// AND the warm-up gate reports reputation has warmed for the configured
// services. A fresh or rolled pod would otherwise be put into the Service
// while selection is still blind, serving failures until it warmed. Point the
// Kubernetes readinessProbe (and a startupProbe with a generous
// failureThreshold) here.
func (r *Router) handleReadyAll(w http.ResponseWriter, req *http.Request) {
	warm := r.warmup == nil || r.warmup.Warm()
	ready := warm && r.sessions.IsReady(req.Context())

	services := r.sessions.ConfiguredServices()
	result := make(map[string]bool, len(services))
	for svc := range services {
		result[string(svc)] = ready
	}
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{
		"ready":    ready,
		"warm":     warm,
		"services": result,
	})
}

// isJSONRPCRequest returns true if the relay context looks like a JSON-RPC
// request, even when RPCType has not been set yet (e.g. parse error early).
func isJSONRPCRequest(ctx *relay.Context) bool {
	if ctx.HTTPRequest == nil {
		return false
	}
	ct := ctx.HTTPRequest.Header.Get("Content-Type")
	return ct == "application/json" || ctx.HTTPRequest.Method == http.MethodPost
}

// withDefault returns d if dur is zero.
func withDefault(dur, d time.Duration) time.Duration {
	if dur == 0 {
		return d
	}
	return dur
}

// withDefaultInt returns d if n is zero.
func withDefaultInt(n, d int) int {
	if n == 0 {
		return d
	}
	return n
}
