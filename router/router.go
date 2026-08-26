// Package router wires HTTP routes to the relay middleware chain and admin API.
// It uses the Go 1.22+ enhanced net/http.ServeMux with method+pattern routing.
package router

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
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

// Router is the main HTTP server. It dispatches requests to the relay chain
// or admin API and owns the http.Server lifecycle.
type Router struct {
	mux       *http.ServeMux
	server    *http.Server
	chain     relay.Handler
	sessions  protocol.SessionManager
	wsRelayer WebSocketOpener // optional; if nil, WS upgrade requests 503
	logger    *slog.Logger
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

	// Relay endpoints — the main traffic path.
	mux.HandleFunc("POST /v1", r.handleRelay)
	mux.HandleFunc("POST /v1/{path...}", r.handleRelay)
	mux.HandleFunc("GET /v1", r.handleMaybeWebSocket)
	mux.HandleFunc("GET /v1/{path...}", r.handleMaybeWebSocket)

	// Health / readiness.
	mux.HandleFunc("GET /health", r.handleHealth)
	mux.HandleFunc("GET /ready/{service}", r.handleReadyService)
	mux.HandleFunc("GET /ready", r.handleReadyAll)

	addr := fmt.Sprintf(":%d", cfg.Port)
	r.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  withDefault(cfg.ReadTimeout, 30*time.Second),
		WriteTimeout: withDefault(cfg.WriteTimeout, 30*time.Second),
		IdleTimeout:  withDefault(cfg.IdleTimeout, 120*time.Second),
	}

	return r
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

// handleMaybeWebSocket routes GET /v1[/...] requests to the WebSocket relayer
// when the client is attempting an upgrade, and to the normal relay chain
// otherwise. If no WS relayer is configured, upgrade attempts return 503.
func (r *Router) handleMaybeWebSocket(w http.ResponseWriter, req *http.Request) {
	if !isWebSocketUpgrade(req) {
		r.handleRelay(w, req)
		return
	}
	if r.wsRelayer == nil {
		http.Error(w, "websocket relays not configured", http.StatusServiceUnavailable)
		return
	}
	serviceID := domain.ServiceID(req.Header.Get("Target-Service-Id"))
	if serviceID == "" {
		http.Error(w, "missing Target-Service-Id header", http.StatusBadRequest)
		return
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

	if err := r.chain.HandleRelay(ctx); err != nil {
		r.logger.Error("relay chain error", "service", ctx.ServiceID, "endpoint", ctx.Endpoint, "error", err)
		r.writeRelayError(rw, ctx, err)
		return
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
		}

		rw.SetStatusCode(status)
		if writeErr := rw.Write(body); writeErr != nil {
			r.logger.Error("failed to write relay response", "error", writeErr)
		}
	}
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
	r.logger.Error("relay error", "service", ctx.ServiceID, "error", err)

	if ctx.RPCType == domain.RPCTypeJSONRPC || isJSONRPCRequest(ctx) {
		var id json.RawMessage = []byte("null")
		if len(ctx.Payloads) > 0 {
			id = ctx.Payloads[0].JSONRPCID()
		}
		// domain.ClientMessage, not err.Error(): the cause chain carries the
		// operator's own infrastructure (a dial failure names the fullnode's
		// host and port), and this gateway authenticates no one. The chain is
		// already in the log line above, which is where it is useful.
		renderJSONRPCError(rw, -32603, domain.ClientMessage(err), id)
		return
	}

	renderJSONError(rw, http.StatusBadGateway, domain.ClientMessage(err))
}

// handleHealth returns 200 when the protocol layer is ready, 503 otherwise.
func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	if r.sessions.IsReady(req.Context()) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
}

// handleReadyService checks whether a specific service is configured and ready.
func (r *Router) handleReadyService(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("service"))
	services := r.sessions.ConfiguredServices()
	if _, ok := services[serviceID]; !ok {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("service %q not found", serviceID))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ready":   true,
		"service": string(serviceID),
	})
}

// handleReadyAll reports readiness for every configured service.
func (r *Router) handleReadyAll(w http.ResponseWriter, req *http.Request) {
	services := r.sessions.ConfiguredServices()
	result := make(map[string]bool, len(services))
	for svc := range services {
		result[string(svc)] = true
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ready":    true,
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
