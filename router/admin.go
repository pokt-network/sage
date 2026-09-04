package router

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/methodblock"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/traffic"
	"github.com/pokt-network/sage/tuning"
)

// AdminAPI provides HTTP endpoints for runtime inspection and control.
type AdminAPI struct {
	flags       featureflag.FlagStore
	repService  reputation.Service
	timeline    *reputation.Timeline
	breaker     *circuitbreaker.Breaker
	blocks      *methodblock.Store
	drains      drain.Store
	endpoints   protocol.EndpointProvider
	maxDrain    time.Duration
	qosRegistry *qos.Registry
	tuning      *tuning.Store
	reloader    Reloader
	sampler     *traffic.Sampler
	wsRebinder  WSRebinder
	blocklist   Blocklist
	logger      *slog.Logger
}

// WSRebinder replaces the supplier under every live WebSocket connection of
// a service. shannon.WSRelayer satisfies it.
type WSRebinder interface {
	RebindService(serviceID domain.ServiceID) int
}

// SetWebSocketRebinder installs the relayer the WebSocket rebind route acts
// through. Without one the route answers 501.
func (a *AdminAPI) SetWebSocketRebinder(r WSRebinder) { a.wsRebinder = r }

// NewAdminAPI constructs an AdminAPI.
//
// drains, endpoints, reloader and sampler are nil-safe: with drains nil every
// drain route answers 503 (no store configured); with endpoints nil,
// matched_endpoints and the last-operator check see no live endpoints; with
// reloader nil POST /admin/reload answers 501; with sampler nil every
// /admin/request-sample route answers 503 (no sampler configured). maxDrain
// should already be the resolved ceiling
// (config.AdminConfig.EffectiveMaxDrain()), not a raw possibly-zero config
// value.
func NewAdminAPI(
	flags featureflag.FlagStore,
	repSvc reputation.Service,
	timeline *reputation.Timeline,
	breaker *circuitbreaker.Breaker,
	blocks *methodblock.Store,
	drains drain.Store,
	endpoints protocol.EndpointProvider,
	maxDrain time.Duration,
	qosReg *qos.Registry,
	tuningStore *tuning.Store,
	reloader Reloader,
	sampler *traffic.Sampler,
	logger *slog.Logger,
) *AdminAPI {
	return &AdminAPI{
		flags:       flags,
		repService:  repSvc,
		timeline:    timeline,
		breaker:     breaker,
		blocks:      blocks,
		drains:      drains,
		endpoints:   endpoints,
		maxDrain:    maxDrain,
		qosRegistry: qosReg,
		tuning:      tuningStore,
		reloader:    reloader,
		sampler:     sampler,
		logger:      logger,
	}
}

// RegisterRoutes registers all admin routes on the provided mux.
func (a *AdminAPI) RegisterRoutes(mux *http.ServeMux) {
	// Feature flags
	mux.HandleFunc("GET /admin/flags", a.handleListFlags)
	mux.HandleFunc("PUT /admin/flags/{flag}", a.handleSetFlag)
	mux.HandleFunc("PUT /admin/flags/{flag}/{serviceID}", a.handleSetFlagForService)
	mux.HandleFunc("DELETE /admin/flags/{flag}/{serviceID}", a.handleDeleteFlagForService)

	// Reputation
	mux.HandleFunc("GET /admin/reputation/{serviceID}", a.handleGetReputation)
	mux.HandleFunc("POST /admin/reputation/reset/{serviceID}/{endpoint...}", a.handleResetReputation)

	// Chain state
	mux.HandleFunc("GET /admin/chain-state/{serviceID}", a.handleGetChainState)
	mux.HandleFunc("POST /admin/chain-state/clear/{serviceID}", a.handleClearChainState)

	// Timeline
	mux.HandleFunc("GET /admin/timeline/{serviceID}", a.handleGetTimeline)
	mux.HandleFunc("GET /admin/timeline/{serviceID}/{endpoint...}", a.handleGetTimelineEndpoint)

	// Circuit breaker
	mux.HandleFunc("POST /admin/circuit-breaker/clear/{serviceID}", a.handleClearCircuitBreaker)
	mux.HandleFunc("GET /admin/circuit-breaker/{serviceID}", a.handleGetCircuitBreaker)

	// Method blocks
	mux.HandleFunc("POST /admin/method-blocks/clear/{serviceID}", a.handleClearMethodBlocks)
	mux.HandleFunc("GET /admin/method-blocks/{serviceID}", a.handleGetMethodBlocks)

	// Operator drain
	mux.HandleFunc("POST /admin/reputation/drain/{serviceID}", a.handleSetDrain)
	mux.HandleFunc("GET /admin/reputation/drain/{serviceID}", a.handleGetDrains)
	mux.HandleFunc("DELETE /admin/reputation/drain/{serviceID}/{domain}", a.handleReleaseDrain)

	// Blocked domains: the permanent, global, fleet-wide ban (see package
	// blocklist). Distinct from a drain, which is per service and expires.
	mux.HandleFunc("GET /admin/blocked-domains", a.handleListBlockedDomains)
	mux.HandleFunc("PUT /admin/blocked-domains/{domain}", a.handleSetBlockedDomain)
	mux.HandleFunc("DELETE /admin/blocked-domains/{domain}", a.handleReleaseBlockedDomain)

	// Runtime tuning overrides
	mux.HandleFunc("GET /admin/tuning", a.handleListTuning)
	mux.HandleFunc("GET /admin/tuning/{knob}", a.handleGetTuning)
	mux.HandleFunc("PUT /admin/tuning/{knob}", a.handleSetTuning)
	mux.HandleFunc("PUT /admin/tuning/{knob}/{serviceID}", a.handleSetTuningForService)
	mux.HandleFunc("DELETE /admin/tuning/{knob}", a.handleClearTuning)
	mux.HandleFunc("DELETE /admin/tuning/{knob}/{serviceID}", a.handleClearTuningForService)

	// Config dump and reload
	mux.HandleFunc("GET /admin/config", a.handleGetConfig)
	mux.HandleFunc("POST /admin/reload", a.handleReload)

	// WebSocket
	mux.HandleFunc("POST /admin/websocket/rebind/{serviceID}", a.handleWebSocketRebind)

	// Request-shape sampler
	mux.HandleFunc("GET /admin/request-sample", a.handleListRequestSamples)
	mux.HandleFunc("GET /admin/request-sample/{serviceID}", a.handleGetRequestSample)
}

// --- Feature flag handlers ---

// handleListFlags returns the effective state of every known feature flag.
//
// The response is a JSON object keyed by flag name, each value carrying
// `enabled` (the global setting) and any per-service overrides. Every flag in
// featureflag.DefaultFlags appears, whether or not anyone has set it — the
// point is to show what exists, not only what has been touched.
func (a *AdminAPI) handleListFlags(w http.ResponseWriter, req *http.Request) {
	flags, err := a.flags.GetAll(req.Context())
	if err != nil {
		a.logger.Error("admin: list flags", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to list flags")
		return
	}
	writeJSON(w, http.StatusOK, flags)
}

// handleSetFlag toggles a feature flag globally.
//
// Body: `{"enabled": true}`. With Redis configured the change is shared with
// every other gateway instance, which picks it up within its flag cache TTL;
// without Redis it applies to this instance only.
//
// A per-service override still wins over the global value — clear it with
// DELETE semantics via the flag store, not by setting the global.
func (a *AdminAPI) handleSetFlag(w http.ResponseWriter, req *http.Request) {
	flag := req.PathValue("flag")
	if flag == "" {
		writeJSONError(w, http.StatusBadRequest, "flag name is required")
		return
	}

	var body struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := a.flags.Set(req.Context(), flag, body.Enabled); err != nil {
		a.logger.Error("admin: set flag", "flag", flag, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to set flag")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"flag": flag, "enabled": body.Enabled})
}

// handleSetFlagForService toggles a feature flag for one service only.
//
// Body: `{"enabled": true}`. This is the narrower switch and takes precedence
// over the global setting for that service. Use it to roll a behaviour out to
// one chain before turning it on everywhere.
func (a *AdminAPI) handleSetFlagForService(w http.ResponseWriter, req *http.Request) {
	flag := req.PathValue("flag")
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if flag == "" || serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "flag and serviceID are required")
		return
	}

	var body struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := a.flags.SetForService(req.Context(), flag, serviceID, body.Enabled); err != nil {
		a.logger.Error("admin: set flag for service", "flag", flag, "service", serviceID, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to set flag for service")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"flag":       flag,
		"service_id": string(serviceID),
		"enabled":    body.Enabled,
	})
}

// handleDeleteFlagForService removes a per-service override, so the service
// follows the global value again. Until 2026-09-04 an override could be set
// and never unset: the keys carried no TTL and a reload deletes only globals.
func (a *AdminAPI) handleDeleteFlagForService(w http.ResponseWriter, req *http.Request) {
	flag := req.PathValue("flag")
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if flag == "" || serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "flag and serviceID are required")
		return
	}
	if err := a.flags.Delete(req.Context(), flag, serviceID); err != nil {
		a.logger.Error("admin: delete flag for service", "flag", flag, "service", serviceID, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to delete flag override")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"flag":       flag,
		"service_id": string(serviceID),
		"deleted":    true,
	})
}

// --- Reputation handlers ---

// handleGetReputation returns every reputation state for a service.
//
// **Keys are reputation keys at the configured granularity — per backend URL by
// default — not endpoint addresses.** Several staked suppliers routinely front
// one URL and share its score, so there is often no single endpoint a key
// belongs to.
//
// When the reputation service implements reputation.StateLister the rows are
// reputation.StateView objects: `score` is the effective value the selector
// uses, and `additive` and `penalty` are the two terms it is the sum of (see
// docs/scoring.md §7). `probe_only` means nothing but health checks has graded
// the key — its score is evidence about the probe payload, not about client
// traffic. Otherwise the response falls back to the older JSON object of score
// keys to numeric scores.
func (a *AdminAPI) handleGetReputation(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}

	if lister, ok := a.repService.(reputation.StateLister); ok {
		states, err := lister.GetStates(req.Context(), serviceID)
		if err != nil {
			a.logger.Error("admin: get reputation", "service", serviceID, "error", err)
			writeJSONError(w, http.StatusInternalServerError, "failed to get reputation scores")
			return
		}
		writeJSON(w, http.StatusOK, states)
		return
	}

	scores, err := a.repService.GetScores(req.Context(), serviceID)
	if err != nil {
		a.logger.Error("admin: get reputation", "service", serviceID, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to get reputation scores")
		return
	}

	// Keys are reputation keys at the configured granularity (per-URL by
	// default), not necessarily endpoint addresses.
	writeJSON(w, http.StatusOK, scores)
}

// handleResetReputation returns one endpoint to the initial score.
//
// The reset spans every RPC type: scores are kept per (identity, RPC type), but
// an operator resetting an endpoint means the endpoint, not whichever protocol
// they happened to name.
//
// Reach for this when an endpoint was penalised for something since fixed and
// you do not want to wait for probation traffic to rehabilitate it.
func (a *AdminAPI) handleResetReputation(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	endpoint := domain.EndpointAddr(req.PathValue("endpoint"))
	if serviceID == "" || endpoint == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID and endpoint are required")
		return
	}

	if err := a.repService.ResetScore(req.Context(), serviceID, endpoint); err != nil {
		a.logger.Error("admin: reset reputation", "service", serviceID, "endpoint", endpoint, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to reset score")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"service_id": string(serviceID),
		"endpoint":   string(endpoint),
		"status":     "reset",
	})
}

// --- Timeline handlers ---

// handleGetTimeline returns the recent reputation events for every endpoint of
// a service, newest last.
//
// This is the "why is this endpoint not getting traffic" endpoint: each event
// carries the signal, the reason code, and the score before and after. The
// timeline is a bounded in-memory ring per endpoint, so it answers for recent
// history, not for all time — and it is per instance, not shared through Redis.
func (a *AdminAPI) handleGetTimeline(w http.ResponseWriter, req *http.Request) {
	serviceID := req.PathValue("serviceID")
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}

	events := a.timeline.GetAll(serviceID + ":")
	if events == nil {
		events = []reputation.TimelineEvent{}
	}
	writeJSON(w, http.StatusOK, events)
}

// handleGetTimelineEndpoint returns the reputation events for a single
// endpoint. The endpoint is the trailing path segment and may contain slashes,
// since an endpoint address embeds a URL.
func (a *AdminAPI) handleGetTimelineEndpoint(w http.ResponseWriter, req *http.Request) {
	serviceID := req.PathValue("serviceID")
	endpoint := req.PathValue("endpoint")
	if serviceID == "" || endpoint == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID and endpoint are required")
		return
	}

	key := serviceID + ":" + endpoint
	events := a.timeline.Get(key)
	if events == nil {
		events = []reputation.TimelineEvent{}
	}
	writeJSON(w, http.StatusOK, events)
}

// --- Circuit breaker handlers ---

// handleClearCircuitBreaker releases every circuit-broken domain for a service
// and reports how many were cleared.
//
// It clears the break itself. Escalation history deliberately survives, so a
// domain let back in and immediately failing again is still treated as a repeat
// offender rather than a first offence.
func (a *AdminAPI) handleClearCircuitBreaker(w http.ResponseWriter, req *http.Request) {
	serviceID := req.PathValue("serviceID")
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}

	count := a.breaker.Clear(serviceID)
	writeJSON(w, http.StatusOK, map[string]any{
		"service_id":      serviceID,
		"cleared_domains": count,
		"message":         "circuit breaker state cleared",
	})
}

// handleGetCircuitBreaker returns the domains currently circuit-broken for a
// service, keyed by domain, each with the reason and when the break expires.
//
// An empty object means nothing is broken. Breaks expire lazily, so a domain
// listed here whose expiry has passed is not actually locked out — the same
// state is exported as the sage_circuit_breaker_state metric, computed at
// scrape time.
func (a *AdminAPI) handleGetCircuitBreaker(w http.ResponseWriter, req *http.Request) {
	serviceID := req.PathValue("serviceID")
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}

	broken := a.breaker.GetBroken(serviceID)
	if broken == nil {
		broken = map[string]circuitbreaker.BrokenState{}
	}
	writeJSON(w, http.StatusOK, broken)
}

// --- Method block handlers ---

// handleGetMethodBlocks lists the hosts currently blocked from receiving a
// method for a service, with each block's expiry. A block with an empty
// method is a host-level block (every method). An empty array means nothing
// is blocked. The same state is exported as the sage_method_blocks metric.
func (a *AdminAPI) handleGetMethodBlocks(w http.ResponseWriter, req *http.Request) {
	serviceID := req.PathValue("serviceID")
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}
	blocks := []methodblock.Block{}
	if a.blocks != nil {
		if active := a.blocks.Active(serviceID); active != nil {
			blocks = active
		}
	}
	writeJSON(w, http.StatusOK, blocks)
}

// handleClearMethodBlocks drops every method block for a service. It exists
// so an operator can undo a false positive; the escalation count goes with
// the marks, so the next mark is a first mark.
func (a *AdminAPI) handleClearMethodBlocks(w http.ResponseWriter, req *http.Request) {
	serviceID := req.PathValue("serviceID")
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}
	cleared := 0
	if a.blocks != nil {
		cleared = a.blocks.Clear(serviceID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service_id": serviceID,
		"cleared":    cleared,
		"message":    "method blocks cleared",
	})
}

// --- Config handler ---

// handleGetConfig returns the gateway's effective runtime configuration:
// resolved feature flags, registered services and their QoS plugins.
//
// It reports what the process is actually running, which is not the same as the
// YAML on disk — defaults have been applied, and flags may have been changed
// through this API since startup.
func (a *AdminAPI) handleGetConfig(w http.ResponseWriter, req *http.Request) {
	flags, err := a.flags.GetAll(req.Context())
	if err != nil {
		a.logger.Error("admin: get config flags", "error", err)
		// Non-fatal: continue with empty flags.
		flags = map[string]featureflag.FlagState{}
	}

	plugins := a.qosRegistry.Plugins()
	qosInfo := make(map[string]string, len(plugins))
	for id, p := range plugins {
		qosInfo[string(id)] = pluginTypeName(p)
	}

	// Derive services list from QoS registry (the authoritative source of
	// configured services available to the admin endpoint).
	services := make([]string, 0, len(plugins))
	for id := range plugins {
		services = append(services, string(id))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"services": services,
		"qos":      qosInfo,
		"flags":    flags,
	})
}

// pluginTypeName returns a human-readable type name for a QoS plugin.
func pluginTypeName(p qos.Plugin) string {
	if p == nil {
		return "nil"
	}
	// Use fmt.Sprintf with %T for the concrete type name.
	// Avoid importing reflect; this is display-only.
	return formatTypeName(p)
}
