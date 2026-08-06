package router

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
)

// AdminAPI provides HTTP endpoints for runtime inspection and control.
type AdminAPI struct {
	flags       featureflag.FlagStore
	repService  reputation.Service
	timeline    *reputation.Timeline
	breaker     *circuitbreaker.Breaker
	qosRegistry *qos.Registry
	logger      *slog.Logger
}

// NewAdminAPI constructs an AdminAPI.
func NewAdminAPI(
	flags featureflag.FlagStore,
	repSvc reputation.Service,
	timeline *reputation.Timeline,
	breaker *circuitbreaker.Breaker,
	qosReg *qos.Registry,
	logger *slog.Logger,
) *AdminAPI {
	return &AdminAPI{
		flags:       flags,
		repService:  repSvc,
		timeline:    timeline,
		breaker:     breaker,
		qosRegistry: qosReg,
		logger:      logger,
	}
}

// RegisterRoutes registers all admin routes on the provided mux.
func (a *AdminAPI) RegisterRoutes(mux *http.ServeMux) {
	// Feature flags
	mux.HandleFunc("GET /admin/flags", a.handleListFlags)
	mux.HandleFunc("PUT /admin/flags/{flag}", a.handleSetFlag)
	mux.HandleFunc("PUT /admin/flags/{flag}/{serviceID}", a.handleSetFlagForService)

	// Reputation
	mux.HandleFunc("GET /admin/reputation/{serviceID}", a.handleGetReputation)
	mux.HandleFunc("POST /admin/reputation/reset/{serviceID}/{endpoint...}", a.handleResetReputation)

	// Timeline
	mux.HandleFunc("GET /admin/timeline/{serviceID}", a.handleGetTimeline)
	mux.HandleFunc("GET /admin/timeline/{serviceID}/{endpoint...}", a.handleGetTimelineEndpoint)

	// Circuit breaker
	mux.HandleFunc("POST /admin/circuit-breaker/clear/{serviceID}", a.handleClearCircuitBreaker)
	mux.HandleFunc("GET /admin/circuit-breaker/{serviceID}", a.handleGetCircuitBreaker)

	// Config dump
	mux.HandleFunc("GET /admin/config", a.handleGetConfig)
}

// --- Feature flag handlers ---

func (a *AdminAPI) handleListFlags(w http.ResponseWriter, req *http.Request) {
	flags, err := a.flags.GetAll(req.Context())
	if err != nil {
		a.logger.Error("admin: list flags", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to list flags")
		return
	}
	writeJSON(w, http.StatusOK, flags)
}

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

// --- Reputation handlers ---

func (a *AdminAPI) handleGetReputation(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
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

// --- Config handler ---

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
