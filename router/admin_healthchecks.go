package router

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/healthcheck"
)

// HealthCheckAdmin is what the admin API needs from the configured-checks
// manager. healthcheck.CheckOverrides implements it.
type HealthCheckAdmin interface {
	View() []healthcheck.CheckView
	Get(serviceID domain.ServiceID) (healthcheck.CheckView, bool)
	Set(serviceID domain.ServiceID, spec healthcheck.ServiceChecksSpec) (healthcheck.CheckView, error)
	Remove(serviceID domain.ServiceID) bool
	// Persistent reports whether changes reach other replicas and survive a
	// restart.
	Persistent() bool
}

// SetHealthChecks hands the admin API the manager. Without one the
// health-check routes answer 503.
func (a *AdminAPI) SetHealthChecks(m HealthCheckAdmin) { a.healthChecks = m }

type healthCheckReply struct {
	healthcheck.CheckView
	Persisted bool `json:"persisted"`
}

// handleListHealthChecks lists every service's configured health checks: the
// ones running (`effective`), where they come from (`origin`: config, admin
// or none) and the file's (`configured`, what DELETE returns to). The QoS
// plugin's own checks are not listed; they always run.
func (a *AdminAPI) handleListHealthChecks(w http.ResponseWriter, _ *http.Request) {
	if a.healthChecks == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "health checks are not adjustable on this instance")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"services":  a.healthChecks.View(),
		"persisted": a.healthChecks.Persistent(),
		"note":      persistenceNote(a.healthChecks.Persistent()),
	})
}

// handleGetHealthChecks returns one service's configured health checks.
func (a *AdminAPI) handleGetHealthChecks(w http.ResponseWriter, req *http.Request) {
	if a.healthChecks == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "health checks are not adjustable on this instance")
		return
	}
	v, ok := a.healthChecks.Get(domain.ServiceID(req.PathValue("serviceID")))
	if !ok {
		writeJSONError(w, http.StatusNotFound, "service has no configured health checks")
		return
	}
	writeJSON(w, http.StatusOK, healthCheckReply{v, a.healthChecks.Persistent()})
}

// handleSetHealthChecks replaces a service's configured health checks (its
// active_health_checks.local block) on the running gateway.
//
// Body: `{"check_interval": "60s", "checks": [{"name": "…", "type":
// "json_rpc|rest|comet_bft", "method": "…", "path": "…", "body": "…",
// "expected_status_code": 200, "reputation_signal": "minor_error",
// "timeout": "5s"}]}`; only `name` is required. It replaces the file's block
// for the service and is always enabled; an empty `checks` stops the
// service's configured checks. The QoS plugin's own checks run regardless.
// Written to the override store first, so every replica applies it and a
// restart keeps it; with no Redis it is this replica only. 400 for an invalid
// check, 404 for a service this gateway does not serve.
func (a *AdminAPI) handleSetHealthChecks(w http.ResponseWriter, req *http.Request) {
	if a.healthChecks == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "health checks are not adjustable on this instance")
		return
	}
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	var spec healthcheck.ServiceChecksSpec
	if err := json.NewDecoder(req.Body).Decode(&spec); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	v, err := a.healthChecks.Set(serviceID, spec)
	switch {
	case err == nil:
		a.logger.Warn("admin: health checks replaced", "service_id", serviceID, "checks", len(spec.Checks))
		writeJSON(w, http.StatusOK, healthCheckReply{v, a.healthChecks.Persistent()})
	case errors.Is(err, healthcheck.ErrInvalidChecks):
		writeJSONError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, healthcheck.ErrUnknownService):
		writeJSONError(w, http.StatusNotFound, err.Error())
	default:
		a.logger.Error("admin: set health checks", "service_id", serviceID, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to set health checks")
	}
}

// handleDeleteHealthChecks clears a service's admin block; the file's block
// runs again. The body says whether there was one to clear, and `persisted`.
func (a *AdminAPI) handleDeleteHealthChecks(w http.ResponseWriter, req *http.Request) {
	if a.healthChecks == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "health checks are not adjustable on this instance")
		return
	}
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	removed := a.healthChecks.Remove(serviceID)
	writeJSON(w, http.StatusOK, map[string]any{"service_id": serviceID, "removed": removed, "persisted": a.healthChecks.Persistent()})
}
