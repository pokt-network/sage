package router

import (
	"fmt"
	"net/http"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// handleGetChainState reads what a service's plugin believes about its chain:
// the perceived head, and the latest height each endpoint reported. It is the
// read half of the chain-state route — the reset existed without it, so an
// operator watching the chain-view spread jump to the whole chain height had
// no way to ask which endpoint did it.
func (a *AdminAPI) handleGetChainState(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}
	plugin := a.qosRegistry.Get(serviceID)
	if plugin == nil {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("service %q is not registered", serviceID))
		return
	}
	out := map[string]any{"service_id": string(serviceID)}
	if viewer, ok := plugin.(qos.ChainViewer); ok {
		view := viewer.ChainView()
		out["perceived"] = view.Perceived
		out["highest"] = view.Highest
		out["lowest"] = view.Lowest
		out["endpoints"] = view.Endpoints
	}
	if lister, ok := plugin.(qos.EndpointHeightLister); ok {
		out["heights"] = lister.EndpointHeights()
	} else {
		out["heights"] = []qos.EndpointHeight{}
		out["message"] = "plugin tracks no per-endpoint heights"
	}
	writeJSON(w, http.StatusOK, out)
}

// handleClearChainState discards the QoS state a service's plugin has
// learned: block consensus (perceived height, external floor) and its
// per-endpoint QoS store (block heights, chain-id observations, archival
// marks — see qos.StateResetter). Reputation, circuit breaker and method
// blocks are untouched; they have their own clear routes.
//
// A plugin that implements qos.StateResetter answers `{"reset": true}`. One
// that does not — it keeps no chain state worth discarding — still answers
// 200 with `{"reset": false, "message": ...}`: asking is not a mistake. An
// unregistered serviceID is the one case that is an error: 404, since there
// is nothing to reset and no looser store to silently no-op against.
func (a *AdminAPI) handleClearChainState(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}

	plugin := a.qosRegistry.Get(serviceID)
	if plugin == nil {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("service %q is not registered", serviceID))
		return
	}

	resetter, ok := plugin.(qos.StateResetter)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{
			"service_id": string(serviceID),
			"reset":      false,
			"message":    "plugin keeps no chain state",
		})
		return
	}

	resetter.ResetState()
	a.logger.Warn("admin: chain state reset", "service_id", string(serviceID))

	writeJSON(w, http.StatusOK, map[string]any{
		"service_id": string(serviceID),
		"reset":      true,
	})
}
