package router

import (
	"context"
	"net/http"
	"strconv"

	"github.com/pokt-network/sage/autodrain"
	"github.com/pokt-network/sage/domain"
)

// AutoDrainEvents reads the auto-drain engine's decision log.
// autodrain.EventLog satisfies it.
type AutoDrainEvents interface {
	Recent(ctx context.Context, svc domain.ServiceID, limit int) ([]autodrain.Event, error)
}

// SetAutoDrainEvents installs the decision log GET /admin/auto-drain/events
// reads. Without one the route answers 503.
func (a *AdminAPI) SetAutoDrainEvents(events AutoDrainEvents) { a.autoDrainEvents = events }

// handleAutoDrainEvents lists the auto-drain engine's decisions, newest first:
// every drain it set and every one it would have set in shadow mode or chose
// not to, with the evidence (collapse picks, share, attempts, success rate,
// the vouched alternative). ?service= filters, ?limit= caps (default 100).
func (a *AdminAPI) handleAutoDrainEvents(w http.ResponseWriter, req *http.Request) {
	if a.autoDrainEvents == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "the auto-drain engine is not running on this instance")
		return
	}
	limit := 100
	if s := req.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			writeJSONError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = n
	}
	events, err := a.autoDrainEvents.Recent(req.Context(), domain.ServiceID(req.URL.Query().Get("service")), limit)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "reading the decision log: "+err.Error())
		return
	}
	if events == nil {
		events = []autodrain.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}
