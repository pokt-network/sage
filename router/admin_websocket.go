package router

import (
	"net/http"

	"github.com/pokt-network/sage/domain"
)

// handleWebSocketRebind replaces the supplier under every live WebSocket
// connection of a service, without closing any client. Each bridge selects
// a supplier it has not used yet (falling back to any if that leaves
// nothing), replays its live subscriptions to it, and keeps serving; the
// client keeps its socket and its subscription ids. Bridges that cannot be
// rebound — no reachable supplier, or their per-connection rebind limit is
// spent — close with 1012 so the client reconnects.
//
// Two uses: a drill, and moving live connections off an operator that was
// just drained (`POST /admin/reputation/drain/...` affects new selections
// only; existing sockets stay where they are until this is called).
//
// Answers `{"service_id", "bridges"}` with how many live bridges were asked;
// zero is a valid answer for a service with no WebSocket clients. 501 when
// this build has no WebSocket relayer wired.
func (a *AdminAPI) handleWebSocketRebind(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}
	if a.wsRebinder == nil {
		writeJSONError(w, http.StatusNotImplemented, "websocket rebind is not available in this build")
		return
	}
	n := a.wsRebinder.RebindService(serviceID)
	a.logger.Warn("admin: websocket rebind requested", "service_id", string(serviceID), "bridges", n)
	writeJSON(w, http.StatusOK, map[string]any{
		"service_id": string(serviceID),
		"bridges":    n,
	})
}
