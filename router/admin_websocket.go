package router

import (
	"net/http"
	"strconv"

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

// handleWebSocketClients reports which clients drive WebSocket traffic and
// which suppliers served them, over the last one to two hours: per client
// address, the connections it opened, how many supplier tenures it ended
// itself within 30s (quick_client_closes), and per supplier — service,
// operator, owner — the tenures, their total seconds and the frames that
// supplier pushed. Busiest clients first.
//
// A client address cannot be a metric label, and this is what the supplier
// metrics cannot show: a client that reconnects until it lands on one owner,
// then holds that connection, is steering paid relays to that owner. Such a
// client carries a "shopping" object naming the owner it settles on: at least
// 5 tenures with other owners it closed within 30s, and at least 80% (and 10
// minutes) of its connected time with that one. shopping_clients counts them.
// Query: service (optional) narrows to one service, limit (default 50) caps
// the clients returned, shopping=true returns only flagged clients. 501 when
// this build has no WebSocket relayer wired.
func (a *AdminAPI) handleWebSocketClients(w http.ResponseWriter, req *http.Request) {
	if a.wsClients == nil {
		writeJSONError(w, http.StatusNotImplemented, "websocket client report is not available in this build")
		return
	}
	limit := 0
	if raw := req.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "limit must be a non-negative integer")
			return
		}
		limit = n
	}
	onlyShopping := req.URL.Query().Get("shopping") == "true"
	writeJSON(w, http.StatusOK, a.wsClients(domain.ServiceID(req.URL.Query().Get("service")), limit, onlyShopping))
}

// handleWebSocketNotificationSamples returns a sample of the hashes suppliers
// pushed in WebSocket subscription notifications: one notification in 100 per
// service, operator, owner and topic, the last 200 of each, on this replica.
// Each names a transaction (a pending transaction, or the one a log belongs
// to) or a block (a new head), for checking offline against the chain: every
// notification is a relay the supplier is paid for, and one that names
// nothing the chain knows is padding no frame count can reveal. Query: service
// (optional). 501 when this build has no WebSocket relayer wired.
func (a *AdminAPI) handleWebSocketNotificationSamples(w http.ResponseWriter, req *http.Request) {
	if a.wsSamples == nil {
		writeJSONError(w, http.StatusNotImplemented, "websocket notification samples are not available in this build")
		return
	}
	writeJSON(w, http.StatusOK, a.wsSamples(domain.ServiceID(req.URL.Query().Get("service"))))
}
