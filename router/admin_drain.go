package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
)

// drainRequest is the POST /admin/reputation/drain/{serviceID} body.
type drainRequest struct {
	Domain   string `json:"domain"`
	Duration string `json:"duration"`
	RPCType  string `json:"rpc_type,omitempty"`
	Reason   string `json:"reason,omitempty"`
	DryRun   bool   `json:"dry_run,omitempty"`
}

// drainResponse is the POST /admin/reputation/drain/{serviceID} response.
type drainResponse struct {
	ServiceID        string        `json:"service_id"`
	Domain           string        `json:"domain"`
	RPCType          string        `json:"rpc_type"`
	Applied          bool          `json:"applied"`
	Released         bool          `json:"released"`
	DryRun           bool          `json:"dry_run"`
	MatchedEndpoints int           `json:"matched_endpoints"`
	DrainedUntil     *time.Time    `json:"drained_until,omitempty"`
	PropagationError string        `json:"propagation_error,omitempty"`
	ActiveDrains     []drain.Entry `json:"active_drains"`
}

// handleSetDrain applies or releases an operator drain for one service.
//
// Body: `{"domain","duration","rpc_type"?,"reason"?,"dry_run"?}`. domain is
// the operator's registrable domain and is required; duration is parsed with
// time.ParseDuration and capped by admin_config.max_drain — a request above
// the ceiling is refused (400), not clamped, so an operator who typed 72h
// learns the limit instead of silently getting a day. duration "0" releases
// any existing drain instead of installing one. A drain that would leave a
// service with no endpoint outside the target operator is refused (409): the
// pool-collapse guard that protects selection everywhere else applies here
// too. dry_run computes matched_endpoints and the last-operator check without
// calling the store. A drain.ErrPropagation from the store (Redis reachable
// from no other replica) still answers 200, with propagation_error set: the
// drain — or, for duration 0, the release — applies on this instance
// regardless, and 500 would describe a local state that did not happen.
func (a *AdminAPI) handleSetDrain(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}

	var body drainRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	targetDomain := strings.ToLower(strings.TrimSpace(body.Domain))
	if targetDomain == "" {
		writeJSONError(w, http.StatusBadRequest, "domain is required")
		return
	}

	duration, err := time.ParseDuration(body.Duration)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid duration %q: %v", body.Duration, err))
		return
	}
	if duration < 0 {
		writeJSONError(w, http.StatusBadRequest, "duration must not be negative")
		return
	}
	if duration > a.maxDrain {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf(
			"duration %s exceeds the admin_config.max_drain ceiling of %s", duration, a.maxDrain))
		return
	}

	rpcType := domain.RPCType(body.RPCType)
	if rpcType != "" && !validRPCType(rpcType) {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("rpc_type %q is not recognised", body.RPCType))
		return
	}

	if a.drains == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "no drain store is configured")
		return
	}

	ctx := req.Context()
	matched := a.matchedEndpoints(ctx, serviceID, targetDomain, rpcType)

	resp := drainResponse{
		ServiceID:        string(serviceID),
		Domain:           targetDomain,
		RPCType:          body.RPCType,
		DryRun:           body.DryRun,
		MatchedEndpoints: matched,
	}

	key := drain.Key{ServiceID: serviceID, Operator: targetDomain, RPCType: rpcType}

	if duration == 0 {
		resp.Released = true
		if !body.DryRun {
			if err := a.drains.Release(ctx, key); err != nil {
				// A propagation failure is reported, not raised: the release
				// applied here, and answering 500 would tell an operator the
				// drain is still in force on the instance that just lifted it.
				// The same reasoning as setting a drain, in the other
				// direction.
				if !isPropagationError(err) {
					a.logger.Error("admin: release drain", "service", serviceID, "domain", targetDomain, "error", err)
					writeJSONError(w, http.StatusInternalServerError, "failed to release drain")
					return
				}
				resp.PropagationError = err.Error()
			}
		}
		resp.ActiveDrains = activeDrainEntries(a.drains.Active(ctx, serviceID))
		writeJSON(w, http.StatusOK, resp)
		return
	}

	if matched > 0 && a.lastOperatorStanding(ctx, serviceID, targetDomain, rpcType) {
		writeJSONError(w, http.StatusConflict, fmt.Sprintf(
			"domain %q is the only operator serving %s; draining it would leave selection with nothing", targetDomain, describeRPCScope(rpcType)))
		return
	}

	until := time.Now().Add(duration)
	resp.Applied = true
	resp.DrainedUntil = &until

	if !body.DryRun {
		entry := drain.Entry{Key: key, Until: until, Reason: body.Reason}
		if err := a.drains.Set(ctx, entry); err != nil {
			if !isPropagationError(err) {
				a.logger.Error("admin: set drain", "service", serviceID, "domain", targetDomain, "error", err)
				writeJSONError(w, http.StatusInternalServerError, "failed to set drain")
				return
			}
			resp.PropagationError = err.Error()
		}
		a.logger.Warn("admin: operator drain set",
			"service_id", string(serviceID),
			"domain", targetDomain,
			"rpc_type", body.RPCType,
			"until", until,
			"reason", body.Reason,
		)
	}

	resp.ActiveDrains = activeDrainEntries(a.drains.Active(ctx, serviceID))
	writeJSON(w, http.StatusOK, resp)
}

// handleGetDrains lists the live drains for a service. An empty result is an
// empty array, never null.
func (a *AdminAPI) handleGetDrains(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}
	if a.drains == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "no drain store is configured")
		return
	}
	entries := activeDrainEntries(a.drains.Active(req.Context(), serviceID))
	writeJSON(w, http.StatusOK, entries)
}

// handleReleaseDrain releases every RPC-type-scoped drain on one operator for
// a service. PATH's path is kept for operator muscle memory.
//
// Every key that failed only to propagate is still counted as released and its
// error accumulated into one propagation_error string, so the response
// describes the whole operator rather than stopping at the first key Redis
// could not be told about.
func (a *AdminAPI) handleReleaseDrain(w http.ResponseWriter, req *http.Request) {
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	targetDomain := strings.ToLower(strings.TrimSpace(req.PathValue("domain")))
	if serviceID == "" || targetDomain == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID and domain are required")
		return
	}
	if a.drains == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "no drain store is configured")
		return
	}

	ctx := req.Context()
	released := 0
	var propagation []string
	for _, e := range a.drains.Active(ctx, serviceID) {
		if e.Operator != targetDomain {
			continue
		}
		if err := a.drains.Release(ctx, e.Key); err != nil {
			a.logger.Error("admin: release drain", "service", serviceID, "domain", targetDomain, "rpc_type", e.RPCType, "error", err)
			if !isPropagationError(err) {
				writeJSONError(w, http.StatusInternalServerError, "failed to release drain")
				return
			}
			// The local release applied; only the fleet does not know. Keep
			// going — abandoning the loop here would leave the operator half
			// released for a reason that is not their problem to fix, and the
			// remaining keys are what the next DELETE would have to retry.
			propagation = append(propagation, err.Error())
		}
		released++
	}

	resp := map[string]any{
		"service_id":    string(serviceID),
		"domain":        targetDomain,
		"released":      released,
		"active_drains": activeDrainEntries(a.drains.Active(ctx, serviceID)),
	}
	if len(propagation) > 0 {
		resp["propagation_error"] = strings.Join(propagation, "; ")
	}
	writeJSON(w, http.StatusOK, resp)
}

// matchedEndpoints counts the live endpoints belonging to targetDomain, over
// rpcType when scoped or over every RPC type when not. Endpoints seen under
// more than one RPC type are counted once.
func (a *AdminAPI) matchedEndpoints(ctx context.Context, serviceID domain.ServiceID, targetDomain string, rpcType domain.RPCType) int {
	seen := map[domain.EndpointAddr]bool{}
	matched := 0
	for addr := range a.liveEndpoints(ctx, serviceID, rpcType) {
		if seen[addr] {
			continue
		}
		seen[addr] = true
		// Lowercased to compare like for like: targetDomain already is, and
		// shannon's own drain check lowercases the operator it derives from
		// the URL. A host with an uppercase letter would otherwise be drained
		// by the chokepoint while being counted zero here — and the
		// last-operator guard below would wave through a drain that empties
		// the pool.
		if strings.ToLower(addr.Operator()) == targetDomain {
			matched++
		}
	}
	return matched
}

// lastOperatorStanding reports whether draining targetDomain would leave some
// RPC type's selection pool with no endpoint outside it.
//
// Selection pools are per RPC type, so a scoped drain checks only that type.
// An unscoped drain checks every type independently rather than the union
// across them: an operator that is the only websocket provider but one of
// several json_rpc providers must still be refused, even though the combined
// set of operators across every type looks diverse. A type with no live
// endpoints at all is skipped — that is not a collapse the drain would cause.
func (a *AdminAPI) lastOperatorStanding(ctx context.Context, serviceID domain.ServiceID, targetDomain string, rpcType domain.RPCType) bool {
	if a.endpoints == nil {
		return false
	}

	for _, rt := range rpcTypesToCheck(rpcType) {
		endpoints, err := a.endpoints.AvailableEndpoints(ctx, serviceID, rt)
		if err != nil || len(endpoints) == 0 {
			continue
		}
		allTarget := true
		for _, addr := range endpoints {
			// Lowercased for the same reason as in matchedEndpoints.
			if strings.ToLower(addr.Operator()) != targetDomain {
				allTarget = false
				break
			}
		}
		if allTarget {
			return true
		}
	}
	return false
}

// liveEndpoints yields the distinct endpoints available for serviceID over
// rpcType, or over every RPC type when rpcType is unscoped ("").
func (a *AdminAPI) liveEndpoints(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType) map[domain.EndpointAddr]struct{} {
	out := map[domain.EndpointAddr]struct{}{}
	if a.endpoints == nil {
		return out
	}

	for _, rt := range rpcTypesToCheck(rpcType) {
		endpoints, err := a.endpoints.AvailableEndpoints(ctx, serviceID, rt)
		if err != nil {
			continue
		}
		for _, addr := range endpoints {
			out[addr] = struct{}{}
		}
	}
	return out
}

// rpcTypesToCheck returns [rpcType], or every RPC type when rpcType is
// unscoped ("").
func rpcTypesToCheck(rpcType domain.RPCType) []domain.RPCType {
	if rpcType == "" {
		return domain.AllRPCTypes()
	}
	return []domain.RPCType{rpcType}
}

// validRPCType reports whether rpcType is one domain.AllRPCTypes() names.
func validRPCType(rpcType domain.RPCType) bool {
	for _, rt := range domain.AllRPCTypes() {
		if rt == rpcType {
			return true
		}
	}
	return false
}

// describeRPCScope renders the RPC scope of a drain request for an error
// message: the named type, or "every RPC type" when unscoped.
func describeRPCScope(rpcType domain.RPCType) string {
	if rpcType == "" {
		return "every RPC type"
	}
	return string(rpcType)
}

// activeDrainEntries normalises a nil slice to an empty one so JSON encodes
// [] rather than null.
func activeDrainEntries(entries []drain.Entry) []drain.Entry {
	if entries == nil {
		return []drain.Entry{}
	}
	return entries
}

// isPropagationError reports whether err wraps drain.ErrPropagation.
func isPropagationError(err error) bool {
	return errors.Is(err, drain.ErrPropagation)
}
