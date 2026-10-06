package router

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/pokt-network/sage/reputation"
)

// policyPenaltyRequest is the body of PUT /admin/reputation/policy/{party}.
type policyPenaltyRequest struct {
	Penalty float64   `json:"penalty"`
	Reason  string    `json:"reason"`
	Until   time.Time `json:"until,omitzero"`
}

// policyAdmin is the reputation service's policy half, or nil with a 501
// written when it has none.
func (a *AdminAPI) policyAdmin(w http.ResponseWriter) reputation.PolicyPenaltyAdmin {
	p, ok := a.repService.(reputation.PolicyPenaltyAdmin)
	if !ok {
		writeJSONError(w, http.StatusNotImplemented, "this reputation service has no policy penalties")
		return nil
	}
	return p
}

// handleListPolicyPenalties lists the policy penalties in force: per party,
// the penalty, the reason, when it was set and, if it has one, when it lapses.
func (a *AdminAPI) handleListPolicyPenalties(w http.ResponseWriter, req *http.Request) {
	p := a.policyAdmin(w)
	if p == nil {
		return
	}
	list, err := p.ListPolicyPenalties(req.Context())
	if err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if list == nil {
		list = []reputation.PolicyPenalty{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"policy_penalties": list})
}

// handleSetPolicyPenalty sets a policy penalty on a party, by hand, for
// conduct SAGE cannot measure: a party reselling a public RPC answers fresh
// and correct, and the measured party penalties (stale share, trust) never
// charge it. party is an owner address ("pokt1…"), which reaches every domain
// dedicated to that owner, or an operator domain. Body: {"penalty": -30,
// "reason": "...", "until": "2026-11-01T00:00:00Z"}; penalty between -100 and
// 0 exclusive, reason required, until optional (none: until deleted). It
// replaces the party's previous one, is shared through Redis with every
// replica and survives restarts, and is charged from the next reputation
// refresh, within 30 seconds, on services with policy_penalty on: as the
// largest of the party's penalties, never below the selection floor.
func (a *AdminAPI) handleSetPolicyPenalty(w http.ResponseWriter, req *http.Request) {
	p := a.policyAdmin(w)
	if p == nil {
		return
	}
	party := strings.ToLower(strings.TrimSpace(req.PathValue("party")))
	if party == "" {
		writeJSONError(w, http.StatusBadRequest, "party is required")
		return
	}
	var body policyPenaltyRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	body.Reason = strings.TrimSpace(body.Reason)
	switch {
	case body.Penalty >= 0 || body.Penalty < -100:
		writeJSONError(w, http.StatusBadRequest, "penalty must be between -100 and 0, exclusive of 0")
		return
	case body.Reason == "":
		writeJSONError(w, http.StatusBadRequest, "reason is required")
		return
	case !body.Until.IsZero() && !body.Until.After(time.Now()):
		writeJSONError(w, http.StatusBadRequest, "until is in the past")
		return
	}
	pen := reputation.PolicyPenalty{Party: party, Penalty: body.Penalty, Reason: body.Reason, SetAt: time.Now().UTC(), Until: body.Until}
	if err := p.SetPolicyPenalty(req.Context(), pen); err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, reputation.ErrNoPolicyStore) {
			status = http.StatusNotImplemented
		}
		writeJSONError(w, status, err.Error())
		return
	}
	a.logger.Warn("policy penalty set", "party", party, "penalty", pen.Penalty, "reason", pen.Reason, "until", pen.Until)
	writeJSON(w, http.StatusOK, pen)
}

// handleDeletePolicyPenalty removes a party's policy penalty; the next
// reputation refresh, within 30 seconds, stops charging it. 404 when the
// party has none.
func (a *AdminAPI) handleDeletePolicyPenalty(w http.ResponseWriter, req *http.Request) {
	p := a.policyAdmin(w)
	if p == nil {
		return
	}
	party := strings.ToLower(strings.TrimSpace(req.PathValue("party")))
	found, err := p.DeletePolicyPenalty(req.Context(), party)
	if err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if !found {
		writeJSONError(w, http.StatusNotFound, "no policy penalty on "+party)
		return
	}
	a.logger.Warn("policy penalty deleted", "party", party)
	writeJSON(w, http.StatusOK, map[string]string{"deleted": party})
}
