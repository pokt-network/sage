package router

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/tuning"
)

// --- Runtime tuning handlers ---

// handleListTuning returns every knob that can be overridden at runtime, with
// whatever has been set on it.
//
// Every registered knob appears whether or not anyone has touched it: the point
// is to show what can be changed, not only what has been. Each knob carries its
// kind, its accepted range and a description, so a client (the admin UI, or a
// person with curl) can render a control and reject a bad value before sending
// it.
//
// Overrides live in memory and are lost on restart, which the response states
// rather than leaving to be discovered.
func (a *AdminAPI) handleListTuning(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"knobs": a.tuning.All(),
		"note":  "overrides are held in memory and are lost on restart; the config file is authoritative again after one",
	})
}

// handleGetTuning returns what is in force for one knob: the config file's
// value, the global override if there is one, and which of the two applies.
//
// The list endpoint shows what has been SET, which is not the same question. An
// operator who has just changed a knob wants to know what it is now, and
// before this the only honest answer available to them was to read the config
// file and the override list and combine the two themselves — which on the
// mainnet canary on 2026-09-03 meant nobody could tell whether a configured
// 500 workers had been clamped, rejected or honoured.
func (a *AdminAPI) handleGetTuning(w http.ResponseWriter, req *http.Request) {
	name := req.PathValue("knob")
	effective, ok := a.tuning.EffectiveFor(name)
	if !ok {
		writeJSONError(w, http.StatusNotFound,
			"unknown knob "+name+"; registered knobs are: "+strings.Join(tuning.KnobNames(), ", "))
		return
	}
	writeJSON(w, http.StatusOK, effective)
}

// handleSetTuning sets a knob globally. Body: `{"value": "3"}` — the value is a
// string in the knob's own notation ("3", "250ms", "0.5") rather than a JSON
// number, so a duration round-trips as what the operator typed.
//
// A per-service override still wins over the global value for that service.
func (a *AdminAPI) handleSetTuning(w http.ResponseWriter, req *http.Request) {
	a.setTuning(w, req, "")
}

// handleSetTuningForService sets a knob for one service only. Body:
// `{"value": "3"}`. This is the narrower switch and takes precedence over the
// global value, which is how a change is tried on one chain first.
func (a *AdminAPI) handleSetTuningForService(w http.ResponseWriter, req *http.Request) {
	a.setTuning(w, req, domain.ServiceID(req.PathValue("serviceID")))
}

// setTuning is the shared body of both setters.
//
// A rejected value answers 400 with the reason from the tuning package —
// unknown knob (listing the ones that exist), unparseable, or out of range.
// Out-of-range is refused rather than clamped: an operator who typed 900s for a
// relay timeout has made a mistake, and quietly storing 300s would leave them
// believing the 900.
func (a *AdminAPI) setTuning(w http.ResponseWriter, req *http.Request, serviceID domain.ServiceID) {
	knob := req.PathValue("knob")
	if knob == "" {
		writeJSONError(w, http.StatusBadRequest, "knob name is required")
		return
	}

	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := a.tuning.Set(knob, serviceID, body.Value); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	a.logger.Warn("admin: runtime tuning override set",
		"knob", knob,
		"service_id", string(serviceID),
		"value", body.Value,
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"knob":       knob,
		"service_id": string(serviceID),
		"value":      body.Value,
	})
}

// handleClearTuning removes the global override for a knob, returning the
// config file's value to effect. Per-service overrides are left alone: they are
// the narrower statement, and clearing them here would revert services the
// operator did not name.
func (a *AdminAPI) handleClearTuning(w http.ResponseWriter, req *http.Request) {
	a.clearTuning(w, req, "")
}

// handleClearTuningForService removes one service's override, leaving the
// global one (or the config value) in effect for it.
func (a *AdminAPI) handleClearTuningForService(w http.ResponseWriter, req *http.Request) {
	a.clearTuning(w, req, domain.ServiceID(req.PathValue("serviceID")))
}

// clearTuning is the shared body of both clears. Clearing something that was
// never set is not an error — the caller asked for a state and got it.
func (a *AdminAPI) clearTuning(w http.ResponseWriter, req *http.Request, serviceID domain.ServiceID) {
	knob := req.PathValue("knob")
	if knob == "" {
		writeJSONError(w, http.StatusBadRequest, "knob name is required")
		return
	}
	if _, ok := tuning.Lookup(knob); !ok {
		writeJSONError(w, http.StatusBadRequest, "unknown knob "+knob)
		return
	}

	cleared := a.tuning.Delete(knob, serviceID)
	if cleared {
		a.logger.Warn("admin: runtime tuning override cleared",
			"knob", knob,
			"service_id", string(serviceID),
		)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"knob":       knob,
		"service_id": string(serviceID),
		"cleared":    cleared,
	})
}
