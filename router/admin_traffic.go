package router

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/traffic"
)

// defaultRequestSampleTop is the number of top fingerprints
// GET /admin/request-sample/{serviceID} returns when the caller does not pass
// ?top. requestSampleTopCap is the most it will ever return, regardless of
// what the caller asks for — an operator-facing report, not an unbounded
// export.
const (
	defaultRequestSampleTop = 20
	requestSampleTopCap     = 100
)

// requestSampleEntry is one service's entry in the
// GET /admin/request-sample list response.
type requestSampleEntry struct {
	// Window says which window Summary was read from: "previous" (the usual
	// case) or "current", when the service has not completed a window yet
	// and there is nothing else to show.
	Window  string          `json:"window"`
	Summary traffic.Summary `json:"summary"`
}

// handleListRequestSamples returns every service the request-shape sampler
// has observed, each with its most recently completed traffic summary.
//
// Each entry prefers the previous (complete) window; a service still filling
// its first window has no previous one yet, so its entry falls back to the
// current window instead and says so via "window": "current" — the
// alternative, omitting the service, would make a gateway that just started
// look like it has no traffic at all. Answers 503 if no sampler is
// configured.
func (a *AdminAPI) handleListRequestSamples(w http.ResponseWriter, req *http.Request) {
	if a.sampler == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "no request sampler is configured")
		return
	}

	out := make(map[string]requestSampleEntry)
	for _, id := range a.sampler.Services() {
		window := "previous"
		summary, ok := a.sampler.Summary(id, true)
		if !ok {
			window = "current"
			summary, ok = a.sampler.Summary(id, false)
			if !ok {
				// The sampler just told us via Services() that id is known,
				// so its current window must exist; this is unreachable
				// short of a race with the window rolling mid-request.
				continue
			}
		}
		out[string(id)] = requestSampleEntry{Window: window, Summary: summary}
	}
	writeJSON(w, http.StatusOK, out)
}

// handleGetRequestSample returns one service's request-shape summary plus its
// top fingerprints for a single window.
//
// Query params: `window` (`current` or `previous`, default `previous`) and
// `top` (default 20, capped at 100 regardless of what is asked for; must be a
// positive integer — 0 or negative is rejected rather than passed through as
// "unlimited"). Answers 503 if no sampler is configured, 400 for an
// unrecognised window or a malformed top, and 404 if the sampler has never
// observed the service. A
// known service whose requested window has not completed yet (typically
// `window=previous` on a service still filling its first window) is not a
// 404: it answers with a zero-valued summary and an empty top list, since the
// service is real and simply has nothing there yet.
func (a *AdminAPI) handleGetRequestSample(w http.ResponseWriter, req *http.Request) {
	if a.sampler == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "no request sampler is configured")
		return
	}

	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	if serviceID == "" {
		writeJSONError(w, http.StatusBadRequest, "serviceID is required")
		return
	}

	previous, err := parseRequestSampleWindow(req.URL.Query().Get("window"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	topN, err := parseRequestSampleTop(req.URL.Query().Get("top"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	summary, ok := a.sampler.Summary(serviceID, previous)
	if !ok {
		// window=current failing means the service has never been observed
		// at all — the current window exists as soon as a service is, so
		// there is nothing else to check.
		if !previous {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("service %q has no sampled traffic", serviceID))
			return
		}
		// window=previous failing is ambiguous on its own: either the
		// service has never been observed, or it has but its first window
		// has not rolled yet. The current window always exists once the
		// service has been observed at all, so it tells the two cases
		// apart without a second call to Summary(serviceID, previous).
		if _, known := a.sampler.Summary(serviceID, false); !known {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("service %q has no sampled traffic", serviceID))
			return
		}
		summary = traffic.Summary{ServiceID: string(serviceID), PerMethod: map[string]traffic.MethodStats{}}
	}

	top := a.sampler.Top(serviceID, previous, topN)
	if top == nil {
		top = []traffic.Fingerprint{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": summary,
		"top":     top,
	})
}

// parseRequestSampleWindow maps the `window` query param to the previous bool
// Sampler.Summary and Sampler.Top expect. Empty defaults to "previous".
func parseRequestSampleWindow(raw string) (previous bool, err error) {
	switch raw {
	case "", "previous":
		return true, nil
	case "current":
		return false, nil
	default:
		return false, fmt.Errorf("window %q must be \"current\" or \"previous\"", raw)
	}
}

// parseRequestSampleTop parses the `top` query param, defaulting to
// defaultRequestSampleTop and capping at requestSampleTopCap. top must be a
// positive integer: 0 or negative is rejected as malformed, the same as a
// non-integer, rather than passed through to Sampler.Top — its n<=0 means
// "unlimited", which would let ?top=0 return up to maxFingerprints entries
// and silently contradict this route's documented 100-entry ceiling.
func parseRequestSampleTop(raw string) (int, error) {
	if raw == "" {
		return defaultRequestSampleTop, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("invalid top %q: must be a positive integer", raw)
	}
	if n > requestSampleTopCap {
		n = requestSampleTopCap
	}
	return n, nil
}
