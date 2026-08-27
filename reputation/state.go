package reputation

import (
	"context"

	"github.com/pokt-network/sage/domain"
)

// State is everything the service remembers about one reputation key.
//
// Score is the additive term (today's score). Rate is the EWMA failure rate
// the chronic term reads. Attempts and TrafficAttempts are counts for the
// admin listing — the second excludes health-check probes so "has anything
// real graded this key?" is answerable. LatencyMS is a traffic-only EWMA kept
// for reporting; it has no effect on any score and is not persisted.
type State struct {
	Score           float64 `json:"score"`
	Rate            float64 `json:"rate,omitempty"`
	Attempts        uint64  `json:"attempts,omitempty"`
	TrafficAttempts uint64  `json:"traffic_attempts,omitempty"`
	LatencyMS       float64 `json:"-"`
}

// StateView is State as the admin API presents it, with the derived values
// every reader actually keys on.
type StateView struct {
	Score           float64 `json:"score"`
	Additive        float64 `json:"additive"`
	Rate            float64 `json:"rate"`
	Penalty         float64 `json:"penalty"`
	Attempts        uint64  `json:"attempts"`
	TrafficAttempts uint64  `json:"traffic_attempts"`
	ProbeOnly       bool    `json:"probe_only"`
	LatencyMS       float64 `json:"latency_ms"`
}

// StateLister is the optional read interface the admin API asks a
// reputation.Service for. Keys are reputation keys at the configured
// granularity, as with GetScores.
type StateLister interface {
	GetStates(ctx context.Context, serviceID domain.ServiceID) (map[string]StateView, error)
}
