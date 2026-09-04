package reputation

import (
	"context"
	"time"

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
	// UpdatedAt is the Unix time of the write that produced this state. Set by
	// the write-behind on the way to storage, read by the storage sweep: a
	// field older than the idle TTL is deleted. Not consulted by scoring.
	UpdatedAt int64 `json:"updated_at,omitempty"`
}

// DefaultIdleTTL is how long a reputation key that has stopped receiving
// signals is remembered by the parts of the system that would otherwise grow
// with every key ever seen — the timeline and the storage write-behind. A
// mainnet session is ~20 minutes, so this is three sessions: a key idle that
// long has left the session, and at per-supplier or per-endpoint granularity
// it is a registration the network rotated out rather than a backend that
// went quiet. The in-memory score cache is bounded separately
// (pruneUninformative) and does NOT expire keys: a penalty is kept for as long
// as the process lives.
const DefaultIdleTTL = time.Hour

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
