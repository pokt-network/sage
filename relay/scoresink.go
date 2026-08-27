package relay

import (
	"sync"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/reputation"
)

// ScoreSink collapses the per-attempt signals of one batch request to one
// signal per (endpoint, RPC type): the worst severity seen, carrying that
// signal's reason and the maximum latency observed for the endpoint.
//
// It is shared across the whole request tree on purpose — batch puts one on
// the parent context before cloning, and Clone() is shallow, so every
// sub-relay (and every retry and hedge arm inside those) adds to the same
// sink. That sharing is the mechanism, not an accident; the mutex is what
// makes it safe.
//
// After Flush the sink forwards each further signal straight to the flush
// function — a hedge arm that outlives its batch is scored on its own rather
// than lost. Hedge deliberately runs its losing arm to completion on a context
// detached with context.WithoutCancel, so an arm inside a batch sub-relay can
// still be holding this pointer well after the batch returned and flushed;
// without the forward, its signal would be collapsed into a map nobody reads.
type ScoreSink struct {
	mu    sync.Mutex
	worst map[sinkKey]reputation.Signal
	// closed is set by the first Flush. From then on the sink is a
	// pass-through rather than an accumulator: there is no second flush
	// coming, so holding a signal is the same as dropping it.
	closed bool
	// late is the most recent Flush's function, used to forward the signals
	// that arrive after it.
	late func(ep domain.EndpointAddr, rpc domain.RPCType, sig reputation.Signal)
}

// sinkKey is what a collapsed signal is keyed by: one endpoint's outcome for
// one RPC type. Reputation is scored per RPC type, so a batch that mixes them
// must not merge across.
type sinkKey struct {
	ep  domain.EndpointAddr
	rpc domain.RPCType
}

// NewScoreSink returns an empty sink.
func NewScoreSink() *ScoreSink {
	return &ScoreSink{worst: make(map[sinkKey]reputation.Signal)}
}

// severityRank orders signal types for worst-of. Higher is worse.
func severityRank(t reputation.SignalType) int {
	switch t {
	case reputation.SignalFatalError:
		return 4
	case reputation.SignalCriticalError:
		return 3
	case reputation.SignalMajorError:
		return 2
	case reputation.SignalMinorError:
		return 1
	case reputation.SignalSuccess:
		return 0
	default:
		// Unknown type: ranked no worse than success, so a type this build
		// does not know cannot outrank a real error and swallow it.
		return 0
	}
}

// Add records one attempt's signal for an endpoint.
//
// Worst severity wins. On equal severity the FIRST signal's reason survives —
// the collapsed signal keeps the reason of the attempt that set the current
// rank, not of the one that matched it. Latency is independent of that: any
// add, including a lower-severity one, raises the stored latency to the
// maximum seen for the key.
//
// After Flush this forwards to the flush function instead of storing; see the
// type comment.
func (s *ScoreSink) Add(ep domain.EndpointAddr, rpc domain.RPCType, sig reputation.Signal) {
	k := sinkKey{ep, rpc}
	s.mu.Lock()
	if s.closed {
		late := s.late
		s.mu.Unlock()
		// Outside the lock: the flush function records to the reputation
		// service, and holding the sink's mutex across that would serialise
		// every late arm behind it.
		if late != nil {
			late(ep, rpc, sig)
		}
		return
	}
	defer s.mu.Unlock()
	cur, ok := s.worst[k]
	if !ok {
		s.worst[k] = sig
		return
	}
	if severityRank(sig.Type) > severityRank(cur.Type) {
		latency := cur.Latency
		if sig.Latency > latency {
			latency = sig.Latency
		}
		sig.Latency = latency
		s.worst[k] = sig
		return
	}
	if sig.Latency > cur.Latency {
		cur.Latency = sig.Latency
		s.worst[k] = cur
	}
}

// Flush calls fn once per (endpoint, RPC type) with the collapsed signal and
// empties the sink. It also closes the sink onto fn: every later Add is
// forwarded to fn directly rather than accumulated, so a signal that arrives
// after the batch returned is still scored.
func (s *ScoreSink) Flush(fn func(ep domain.EndpointAddr, rpc domain.RPCType, sig reputation.Signal)) {
	s.mu.Lock()
	worst := s.worst
	s.worst = make(map[sinkKey]reputation.Signal)
	s.closed = true
	s.late = fn
	s.mu.Unlock()
	for k, sig := range worst {
		fn(k.ep, k.rpc, sig)
	}
}
