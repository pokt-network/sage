package reputation

import "time"

// SignalType classifies the outcome of a relay attempt.
type SignalType string

const (
	// SignalSuccess indicates a successful relay.
	SignalSuccess SignalType = "success"
	// SignalMinorError indicates a minor, recoverable error.
	SignalMinorError SignalType = "minor_error"
	// SignalMajorError indicates a significant error affecting reliability.
	SignalMajorError SignalType = "major_error"
	// SignalCriticalError indicates a severe error that may warrant cooldown.
	SignalCriticalError SignalType = "critical_error"
	// SignalFatalError indicates a fatal error warranting immediate blacklisting.
	SignalFatalError SignalType = "fatal_error"
)

// Signal represents an observation about an endpoint's behavior.
type Signal struct {
	Type      SignalType
	Timestamp time.Time
	Latency   time.Duration
	Reason    string
	// Probe is true for a health-check probe. Probes score exactly like
	// traffic; the flag exists so the admin listing can say which keys nothing
	// real has graded, and so a future mechanism can be checked for being
	// driven by synthetic traffic alone (docs/scoring.md §3.5).
	Probe bool
	// Leftover is true for a retry or hedge attempt. Which relays reach those
	// attempts was decided by an earlier failure or slowness, so they are a
	// skewed sample of an operator's traffic: the per-key score counts them,
	// the operator counters do not (see RecordSignal).
	Leftover bool
	// Detail is the verdict's own explanation (the block a refusal named,
	// the node's message), kept on the timeline event and nowhere else.
	Detail string
	// Weight is how many successful attempts a success stands for in the
	// failure rate: a WebSocket connection records one success per 30 s for
	// every request it answered since. Zero or one is one; it moves only the
	// rate, never the score.
	Weight int
}

// NewSignal creates a signal of type t, timestamped now.
func NewSignal(t SignalType, reason string, latency time.Duration) Signal {
	return Signal{
		Type:      t,
		Timestamp: time.Now(),
		Latency:   latency,
		Reason:    reason,
	}
}

// defaultImpacts maps signal types to their default score impacts.
var defaultImpacts = map[SignalType]int{
	SignalSuccess:       +5,
	SignalMinorError:    -3,
	SignalMajorError:    -10,
	SignalCriticalError: -25,
	SignalFatalError:    -50,
}

// SignalImpacts is the score delta per surviving signal type. Zero means the
// default for that type (defaultImpacts), so an operator sets only the ones
// they mean to move.
type SignalImpacts struct {
	Success, MinorError, MajorError, CriticalError, FatalError int
}

// Normalized fills zero fields from defaultImpacts.
func (i SignalImpacts) Normalized() SignalImpacts {
	def := func(v int, t SignalType) int {
		if v == 0 {
			return defaultImpacts[t]
		}
		return v
	}
	return SignalImpacts{
		Success:       def(i.Success, SignalSuccess),
		MinorError:    def(i.MinorError, SignalMinorError),
		MajorError:    def(i.MajorError, SignalMajorError),
		CriticalError: def(i.CriticalError, SignalCriticalError),
		FatalError:    def(i.FatalError, SignalFatalError),
	}
}

// Impact returns the delta for a signal type under these impacts. Unknown
// types (none should exist) are 0.
func (i SignalImpacts) Impact(t SignalType) float64 {
	switch t {
	case SignalSuccess:
		return float64(i.Success)
	case SignalMinorError:
		return float64(i.MinorError)
	case SignalMajorError:
		return float64(i.MajorError)
	case SignalCriticalError:
		return float64(i.CriticalError)
	case SignalFatalError:
		return float64(i.FatalError)
	}
	return 0
}
