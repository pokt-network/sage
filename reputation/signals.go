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
}

// NewSuccessSignal creates a signal indicating a successful relay.
func NewSuccessSignal(reason string, latency time.Duration) Signal {
	return Signal{
		Type:      SignalSuccess,
		Timestamp: time.Now(),
		Latency:   latency,
		Reason:    reason,
	}
}

// NewMinorErrorSignal creates a signal indicating a minor error.
func NewMinorErrorSignal(reason string, latency time.Duration) Signal {
	return Signal{
		Type:      SignalMinorError,
		Timestamp: time.Now(),
		Latency:   latency,
		Reason:    reason,
	}
}

// NewMajorErrorSignal creates a signal indicating a major error.
func NewMajorErrorSignal(reason string, latency time.Duration) Signal {
	return Signal{
		Type:      SignalMajorError,
		Timestamp: time.Now(),
		Latency:   latency,
		Reason:    reason,
	}
}

// NewCriticalErrorSignal creates a signal indicating a critical error.
func NewCriticalErrorSignal(reason string, latency time.Duration) Signal {
	return Signal{
		Type:      SignalCriticalError,
		Timestamp: time.Now(),
		Latency:   latency,
		Reason:    reason,
	}
}

// NewFatalErrorSignal creates a signal indicating a fatal error.
func NewFatalErrorSignal(reason string, latency time.Duration) Signal {
	return Signal{
		Type:      SignalFatalError,
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

// DefaultImpact returns the default score impact for a signal type.
// Unknown signal types return 0.
func DefaultImpact(signalType SignalType) int {
	return defaultImpacts[signalType]
}

// SignalImpacts is the score delta per surviving signal type. Zero means the
// default for that type (DefaultImpact), so an operator sets only the ones
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
