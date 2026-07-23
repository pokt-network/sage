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
	// SignalRecoverySuccess indicates a previously failing endpoint has recovered.
	SignalRecoverySuccess SignalType = "recovery_success"
	// SignalSlowResponse indicates a response that exceeded the expected latency.
	SignalSlowResponse SignalType = "slow_response"
	// SignalVerySlowResponse indicates a response far exceeding expected latency.
	SignalVerySlowResponse SignalType = "very_slow_response"
	// SignalStaleBlock indicates the endpoint returned a stale block height.
	SignalStaleBlock SignalType = "stale_block"
)

// Signal represents an observation about an endpoint's behavior.
type Signal struct {
	Type      SignalType
	Timestamp time.Time
	Latency   time.Duration
	Reason    string
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

// NewRecoverySuccessSignal creates a signal indicating recovery from failure.
func NewRecoverySuccessSignal(reason string, latency time.Duration) Signal {
	return Signal{
		Type:      SignalRecoverySuccess,
		Timestamp: time.Now(),
		Latency:   latency,
		Reason:    reason,
	}
}

// NewSlowResponseSignal creates a signal indicating a slow response.
func NewSlowResponseSignal(reason string, latency time.Duration) Signal {
	return Signal{
		Type:      SignalSlowResponse,
		Timestamp: time.Now(),
		Latency:   latency,
		Reason:    reason,
	}
}

// NewVerySlowResponseSignal creates a signal indicating a very slow response.
func NewVerySlowResponseSignal(reason string, latency time.Duration) Signal {
	return Signal{
		Type:      SignalVerySlowResponse,
		Timestamp: time.Now(),
		Latency:   latency,
		Reason:    reason,
	}
}

// NewStaleBlockSignal creates a signal indicating a stale block was returned.
func NewStaleBlockSignal(reason string, latency time.Duration) Signal {
	return Signal{
		Type:      SignalStaleBlock,
		Timestamp: time.Now(),
		Latency:   latency,
		Reason:    reason,
	}
}

// defaultImpacts maps signal types to their default score impacts.
var defaultImpacts = map[SignalType]int{
	SignalSuccess:          +5,
	SignalMinorError:       -3,
	SignalMajorError:       -10,
	SignalCriticalError:    -25,
	SignalFatalError:       -50,
	SignalRecoverySuccess:  +5,
	SignalSlowResponse:     -1,
	SignalVerySlowResponse: -3,
	SignalStaleBlock:       -15,
}

// DefaultImpact returns the default score impact for a signal type.
// Unknown signal types return 0.
func DefaultImpact(signalType SignalType) int {
	return defaultImpacts[signalType]
}
