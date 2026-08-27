package reputation

import (
	"testing"
	"time"
)

func TestDefaultImpact(t *testing.T) {
	tests := []struct {
		signal   SignalType
		expected int
	}{
		{SignalSuccess, 5},
		{SignalMinorError, -3},
		{SignalMajorError, -10},
		{SignalCriticalError, -25},
		{SignalFatalError, -50},
		{SignalType("unknown"), 0},
		// The latency and stale-block types were removed: latency never fed the
		// score, and staleness is a QoS filter, not a reputation penalty.
		{SignalType("slow_response"), 0},
		{SignalType("stale_block"), 0},
	}
	for _, tt := range tests {
		t.Run(string(tt.signal), func(t *testing.T) {
			got := DefaultImpact(tt.signal)
			if got != tt.expected {
				t.Errorf("DefaultImpact(%s) = %d, want %d", tt.signal, got, tt.expected)
			}
		})
	}
}

func TestSignalConstructors(t *testing.T) {
	before := time.Now()
	constructors := []struct {
		name string
		fn   func(string, time.Duration) Signal
		typ  SignalType
	}{
		{"Success", NewSuccessSignal, SignalSuccess},
		{"MinorError", NewMinorErrorSignal, SignalMinorError},
		{"MajorError", NewMajorErrorSignal, SignalMajorError},
		{"CriticalError", NewCriticalErrorSignal, SignalCriticalError},
		{"FatalError", NewFatalErrorSignal, SignalFatalError},
	}
	for _, tc := range constructors {
		t.Run(tc.name, func(t *testing.T) {
			sig := tc.fn("test reason", 100*time.Millisecond)
			if sig.Type != tc.typ {
				t.Errorf("type = %s, want %s", sig.Type, tc.typ)
			}
			if sig.Reason != "test reason" {
				t.Errorf("reason = %q, want %q", sig.Reason, "test reason")
			}
			if sig.Latency != 100*time.Millisecond {
				t.Errorf("latency = %v, want 100ms", sig.Latency)
			}
			if sig.Timestamp.Before(before) {
				t.Errorf("timestamp %v is before test start %v", sig.Timestamp, before)
			}
		})
	}
}

// The constructors leave Probe false: a probe is marked by the caller that
// knows it is one (the health-check executor), not by the signal's severity.
func TestSignalConstructors_ProbeDefaultsFalse(t *testing.T) {
	if NewSuccessSignal("ok", 0).Probe {
		t.Error("a constructed signal must not claim to be a probe")
	}
}

func TestSignalImpacts_Normalized(t *testing.T) {
	// Only the fields an operator set move; the rest keep their defaults.
	got := SignalImpacts{Success: 1, CriticalError: -50}.Normalized()
	want := SignalImpacts{Success: 1, MinorError: -3, MajorError: -10, CriticalError: -50, FatalError: -50}
	if got != want {
		t.Errorf("Normalized() = %+v, want %+v", got, want)
	}
	// The zero value is exactly the defaults.
	if def := (SignalImpacts{}).Normalized(); def != (SignalImpacts{Success: 5, MinorError: -3, MajorError: -10, CriticalError: -25, FatalError: -50}) {
		t.Errorf("zero SignalImpacts normalized to %+v", def)
	}
}

func TestSignalImpacts_Impact(t *testing.T) {
	i := SignalImpacts{Success: 5, MinorError: -3, MajorError: -10, CriticalError: -25, FatalError: -50}
	for _, tc := range []struct {
		t    SignalType
		want float64
	}{
		{SignalSuccess, 5},
		{SignalMinorError, -3},
		{SignalMajorError, -10},
		{SignalCriticalError, -25},
		{SignalFatalError, -50},
		{SignalType("stale_block"), 0},
	} {
		if got := i.Impact(tc.t); got != tc.want {
			t.Errorf("Impact(%s) = %v, want %v", tc.t, got, tc.want)
		}
	}
}
