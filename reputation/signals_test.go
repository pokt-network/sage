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
		{SignalRecoverySuccess, 5},
		{SignalSlowResponse, -1},
		{SignalVerySlowResponse, -3},
		{SignalStaleBlock, -15},
		{SignalType("unknown"), 0},
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
		{"RecoverySuccess", NewRecoverySuccessSignal, SignalRecoverySuccess},
		{"SlowResponse", NewSlowResponseSignal, SignalSlowResponse},
		{"VerySlowResponse", NewVerySlowResponseSignal, SignalVerySlowResponse},
		{"StaleBlock", NewStaleBlockSignal, SignalStaleBlock},
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
