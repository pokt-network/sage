package heuristic

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

// HC-1. ErrEndpointsStale is the relayer saying the selected endpoint left
// the session after rollover: the gateway's view was stale, nothing reached
// the supplier. The catch-all transport_error branch penalises it minor.
func TestAudit2_StaleSessionTransportErrorNotPenalized(t *testing.T) {
	err := domain.NewRelayError(domain.ErrTransport, "x", domain.ErrEndpointsStale, true)
	got := AnalyzeTransportError(err, nil)
	if got.ShouldPenalize {
		t.Fatalf("ErrEndpointsStale graded %q severity %q attribution %v with ShouldPenalize=true; want no penalty",
			got.Reason, got.PenaltySeverity, got.Attribution)
	}
}
