package heuristic

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

// TestAnalyze_CapabilityLimitation_NoPenalty guards that archival/capability
// limitation errors (the supplier correctly reports it can't serve historical
// or pruned state) are attributed to the blockchain and never penalize or
// circuit-break the supplier.
func TestAnalyze_CapabilityLimitation_NoPenalty(t *testing.T) {
	bodies := []string{
		`{"jsonrpc":"2.0","error":{"code":-32000,"message":"historical state not available"},"id":1}`,
		`{"jsonrpc":"2.0","error":{"code":-32000,"message":"state at block 100 is pruned"},"id":1}`,
		`{"jsonrpc":"2.0","error":{"code":-32000,"message":"requested height is not available, lowest available is 500"},"id":1}`,
	}
	for _, body := range bodies {
		got := Analyze([]byte(body), 200, domain.RPCTypeJSONRPC)
		if got.Attribution != AttrBlockchain {
			t.Errorf("body %q: attribution = %v, want blockchain", body, got.Attribution)
		}
		if got.ShouldPenalize {
			t.Errorf("body %q: capability limitation must not penalize supplier", body)
		}
		if got.ShouldCircuitBreak {
			t.Errorf("body %q: capability limitation must not circuit-break", body)
		}
		if !got.ShouldRetry {
			t.Errorf("body %q: should retry on an archival-capable supplier", body)
		}
	}
}
