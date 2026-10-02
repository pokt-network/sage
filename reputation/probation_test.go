package reputation

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// OnProbation reads the same band the selector routes by: a key driven into
// probation is on it, a fresh key and a key taken out of selection are not.
func TestService_OnProbation(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, DefaultServiceConfig())
	var checker ProbationChecker = svc
	ctx := context.Background()
	const demoted, fresh = domain.EndpointAddr("s1-https://slow.example"), domain.EndpointAddr("s2-https://ok.example")
	for i := 0; i < 8; i++ { // 100 - 8×10 = 20: inside [10, 30)
		_ = svc.RecordSignal(ctx, "eth", demoted, domain.RPCTypeJSONRPC, NewSignal(SignalMajorError, "timeout", 0))
	}
	_ = svc.RecordSignal(ctx, "eth", fresh, domain.RPCTypeJSONRPC, NewSignal(SignalSuccess, "ok", 0))

	if !checker.OnProbation(ctx, "eth", demoted, domain.RPCTypeJSONRPC) {
		t.Error("a key at 20 is on probation")
	}
	if checker.OnProbation(ctx, "eth", fresh, domain.RPCTypeJSONRPC) {
		t.Error("a key at 100 is not on probation")
	}
	if checker.OnProbation(ctx, "eth", demoted, domain.RPCTypeWebSocket) {
		t.Error("probation is per RPC type: the websocket key was never graded")
	}
}
