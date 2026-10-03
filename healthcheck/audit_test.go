package healthcheck

import (
	"context"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/reputation"
)

// Regression for the 2026-10-03 correctness audit. It asserts the correct
// behaviour; on the code the audit read it fails.

// Two suppliers behind one JSON-RPC URL are one backend group, but they stake
// REST on different hosts. A REST probe dials ONE of those hosts; its result
// must not be recorded against the other, which was never asked.
//
// Driven through the executor rather than reputation.RecordSignalOnce: that
// call's contract is one record per distinct key among the endpoints it is
// given, and it cannot know which was dialed. The grouping that hands it both
// is the executor's (groupByBackend keys on the address URL, whatever the
// check's RPC type).
func TestAudit_ProbeFanOutChargesUnprobedFace(t *testing.T) {
	s1 := domain.EndpointAddr("pokt1one-https://rm.example.com")
	s2 := domain.EndpointAddr("pokt1two-https://rm.example.com")
	restURL := map[domain.EndpointAddr]string{s1: "https://rest-a.example.com", s2: "https://rest-b.example.com"}

	rep := reputation.NewService(reputation.NewMemoryStorage(), nil, reputation.ServiceConfig{})
	rep.SetURLResolver(func(ep domain.EndpointAddr, rt domain.RPCType) (string, bool) {
		u, ok := restURL[ep]
		return u, ok && rt == domain.RPCTypeREST
	})

	e, relayer := newRPCTypeExecutor(t, &perTypeEndpoints{byType: map[domain.RPCType]domain.EndpointAddrList{
		domain.RPCTypeREST: {s1, s2},
	}}, checkOfType("rest_head", domain.RPCTypeREST))
	e.repService = rep
	// A failure, so the probed face's key moves off its initial score and an
	// unprobed key that was charged cannot hide as a fresh 100.
	relayer.response = &domain.Response{HTTPStatusCode: 500, Body: []byte(`{"code":13,"message":"internal"}`)}

	e.runOnce(context.Background())
	e.wg.Wait()

	relayer.mu.Lock()
	calls := append([]relayCall(nil), relayer.calls...)
	relayer.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("relays = %v, want one probe for the backend group", calls)
	}
	probed := calls[0].endpoint
	unprobed := s1
	if probed == s1 {
		unprobed = s2
	}

	scores, _ := rep.GetScores(context.Background(), "svc")
	if s, ok := scores[restURL[probed]+"|rest"]; !ok || s >= 100 {
		t.Fatalf("precondition: the probed REST face %s was not charged: scores %v", restURL[probed], scores)
	}
	for key, score := range scores {
		if strings.HasPrefix(key, restURL[unprobed]) {
			t.Fatalf("one probe through %s (%s) charged %s, never dialed: %s = %v",
				probed, restURL[probed], restURL[unprobed], key, score)
		}
	}
}
