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

// resolvingEndpoints answers which REST host each endpoint dials, as the
// protocol does in production (protocol.URLResolver).
type resolvingEndpoints struct {
	*perTypeEndpoints
	rest map[domain.EndpointAddr]string
}

func (r resolvingEndpoints) EndpointURLFor(ep domain.EndpointAddr, rt domain.RPCType) (string, bool) {
	u, ok := r.rest[ep]
	return u, ok && rt == domain.RPCTypeREST
}

// Two suppliers behind one JSON-RPC URL, staking REST on different hosts,
// are two backends for a REST check. Grouped by the address URL they were
// one, and one probe through either host charged both: the other host was
// never asked. Each REST host must be probed by its own probe, and a third
// supplier on an already-probed REST host still shares its group.
func TestAudit_ProbeFanOutChargesUnprobedFace(t *testing.T) {
	s1 := domain.EndpointAddr("pokt1one-https://rm.example.com")
	s2 := domain.EndpointAddr("pokt1two-https://rm.example.com")
	s3 := domain.EndpointAddr("pokt1three-https://other.example.com")
	restURL := map[domain.EndpointAddr]string{s1: "https://rest-a.example.com", s2: "https://rest-b.example.com", s3: "https://rest-a.example.com"}

	rep := reputation.NewService(reputation.NewMemoryStorage(), nil, reputation.ServiceConfig{})
	rep.SetURLResolver(func(ep domain.EndpointAddr, rt domain.RPCType) (string, bool) {
		u, ok := restURL[ep]
		return u, ok && rt == domain.RPCTypeREST
	})

	eps := &perTypeEndpoints{byType: map[domain.RPCType]domain.EndpointAddrList{
		domain.RPCTypeREST: {s1, s2, s3},
	}}
	e, relayer := newRPCTypeExecutor(t, eps, checkOfType("rest_head", domain.RPCTypeREST))
	e.endpoints = resolvingEndpoints{perTypeEndpoints: eps, rest: restURL}
	e.repService = rep
	// A failure, so a charged key moves off its initial score and cannot
	// hide as a fresh 100.
	relayer.response = &domain.Response{HTTPStatusCode: 500, Body: []byte(`{"code":13,"message":"internal"}`)}

	e.runOnce(context.Background())
	e.wg.Wait()

	relayer.mu.Lock()
	calls := append([]relayCall(nil), relayer.calls...)
	relayer.mu.Unlock()
	dialed := map[string]int{}
	for _, c := range calls {
		dialed[restURL[c.endpoint]]++
	}
	if len(calls) != 2 || dialed["https://rest-a.example.com"] != 1 || dialed["https://rest-b.example.com"] != 1 {
		t.Fatalf("probes = %v; want one per REST host", calls)
	}

	scores, _ := rep.GetScores(context.Background(), "svc")
	for key, score := range scores {
		host, _, _ := strings.Cut(key, "|")
		if dialed[host] == 0 {
			t.Fatalf("%s was charged (%v) but never dialed: scores %v", host, score, scores)
		}
	}
}
