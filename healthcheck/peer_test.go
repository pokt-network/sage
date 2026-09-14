package healthcheck

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// idleSource is a peer feed that delivers nothing; tests hand results to
// applyPeerResult directly.
type idleSource struct{}

func (idleSource) Run(ctx context.Context, _ func(ProbeResult)) error {
	<-ctx.Done()
	return ctx.Err()
}

// oneShotSource delivers one result, then idles.
type oneShotSource struct{ r ProbeResult }

func (s oneShotSource) Run(ctx context.Context, apply func(ProbeResult)) error {
	apply(s.r)
	<-ctx.Done()
	return ctx.Err()
}

const (
	localA = domain.EndpointAddr("supplierA-https://node1.example.com")
	localB = domain.EndpointAddr("supplierB-https://node2.example.com")
	// peerOnNode1 is the other instance's registration of node1: another
	// supplier address in front of the same backend URL.
	peerOnNode1 = domain.EndpointAddr("peerSupplier-https://node1.example.com")
)

func peerExecutor(t *testing.T) (*Executor, *stubRelayer, *stubRepService) {
	t.Helper()
	relayer := &stubRelayer{response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)}}
	eps := &stubEndpointProvider{endpoints: domain.EndpointAddrList{localA, localB}}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}}
	rep := &stubRepService{}
	exec := newTestExecutor(relayer, eps, sessions, probeableRegistry(t, "eth"), rep)
	exec.SetPeerSource(idleSource{}, 0)
	return exec, relayer, rep
}

func peerResult(ep domain.EndpointAddr, at time.Time) ProbeResult {
	return ProbeResult{
		ServiceID: "eth", Endpoint: ep, Siblings: domain.EndpointAddrList{ep},
		Check: "block_number", RPCType: domain.RPCTypeJSONRPC,
		StatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`), ProbedAt: at,
	}
}

func relayedTo(r *stubRelayer) []domain.EndpointAddr {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.EndpointAddr, 0, len(r.calls))
	for _, c := range r.calls {
		out = append(out, c.endpoint)
	}
	return out
}

// A fresh result from the other instance covers the backend it probed: it
// lands on THIS instance's registration of that backend (the other's
// address is not ours), and the leader does not probe that backend again.
// The backend the other did not cover is still probed.
func TestPeer_FreshResultCoversItsBackendOnLocalRegistrations(t *testing.T) {
	exec, relayer, rep := peerExecutor(t)
	exec.applyPeerResult(context.Background(), peerResult(peerOnNode1, exec.now()))

	rep.mu.Lock()
	calls := append([]domain.EndpointAddrList(nil), rep.onceCalls...)
	rep.mu.Unlock()
	if len(calls) != 1 || !slices.Equal(calls[0], domain.EndpointAddrList{localA}) {
		t.Fatalf("signal recorded against %v, want this instance's own registration of node1 only", calls)
	}

	exec.runOnce(context.Background())
	exec.wg.Wait()
	if got := relayedTo(relayer); !slices.Equal(got, []domain.EndpointAddr{localB}) {
		t.Fatalf("probed %v, want only node2: node1 was just covered by the peer", got)
	}
}

// What the peer result cannot vouch for is probed here: a stale result, a
// backend this instance does not reach, a transport failure on a
// registration this instance does not hold, a service it does not serve.
// None is applied, and none counts as covered.
func TestPeer_StaleOrForeignResultsDoNotCover(t *testing.T) {
	cases := map[string]func(*Executor) ProbeResult{
		"stale": func(e *Executor) ProbeResult {
			return peerResult(peerOnNode1, e.now().Add(-2*defaultInterval))
		},
		"backend we do not reach": func(e *Executor) ProbeResult {
			return peerResult("x-https://elsewhere.example.com", e.now())
		},
		"transport failure on the peer's own registration": func(e *Executor) ProbeResult {
			r := peerResult(peerOnNode1, e.now())
			r.TransportError, r.TransportReason, r.TransportSeverity = "dial", "connection_refused", "major"
			return r
		},
		"service we do not serve": func(e *Executor) ProbeResult {
			r := peerResult(peerOnNode1, e.now())
			r.ServiceID = "poly"
			return r
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			exec, relayer, rep := peerExecutor(t)
			exec.applyPeerResult(context.Background(), build(exec))
			if name != "stale" {
				rep.mu.Lock()
				n := len(rep.signals)
				rep.mu.Unlock()
				if n != 0 {
					t.Fatalf("%d signals applied, want none", n)
				}
			}
			exec.runOnce(context.Background())
			exec.wg.Wait()
			if got := relayedTo(relayer); len(got) != 2 {
				t.Fatalf("probed %v, want both backends", got)
			}
		})
	}
}

// Start runs the peer feed on every replica, and what it delivers is recorded
// as coverage the leader's schedule reads.
func TestPeer_StartRunsTheFeed(t *testing.T) {
	exec, _, _ := peerExecutor(t)
	exec.SetPeerSource(oneShotSource{r: peerResult(peerOnNode1, time.Now())}, 0)
	exec.Start(context.Background())
	defer exec.Stop()

	key := probeKey{service: "eth", backend: "https://node1.example.com", check: "block_number"}
	deadline := time.Now().Add(2 * time.Second)
	for !exec.coveredByPeer(key, defaultInterval, time.Now()) {
		if time.Now().After(deadline) {
			t.Fatal("the peer feed's result never counted as coverage")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
