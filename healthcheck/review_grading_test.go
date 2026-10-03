package healthcheck

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/cosmos"
	"github.com/pokt-network/sage/reputation"
)

// Review of 5fd0d96..c3a3a7a: grading changes that can hide a broken
// supplier. Each test asserts the invariant; on c3a3a7a they fail.

// A poktroll miner that refuses every relay (relayer_proxy 14: its ingress
// mangles the body SAGE sends every supplier alike; or 13: a max_body_size
// below a signed request) refuses the probes too. The relay path grades it
// miner_request_refused (no penalty, no method block); the probe gets
// TransportSeverity "" and applyResult records nothing (executor.go:1022-1030,
// 1469-1478); no height is learned, and an unknown height is admitted by
// selection (qos/endpointstore.go HeightGetter). Nothing moves the key from
// where it stands, 100 for a fresh one (reputation/service.go:197), tier 1.
func TestReview_ProbeRefusedOnEveryRequestRecordsNothing(t *testing.T) {
	for _, code := range []uint32{13, 14} {
		refusal := domain.NewRelayError(domain.ErrEndpoint, "relay miner refused the relay",
			&domain.MinerError{Codespace: "relayer_proxy", Code: code, Message: "refused"}, true)
		rep := &stubRepService{}
		exe := newTestExecutor(&stubRelayer{err: refusal},
			&stubEndpointProvider{endpoints: domain.EndpointAddrList{"supplierA-https://node1.example.com"}},
			&stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}},
			probeableRegistry(t, "eth"), rep)
		exe.runOnce(context.Background())
		exe.wg.Wait()

		rep.mu.Lock()
		n := len(rep.signals)
		rep.mu.Unlock()
		if n == 0 {
			t.Errorf("relayer_proxy %d on every probe: no signal recorded; with the relay path also unscored, "+
				"a supplier that refuses everything is never graded by anything", code)
		}
	}
}

// A REST face answering 200 with "OK" or a lone newline to every route (a
// default backend behind a misrouted vhost) now passes the relay verdict
// (analyzer.go: REST empty or plain text is success, +5 per relay at
// reputation/signals.go:54) and passes the Cosmos REST probe too: rest_syncing
// is not Essential and Cosmos ExtractData only rejects a zero-length body
// (qos/cosmos/plugin.go:418), so it records a success. Nothing grades the
// backend down.
func TestReview_CosmosRESTProbePassesABodyThatAnswersNothing(t *testing.T) {
	reg := qos.NewRegistry()
	if err := reg.Register("osmosis", cosmos.NewPlugin(nil, cosmos.Config{})); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"OK", "\n"} {
		rep := &stubRepService{}
		exe := newTestExecutor(&stubRelayer{}, &stubEndpointProvider{},
			&stubSessionManager{services: map[domain.ServiceID]struct{}{"osmosis": {}}}, reg, rep)
		exe.applyResult(context.Background(), ProbeResult{
			ServiceID: "osmosis", Endpoint: "supplierA-https://rest.example.com",
			Check: "rest_syncing", RPCType: domain.RPCTypeREST,
			StatusCode: 200, Body: []byte(body), ProbedAt: time.Now(), Source: ResultSourceProbe,
		})
		rep.mu.Lock()
		signals := append([]signalRecord(nil), rep.signals...)
		rep.mu.Unlock()
		if len(signals) == 1 && signals[0].signal.Type == reputation.SignalSuccess {
			t.Errorf("REST 200 %q: the relay verdict passes it and the rest_syncing probe records %s; "+
				"a REST face that answers nothing is graded by nothing", body, signals[0].signal.Type)
		}
	}
}

// TRON's REST face (/wallet/*, 28% of TRON traffic per the plugin's own doc)
// is a separate reputation key and has no probe of its own: the plugin's
// checks are EVM's JSON-RPC ones. Client traffic is the only thing that
// grades it, so the relay verdict must, by default, grade a REST face
// answering 200 with nothing as the failure it is
// (featureflag.FlagRESTBodiesAsAnswers off).
func TestReview_TronRESTFaceHasNoGraderForAnEmptyAnswer(t *testing.T) {
	if featureflag.DefaultFlags[featureflag.FlagRESTBodiesAsAnswers] {
		t.Fatal("rest_bodies_as_answers is on by default: a TRON REST face answering 200 with nothing is graded by nothing")
	}
	if v := heuristic.Analyze(nil, 200, domain.RPCTypeREST); v.IsSuccess() || !v.ShouldPenalize {
		t.Fatalf("an empty REST 200 graded %q penalize=%v by default; want a scored failure", v.Reason, v.ShouldPenalize)
	}
}

// With max_age 0 (the default; config/service.go:924 documents it as "the
// check's own interval") a peer's result covers a check for
// max(check.Interval, service interval) (executor.go:591-592), but is applied
// only within the service interval (peer.go peerAgeLimit). A peer result for
// a check with its own longer interval (EVM chain_id, 5 minutes), arriving
// between the two ages (boot replay of the stream, a feed resume after a
// Redis blip, clock skew), is dropped and still suppresses this instance's
// own probe: the verdict it carries reaches this instance neither way until
// the window closes.
func TestReview_PeerResultDroppedAsStaleStillCoversALongerIntervalCheck(t *testing.T) {
	payload := domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_chainId")
	reg := qos.NewRegistry()
	if err := reg.Register("eth", &checkOnlyPlugin{checks: []qos.HealthCheck{{Name: "chain_id", Payload: payload, Interval: 5 * time.Minute}}}); err != nil {
		t.Fatal(err)
	}
	relayer := &stubRelayer{response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)}}
	rep := &stubRepService{}
	exec := newTestExecutor(relayer,
		&stubEndpointProvider{endpoints: domain.EndpointAddrList{localA, localB}},
		&stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}}, reg, rep)
	exec.SetPeerSource(idleSource{}, nil)

	// The peer saw node1 fail its chain_id check two minutes ago.
	r := peerResult(peerOnNode1, exec.now().Add(-2*time.Minute))
	r.Check, r.StatusCode, r.Body = "chain_id", 500, []byte(`{"error":"internal"}`)
	exec.applyPeerResult(context.Background(), r)

	rep.mu.Lock()
	applied := len(rep.signals)
	rep.mu.Unlock()

	exec.runOnce(context.Background())
	exec.wg.Wait()
	probedNode1 := slices.Contains(relayedTo(relayer), localA)

	if applied == 0 && !probedNode1 {
		t.Fatalf("a 2m-old peer chain_id failure was dropped as stale (service interval %v) yet covered node1 "+
			"(check interval 5m): this instance neither applied the peer's verdict nor probed node1 itself", defaultInterval)
	}
}
