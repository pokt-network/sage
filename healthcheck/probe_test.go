package healthcheck

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
)

type fixedLeader bool

func (l fixedLeader) IsLeader() bool { return bool(l) }

// memSink collects published results.
type memSink struct {
	mu      sync.Mutex
	results []ProbeResult
}

func (s *memSink) Publish(_ context.Context, r ProbeResult) error {
	s.mu.Lock()
	s.results = append(s.results, r)
	s.mu.Unlock()
	return nil
}

func (s *memSink) all() []ProbeResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ProbeResult(nil), s.results...)
}

// A follower must not spend a single relay: probing is the leader's job and
// every probe is paid for from the app's stake.
func TestRunOnce_FollowerSendsNoRelays(t *testing.T) {
	exe, relayer, rep, _ := dedupTestFixture(t)
	exe.SetLeader(fixedLeader(false))

	exe.runOnce(context.Background())
	exe.wg.Wait()

	relayer.mu.Lock()
	defer relayer.mu.Unlock()
	if len(relayer.calls) != 0 {
		t.Fatalf("follower sent %d relays, want 0", len(relayer.calls))
	}
	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.signals) != 0 {
		t.Fatalf("follower recorded %d signals from probes it never made", len(rep.signals))
	}
}

// The leader publishes one result per probe, carrying what a follower needs
// to apply it: the backend's siblings, the check, and the response.
func TestRunOnce_LeaderPublishesOneResultPerProbe(t *testing.T) {
	exe, relayer, _, _ := dedupTestFixture(t)
	sink := &memSink{}
	exe.SetLeader(fixedLeader(true))
	exe.SetProbeSink(sink)

	exe.runOnce(context.Background())
	exe.wg.Wait()

	relayer.mu.Lock()
	probes := len(relayer.calls)
	relayer.mu.Unlock()
	got := sink.all()
	if probes == 0 || len(got) != probes {
		t.Fatalf("published %d results for %d probes", len(got), probes)
	}
	for _, r := range got {
		if r.ServiceID != "eth" || r.Endpoint == "" || len(r.Siblings) == 0 || r.Check == "" || r.StatusCode != 200 || len(r.Body) == 0 {
			t.Fatalf("incomplete result: %+v", r)
		}
	}
}

// A streamed result applied on a follower must produce exactly what the
// probe produced on the leader: one signal per backend key, the height on
// every sibling.
func TestApplyResult_StreamedMatchesProbe(t *testing.T) {
	exe, _, rep, plugin := dedupTestFixture(t)
	exe.SetLeader(fixedLeader(false))

	siblings := domain.EndpointAddrList{
		"supplierA-https://node1.example.com",
		"supplierB-https://node1.example.com",
		"supplierC-https://node1.example.com",
	}
	exe.applyResult(context.Background(), ProbeResult{
		ServiceID:  "eth",
		Endpoint:   siblings[1],
		Siblings:   siblings,
		Check:      "block_height",
		RPCType:    domain.RPCTypeJSONRPC,
		Request:    []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`),
		StatusCode: 200,
		Body:       []byte(`{"jsonrpc":"2.0","result":"0x10","id":1}`),
		LatencyMS:  12,
		Source:     ResultSourceStream,
	})

	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.onceCalls) != 1 || len(rep.onceCalls[0]) != 3 {
		t.Fatalf("onceCalls = %v, want one call naming the three siblings", rep.onceCalls)
	}
	for _, sig := range rep.signals {
		if sig.signal.Type != reputation.SignalSuccess || !sig.signal.Probe {
			t.Fatalf("signal = %+v, want a probe success", sig)
		}
	}
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	for _, s := range siblings {
		if plugin.updates[s] != plugin.height {
			t.Fatalf("sibling %s height = %d, want %d", s, plugin.updates[s], plugin.height)
		}
	}
}

// A transport failure travels as its verdict, not its error, and lands on
// the probed endpoint only.
func TestApplyResult_TransportFailureIsGradedFromVerdict(t *testing.T) {
	exe, _, rep, _ := dedupTestFixture(t)
	exe.applyResult(context.Background(), ProbeResult{
		ServiceID:         "eth",
		Endpoint:          "supplierA-https://node1.example.com",
		Siblings:          domain.EndpointAddrList{"supplierA-https://node1.example.com", "supplierB-https://node1.example.com"},
		Check:             "block_height",
		RPCType:           domain.RPCTypeJSONRPC,
		TransportError:    "dial tcp: connection refused",
		TransportSeverity: "critical",
		TransportReason:   "transport_connect_failed",
		Source:            ResultSourceStream,
	})
	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.signals) != 1 || rep.signals[0].signal.Type != reputation.SignalCriticalError || rep.signals[0].endpoint != "supplierA-https://node1.example.com" {
		t.Fatalf("signals = %+v, want one critical on the probed endpoint", rep.signals)
	}
}

func TestProbeResult_JSONRoundTrip(t *testing.T) {
	in := ProbeResult{
		ServiceID: "eth", Endpoint: "a-https://x", Siblings: domain.EndpointAddrList{"a-https://x", "b-https://x"},
		Check: "chain_id", RPCType: domain.RPCTypeJSONRPC, Request: []byte(`{}`), StatusCode: 200, Body: []byte(`{"r":1}`),
		LatencyMS: 5, TransportError: "", ProbedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out ProbeResult
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	out.Source = in.Source
	if out.ServiceID != in.ServiceID || out.Endpoint != in.Endpoint || len(out.Siblings) != 2 || string(out.Body) != string(in.Body) || !out.ProbedAt.Equal(in.ProbedAt) {
		t.Fatalf("round trip lost data: %+v", out)
	}
}

// The executor's source loop must feed applyResult and survive a source
// that returns.
func TestExecutor_SourceFeedsApply(t *testing.T) {
	exe, _, rep, _ := dedupTestFixture(t)
	exe.SetLeader(fixedLeader(false))
	src := &memSource{results: []ProbeResult{{
		ServiceID: "eth", Endpoint: "supplierD-https://node2.example.com",
		Siblings: domain.EndpointAddrList{"supplierD-https://node2.example.com"},
		Check:    "block_height", RPCType: domain.RPCTypeJSONRPC, StatusCode: 200, Body: []byte(`{"result":"0x1"}`),
	}}}
	exe.SetProbeSource(src)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exe.Start(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for {
		rep.mu.Lock()
		n := len(rep.signals)
		rep.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("streamed result never applied")
		}
		time.Sleep(5 * time.Millisecond)
	}
	exe.Stop()
}

type memSource struct{ results []ProbeResult }

func (m *memSource) Run(ctx context.Context, apply func(ProbeResult)) error {
	for _, r := range m.results {
		apply(r)
	}
	<-ctx.Done()
	return errors.New("done")
}

var _ qos.Plugin = (*heightPlugin)(nil)
