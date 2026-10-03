package shannon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
)

// Review of 5fd0d96..c3a3a7a for new load and deploy-time state. Each test
// asserts the behaviour the gateway had, or should have, and fails on
// c3a3a7a.

// fetchCountingNode counts GetSession calls, optionally holding each one and
// failing it. Fields are set before any concurrent use.
type fetchCountingNode struct {
	fullNodeIface
	calls atomic.Int64
	hold  time.Duration
	err   error
}

func (c *fetchCountingNode) GetSession(ctx context.Context, serviceID, appAddr string) (*sessiontypes.Session, error) {
	c.calls.Add(1)
	if c.hold > 0 {
		time.Sleep(c.hold)
	}
	if c.err != nil {
		return nil, c.err
	}
	return c.fullNodeIface.GetSession(ctx, serviceID, appAddr)
}

// suppressBackgroundRefresh marks a background refresh as in flight, so
// getSession's grace path schedules none and every counted fetch is one a
// caller waited on.
func suppressBackgroundRefresh(sm *sessionManager) {
	sm.bgRefreshing.Store(sessionCacheKey("eth", "pokt1app"), struct{}{})
}

// Past grace with the full node failing, a WebSocket dial costs two
// sequential GetSession calls: currentSession's own refresh, then, its grace
// fallback not applying, getSession's sync refresh. resolveEndpoint used
// getSession alone before 53f86eb: one.
func TestReviewLoad_CurrentSessionPastGraceFetchesTwiceOnError(t *testing.T) {
	fn := &fetchCountingNode{fullNodeIface: &stubFullNode{session: buildTestSession("s1", "pokt1supplier", "https://relay.example.com"), height: 105}}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	sm.graceBlocks.Store(10)
	sm.latestBlockHeight.Store(105)
	if _, err := sm.getSession(context.Background(), "eth", "pokt1app"); err != nil {
		t.Fatal(err)
	}
	fn.calls.Store(0)
	fn.err = errors.New("full node unavailable")
	sm.latestBlockHeight.Store(121) // grace ended at 120

	if _, err := sm.currentSession(context.Background(), "eth", "pokt1app"); err == nil {
		t.Fatal("precondition: past grace with the full node failing, the dial fails")
	}
	if got := fn.calls.Load(); got > 1 {
		t.Fatalf("one WebSocket dial past grace issued %d GetSession calls to a failing full node; want 1 (each is bounded by sessionFetchTimeout=%v, so %d x that per dial)",
			got, sessionFetchTimeout, got)
	}
}

// The wait ignores the caller: refreshSession detaches the context and
// singleflight.Do does not take one, so a dial whose client has already gone
// still waits out both fetches.
func TestReviewLoad_CurrentSessionIgnoresACancelledCaller(t *testing.T) {
	const hold = 150 * time.Millisecond
	fn := &fetchCountingNode{fullNodeIface: &stubFullNode{session: buildTestSession("s1", "pokt1supplier", "https://relay.example.com"), height: 105}}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	sm.graceBlocks.Store(10)
	sm.latestBlockHeight.Store(105)
	if _, err := sm.getSession(context.Background(), "eth", "pokt1app"); err != nil {
		t.Fatal(err)
	}
	fn.calls.Store(0)
	fn.hold, fn.err = hold, errors.New("full node unavailable")
	sm.latestBlockHeight.Store(121)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, _ = sm.currentSession(ctx, "eth", "pokt1app")
	if elapsed := time.Since(start); elapsed >= hold {
		t.Fatalf("a dial whose client had already gone waited %v on the full node (%d fetches of %v each); want it released", elapsed.Round(time.Millisecond), fn.calls.Load(), hold)
	}
}

// Inside grace the ended session is in hand and both miners' grace honours
// it for HTTP, yet a WebSocket dial now waits on the full node first. With a
// full node that hangs, each Open, each rebind and each probe waits up to
// sessionFetchTimeout (15s) before falling back to the session it already
// had. Before 53f86eb resolveEndpoint returned it at once.
func TestReviewLoad_WSDialInGraceWaitsOnAHungFullNode(t *testing.T) {
	const hold = 300 * time.Millisecond
	enabled := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: true}
	supplier := newEchoSupplier(t)
	r, _, _ := probeFixture(t, wsURL(supplier), 40, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`, enabled)
	p := r.deps.Protocol
	fn := &fetchCountingNode{fullNodeIface: p.fullNode}
	p.sessions.fullNode = fn
	p.sessions.graceBlocks.Store(10)
	p.sessions.latestBlockHeight.Store(105)
	ended, err := p.sessions.getSession(context.Background(), "eth", "pokt1app")
	if err != nil {
		t.Fatal(err)
	}
	p.sessions.getOrCreateEndpoints(ended)
	suppressBackgroundRefresh(p.sessions)

	fn.calls.Store(0)
	fn.hold, fn.err = hold, errors.New("context deadline exceeded")
	p.sessions.latestBlockHeight.Store(111) // one block into grace

	start := time.Now()
	target, reason, err := r.resolveEndpoint(context.Background(), "eth", map[domain.EndpointAddr]bool{}, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("precondition: inside grace the dial still resolves: %s: %v", reason, err)
	}
	if target.session.SessionId != ended.SessionId {
		t.Fatalf("precondition: the dial falls back to the ended session, got %s", target.session.SessionId)
	}
	if elapsed >= hold {
		t.Fatalf("a WebSocket dial inside grace waited %v on a hung full node (%d GetSession) before signing the session it already held; want no wait",
			elapsed.Round(time.Millisecond), fn.calls.Load())
	}
}

// A full node that does not serve the next session yet answers with the
// ended one (sage_session_fetches_total outcome=same_session). Every
// sequential WebSocket dial, rebind and probe in that window then pays its
// own GetSession round trip: the cached session's end stays below the
// height, so nothing stops the next one. HTTP serves the ended session
// through grace without waiting. Before 53f86eb a dial or probe in grace
// waited on no fetch.
func TestReviewLoad_SameSessionEveryWSDialFetches(t *testing.T) {
	enabled := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: true}
	supplier := newEchoSupplier(t)
	r, _, _ := probeFixture(t, wsURL(supplier), 40, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`, enabled)
	p := r.deps.Protocol
	fn := &fetchCountingNode{fullNodeIface: p.fullNode}
	p.sessions.fullNode = fn
	p.sessions.graceBlocks.Store(10)
	p.sessions.latestBlockHeight.Store(105)
	ended, err := p.sessions.getSession(context.Background(), "eth", "pokt1app")
	if err != nil {
		t.Fatal(err)
	}
	p.sessions.getOrCreateEndpoints(ended)
	suppressBackgroundRefresh(p.sessions)
	fn.calls.Store(0)
	p.sessions.latestBlockHeight.Store(111) // the full node still answers the session ending at 110

	const dials = 10
	for range dials {
		if _, reason, err := r.resolveEndpoint(context.Background(), "eth", map[domain.EndpointAddr]bool{}, nil); err != nil {
			t.Fatalf("resolveEndpoint: %s: %v", reason, err)
		}
	}
	afterDials := fn.calls.Load()

	addr := domain.EndpointAddr("pokt1supplier-" + wsURL(supplier))
	frame := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	if got := r.runProbe(context.Background(), wsProbeTarget{serviceID: "eth", addr: addr, url: wsURL(supplier), frame: frame}); got != wsProbeOK {
		t.Fatalf("precondition: probe = %s, want ok", got)
	}
	afterProbe := fn.calls.Load() - afterDials

	if afterDials > 1 || afterProbe > 0 {
		t.Fatalf("with the full node answering same_session inside grace: %d sequential dials issued %d GetSession, one probe %d; want at most 1 for the boundary and 0 per probe",
			dials, afterDials, afterProbe)
	}
}

// The health-check grouping (healthcheck/executor.go dialedURL), the peer
// coverage key (healthcheck/peer.go backendKey) and the breaker and method
// block host (relay/middleware dialedHost) all ask EndpointURLFor, which has
// no service: it reads byAddr, where the last session extracted wins. Two
// suppliers whose REST faces are rest-a and rest-b in eth, and one shared
// host in bsc, read as one REST backend for eth once bsc was extracted last:
// eth's REST check probes them as one group and charges both with one probe,
// and a REST circuit break on either takes both out of eth's pool. Which
// answer a pod holds turns on extraction order, so it differs between pods
// (breaker marks are shared through Redis) and flips at a rollover.
func TestReviewLoad_EndpointURLForAnswersAnotherServicesFace(t *testing.T) {
	sm := newSessionManager(nil, map[domain.ServiceID]struct{}{"eth": {}, "bsc": {}}, newTestLogger())
	p := &Protocol{sessions: sm, metrics: noopSupplierMetrics{}}

	stake := func(service, supplier, rpc, rest, id string, end int64) *sessiontypes.Session {
		s := buildMultiServiceSession(service, supplier, map[sharedtypes.RPCType]string{
			sharedtypes.RPCType_JSON_RPC: rpc,
			sharedtypes.RPCType_REST:     rest,
		})
		s.SessionId, s.Header.SessionId = id, id
		s.Header.SessionStartBlockHeight, s.Header.SessionEndBlockHeight = end-10, end
		return s
	}
	one := domain.EndpointAddr("pokt1one-https://rm1.example.com")
	two := domain.EndpointAddr("pokt1two-https://rm2.example.com")

	sm.getOrCreateEndpoints(stake("eth", "pokt1one", "https://rm1.example.com", "https://rest-a.example.com", "eth-1", 110))
	sm.getOrCreateEndpoints(stake("eth", "pokt1two", "https://rm2.example.com", "https://rest-b.example.com", "eth-2", 110))
	sm.getOrCreateEndpoints(stake("bsc", "pokt1one", "https://rm1.example.com", "https://rest-shared.example.com", "bsc-1", 110))
	sm.getOrCreateEndpoints(stake("bsc", "pokt1two", "https://rm2.example.com", "https://rest-shared.example.com", "bsc-2", 110))

	host := func(ep domain.EndpointAddr) string { return dialedHostVia(p, ep, domain.RPCTypeREST) }
	beforeOne, beforeTwo := host(one), host(two)

	// eth dials rest-a and rest-b: they are two backends to eth.
	if beforeOne == beforeTwo {
		t.Errorf("eth's REST faces rest-a and rest-b both resolve to %q: one eth REST probe charges both, one REST break removes both", beforeOne)
	}

	// eth's next session is extracted after bsc's: the same question now
	// gets eth's answer, so the group key and the breaker host move.
	sm.getOrCreateEndpoints(stake("eth", "pokt1one", "https://rm1.example.com", "https://rest-a.example.com", "eth-3", 120))
	if after := host(one); after != beforeOne {
		t.Errorf("the REST host of %s moved from %q to %q when another service's session rolled over; breaker marks and probe schedule keyed on it moved too",
			one, beforeOne, after)
	}
}

// dialedHostVia is relay/middleware's dialedHost over this protocol.
func dialedHostVia(p *Protocol, ep domain.EndpointAddr, rpcType domain.RPCType) string {
	if raw, ok := p.EndpointURLFor(ep, rpcType); ok {
		if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
			return u.Hostname()
		}
	}
	return ep.Domain()
}

// JSON-RPC groups keep their key: the address is built from the JSON-RPC URL
// when one is staked (PublicURL), so EndpointURLFor(addr, json_rpc) is the
// address's own URL. Health-check probeKeys and breaker hosts for JSON-RPC
// do not change form on the roll. (Passes: recorded as a checked area.)
func TestReviewLoad_JSONRPCKeyUnchanged(t *testing.T) {
	sm := newSessionManager(nil, map[domain.ServiceID]struct{}{"cosmos": {}}, newTestLogger())
	p := &Protocol{sessions: sm, metrics: noopSupplierMetrics{}}
	sm.getOrCreateEndpoints(buildMultiServiceSession("cosmos", "pokt1one", map[sharedtypes.RPCType]string{
		sharedtypes.RPCType_JSON_RPC:  "https://rpc.example.com",
		sharedtypes.RPCType_REST:      "https://rest.example.com",
		sharedtypes.RPCType_COMET_BFT: "https://comet.example.com",
		sharedtypes.RPCType_WEBSOCKET: "wss://ws.example.com",
	}))
	addr := domain.EndpointAddr("pokt1one-https://rpc.example.com")
	addrURL, _ := addr.URL()
	if got, ok := p.EndpointURLFor(addr, domain.RPCTypeJSONRPC); !ok || got != addrURL {
		t.Fatalf("EndpointURLFor(json_rpc) = %q, %v; want the address's own %q", got, ok, addrURL)
	}
	if got := dialedHostVia(p, addr, domain.RPCTypeJSONRPC); got != addr.Domain() {
		t.Fatalf("breaker host for json_rpc = %q; want the address host %q", got, addr.Domain())
	}
}

// benchProtocol is a protocol holding one session of n suppliers, each with
// JSON-RPC and REST on separate hosts, as relay traffic would leave it.
func benchProtocol(n int) (*Protocol, domain.EndpointAddrList) {
	sm := newSessionManager(nil, map[domain.ServiceID]struct{}{"eth": {}}, slog.New(slog.DiscardHandler))
	s := &sessiontypes.Session{
		SessionId: "bench",
		Header: &sessiontypes.SessionHeader{
			SessionId: "bench", ServiceId: "eth", ApplicationAddress: "pokt1app",
			SessionStartBlockHeight: 100, SessionEndBlockHeight: 110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
	}
	for i := range n {
		op := fmt.Sprintf("pokt1supplier%02d", i)
		s.Suppliers = append(s.Suppliers, &sharedtypes.Supplier{
			OperatorAddress: op, OwnerAddress: op + "-owner",
			Services: []*sharedtypes.SupplierServiceConfig{{ServiceId: "eth", Endpoints: []*sharedtypes.SupplierEndpoint{
				{Url: fmt.Sprintf("https://rpc%02d.example.com", i), RpcType: sharedtypes.RPCType_JSON_RPC},
				{Url: fmt.Sprintf("https://rest%02d.example.com/v1", i), RpcType: sharedtypes.RPCType_REST},
			}}},
		})
	}
	var addrs domain.EndpointAddrList
	for addr := range sm.getOrCreateEndpoints(s) {
		addrs = append(addrs, addr)
	}
	return &Protocol{sessions: sm, metrics: noopSupplierMetrics{}}, addrs
}

// The circuit breaker's pre-filter per request over a 50-endpoint pool:
// before 696b456 it took ep.Domain(); now dialedHost.
func BenchmarkReviewLoad_BreakerPrefilterAddressHost(b *testing.B) {
	_, addrs := benchProtocol(50)
	b.ReportAllocs()
	for b.Loop() {
		for _, ep := range addrs {
			_ = ep.Domain()
		}
	}
}

func BenchmarkReviewLoad_BreakerPrefilterDialedHost(b *testing.B) {
	p, addrs := benchProtocol(50)
	b.ReportAllocs()
	for b.Loop() {
		for _, ep := range addrs {
			_ = dialedHostVia(p, ep, domain.RPCTypeJSONRPC)
		}
	}
}

// checkKeyURL, once per HTTP relay in sendRelay.
func BenchmarkReviewLoad_CheckKeyURL(b *testing.B) {
	p, addrs := benchProtocol(50)
	ep := addrs[0]
	dialed, _ := p.EndpointURLFor(ep, domain.RPCTypeREST)
	b.ReportAllocs()
	for b.Loop() {
		p.checkKeyURL("eth", ep, domain.RPCTypeREST, dialed)
	}
}
