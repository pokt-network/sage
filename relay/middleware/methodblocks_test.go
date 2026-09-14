package middleware

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/methodblock"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// normPlugin is a qos.Plugin that names every payload's method verbatim,
// except "" which it reports as no method.
type normPlugin struct{}

func (normPlugin) ParseRequest(context.Context, *http.Request, []byte, domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}
func (normPlugin) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}
func (normPlugin) NormalizeMethod(p domain.Payload) string { return p.Method() }

type spyEvents struct {
	mu     sync.Mutex
	events []string
}

func (s *spyEvents) RecordMethodBlockEvent(_ domain.ServiceID, method, event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event+":"+method)
}

func methodCtx(method string, eps domain.EndpointAddrList) *relay.Context {
	ctx := baseContext()
	ctx.Endpoints = eps
	ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, method)}
	return ctx
}

func registryWith(t *testing.T) *qos.Registry {
	t.Helper()
	reg := qos.NewRegistry()
	if err := reg.Register("eth", normPlugin{}); err != nil {
		t.Fatal(err)
	}
	return reg
}

// stubRepService is a minimal reputation.Service for MethodBlocks tests. It
// tracks a fixed score per endpoint and derives Vouched from the same
// probation threshold reputation.DefaultSelectorConfig uses; an endpoint
// absent from scores has no recorded score, so it is NOT vouched — mirroring
// the cold-start case where a dead host still carries the initial score.
type stubRepService struct {
	scores map[domain.EndpointAddr]float64
}

var _ reputation.Service = (*stubRepService)(nil)

func (s *stubRepService) RecordSignal(context.Context, domain.ServiceID, domain.EndpointAddr, domain.RPCType, reputation.Signal) error {
	return nil
}

func (s *stubRepService) GetScore(context.Context, domain.ServiceID, domain.EndpointAddr, domain.RPCType) (float64, error) {
	return 100, nil
}

func (s *stubRepService) GetScores(context.Context, domain.ServiceID) (map[string]float64, error) {
	return nil, nil
}

func (s *stubRepService) SelectBest(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType) domain.EndpointAddr {
	if len(eps) == 0 {
		return ""
	}
	return eps[0]
}

func (s *stubRepService) SelectSpread(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType, _ map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(eps) == 0 {
		return ""
	}
	return eps[0]
}

func (s *stubRepService) ResetScore(context.Context, domain.ServiceID, domain.EndpointAddr) error {
	return nil
}

func (s *stubRepService) Vouched(_ context.Context, _ domain.ServiceID, ep domain.EndpointAddr, _ domain.RPCType) bool {
	score, known := s.scores[ep]
	if !known {
		return false
	}
	return score >= reputation.DefaultSelectorConfig().ProbationThreshold
}

// A timeout on eth_getLogs marks the host for eth_getLogs and nothing else.
func TestMethodBlocks_TimeoutMarksOnlyThatMethod(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true, Reason: "transport_timeout"}
		return retryableErr("timeout")
	})
	h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), nil, nil)(inner)
	_ = h.HandleRelay(methodCtx("eth_getLogs", eps))

	if !store.Blocked("eth", eps[0].Domain(), "eth_getLogs") {
		t.Fatal("timed-out method not marked")
	}
	if store.Blocked("eth", eps[0].Domain(), "eth_call") {
		t.Fatal("another method was blocked")
	}
}

// The filter must remove exactly the blocked host for the blocked method —
// built so ONE endpoint survives, because filter-all and filter-none look the
// same once selection falls back to the unfiltered list.
func TestMethodBlocks_FiltersBlockedHostForThatMethodOnly(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	store.Mark("eth", eps[0].Domain(), "eth_getLogs", true)

	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), nil, nil)(inner)

	_ = h.HandleRelay(methodCtx("eth_getLogs", eps))
	if len(seen) != 1 || seen[0] != eps[1] {
		t.Fatalf("eth_getLogs saw %v, want only %v", seen, eps[1])
	}

	_ = h.HandleRelay(methodCtx("eth_call", eps))
	if len(seen) != 2 {
		t.Fatalf("eth_call saw %v, want both hosts", seen)
	}
}

// A block must never route a method onto a host selection already considers
// junk. A is blocked for the method; B survives the filter but scores below
// the probation threshold, so the survivor is not vouched for and the
// middleware must fall back to the empty-case behavior: degrade and serve
// everything.
func TestMethodBlocks_BypassesWhenNoSurvivorIsVouched(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	store.Mark("eth", eps[0].Domain(), "eth_getLogs", true)
	rep := &stubRepService{scores: map[domain.EndpointAddr]float64{eps[1]: 0}}
	events := &spyEvents{}

	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		return nil
	})
	h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), rep, events)(inner)

	ctx := methodCtx("eth_getLogs", eps)
	_ = h.HandleRelay(ctx)
	if len(seen) != 2 {
		t.Fatalf("inner must see both endpoints, saw %v", seen)
	}
	if !ctx.Degraded {
		t.Fatal("bypass must mark the relay degraded")
	}
	if len(events.events) != 1 || events.events[0] != "bypass:eth_getLogs" {
		t.Fatalf("events = %v", events.events)
	}
}

// Same setup as above, except B scores above the probation threshold: the
// survivor is vouched for, so the filter applies normally.
func TestMethodBlocks_FiltersWhenSurvivorIsVouched(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	store.Mark("eth", eps[0].Domain(), "eth_getLogs", true)
	rep := &stubRepService{scores: map[domain.EndpointAddr]float64{eps[1]: 60}}
	events := &spyEvents{}

	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		return nil
	})
	h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), rep, events)(inner)

	ctx := methodCtx("eth_getLogs", eps)
	_ = h.HandleRelay(ctx)
	if len(seen) != 1 || seen[0] != eps[1] {
		t.Fatalf("inner saw %v, want only %v", seen, eps[1])
	}
	if ctx.Degraded {
		t.Fatal("a vouched survivor must not degrade the relay")
	}
	if len(events.events) != 0 {
		t.Fatalf("events = %v, want none", events.events)
	}
}

// The sei shape: four vouched hosts, three blocked for the method. One
// vouched survivor would take all of the method's load, so the block is
// bypassed; with two of four left the filter applies.
func TestMethodBlocks_BypassesWhenFilterShedsMostVouchedCapacity(t *testing.T) {
	eps := testEndpoints(5)
	rep := &stubRepService{scores: map[domain.EndpointAddr]float64{
		eps[0]: 70, eps[1]: 70, eps[2]: 70, eps[3]: 70, eps[4]: 0,
	}}
	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		return nil
	})

	for _, tc := range []struct {
		blocked  int
		wantSeen int
	}{{blocked: 3, wantSeen: 5}, {blocked: 2, wantSeen: 3}} {
		store := methodblock.New()
		for _, ep := range eps[:tc.blocked] {
			store.Mark("eth", ep.Domain(), "eth_call", true)
		}
		h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), rep, nil)(inner)
		ctx := methodCtx("eth_call", eps)
		_ = h.HandleRelay(ctx)
		if len(seen) != tc.wantSeen {
			t.Fatalf("%d of 4 vouched blocked: inner saw %d endpoints, want %d", tc.blocked, len(seen), tc.wantSeen)
		}
		if ctx.Degraded != (tc.wantSeen == len(eps)) {
			t.Fatalf("%d blocked: degraded = %v", tc.blocked, ctx.Degraded)
		}
	}
}

// B has no recorded score at all — the cold-start case that let a mark
// divert onto a DNS-dead host right after boot, before the first health
// check: scoreForSelector would answer InitialScore, but Vouched must not.
// The middleware must bypass rather than absorb this diversion into the
// filtered list.
func TestMethodBlocks_UnknownSurvivorDoesNotAbsorbADiversion(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	store.Mark("eth", eps[0].Domain(), "eth_getLogs", true)
	rep := &stubRepService{scores: map[domain.EndpointAddr]float64{}}
	events := &spyEvents{}

	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		return nil
	})
	h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), rep, events)(inner)

	ctx := methodCtx("eth_getLogs", eps)
	_ = h.HandleRelay(ctx)
	if len(seen) != 2 {
		t.Fatalf("inner must see both endpoints, saw %v", seen)
	}
	if !ctx.Degraded {
		t.Fatal("bypass must mark the relay degraded")
	}
	if len(events.events) != 1 || events.events[0] != "bypass:eth_getLogs" {
		t.Fatalf("events = %v", events.events)
	}
}

// A block must never empty a pool. Every host blocked ⇒ degrade and serve
// the unfiltered list, and say so.
func TestMethodBlocks_EveryHostBlockedDegradesInsteadOfEmptying(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	for _, ep := range eps {
		store.Mark("eth", ep.Domain(), "eth_getLogs", true)
	}
	events := &spyEvents{}
	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		return nil
	})
	h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), nil, events)(inner)

	ctx := methodCtx("eth_getLogs", eps)
	_ = h.HandleRelay(ctx)
	if len(seen) != 2 {
		t.Fatalf("pool emptied: %v", seen)
	}
	if !ctx.Degraded {
		t.Fatal("bypass must mark the relay degraded")
	}
	if len(events.events) != 1 || events.events[0] != "bypass:eth_getLogs" {
		t.Fatalf("events = %v", events.events)
	}
}

func TestMethodBlocks_ThirdMethodEscalatesAndIsCounted(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(1)
	events := &spyEvents{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true, Attribution: heuristic.AttrSupplier}
		return retryableErr("timeout")
	})
	h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), nil, events)(inner)
	for _, m := range []string{"a", "b", "c"} {
		_ = h.HandleRelay(methodCtx(m, eps))
	}
	if !store.Blocked("eth", eps[0].Domain(), "anything") {
		t.Fatal("host not escalated")
	}
	want := []string{"mark:a", "mark:b", "escalate:c"}
	if len(events.events) != 3 {
		t.Fatalf("events = %v", events.events)
	}
	for i := range want {
		if events.events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events.events, want)
		}
	}
}

// -32601 is MethodBlocking and AttrClient. A healthy node that simply does
// not run debug_*/trace_* answers it to as many catalogued methods as a
// client asks for, so those marks must never add up to a host-wide block —
// otherwise any client can remove a good host from every method. The same
// three methods failing with supplier-attributed timeouts must escalate.
func TestMethodBlocks_ClientAttributedMarksDoNotEscalate(t *testing.T) {
	run := func(t *testing.T, attr heuristic.ErrorAttribution) *methodblock.Store {
		t.Helper()
		store := methodblock.New()
		eps := testEndpoints(1)
		inner := relay.HandlerFunc(func(ctx *relay.Context) error {
			ctx.Endpoint = eps[0]
			ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true, Attribution: attr}
			return retryableErr("failed")
		})
		h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), nil, nil)(inner)
		for _, m := range []string{"debug_traceCall", "trace_block", "debug_storageRangeAt"} {
			_ = h.HandleRelay(methodCtx(m, eps))
		}
		return store
	}

	host := testEndpoints(1)[0].Domain()

	client := run(t, heuristic.AttrClient)
	if client.Blocked("eth", host, "eth_call") {
		t.Fatal("three -32601s host-blocked a healthy node")
	}
	if !client.Blocked("eth", host, "debug_traceCall") {
		t.Fatal("a -32601 must still keep that one method away from the host")
	}

	supplier := run(t, heuristic.AttrSupplier)
	if !supplier.Blocked("eth", host, "eth_call") {
		t.Fatal("three supplier-attributed timeouts must escalate to a host block")
	}
}

// Built with two endpoints and only one pre-blocked, so a guard that failed
// to short-circuit would be caught filtering it out: len(seen) would drop to
// 1 instead of staying 2, and the unblocked host would get marked instead of
// staying clean.
func TestMethodBlocks_NoNormalizerPassesThrough(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	store.Mark("eth", eps[0].Domain(), "eth_getLogs", true)
	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		ctx.Endpoint = eps[1] // the UNBLOCKED host
		ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true}
		return retryableErr("timeout")
	})
	// Registry with no plugin for "eth".
	h := MethodBlocks(store, qos.NewRegistry(), nil, newFlags("method_blocks"), nil, nil)(inner)
	_ = h.HandleRelay(methodCtx("eth_getLogs", eps))
	if len(seen) != 2 {
		t.Fatal("without a normalizer nothing may be filtered")
	}
	if store.Blocked("eth", eps[1].Domain(), "eth_getLogs") {
		t.Fatal("without a normalizer nothing may be marked")
	}
}

// Built with two endpoints and only one pre-blocked, so a guard that failed
// to short-circuit would be caught filtering it out: len(seen) would drop to
// 1 instead of staying 2.
func TestMethodBlocks_FlagOffPassesThrough(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	store.Mark("eth", eps[0].Domain(), "eth_getLogs", true)
	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error { seen = ctx.Endpoints; return nil })
	h := MethodBlocks(store, registryWith(t), nil, newFlags(), nil, nil)(inner)
	_ = h.HandleRelay(methodCtx("eth_getLogs", eps))
	if len(seen) != 2 {
		t.Fatal("flag off must not filter")
	}
}

// The reason this middleware sits INSIDE Hedge: the losing arm's timeout is
// the whole incident, and Observe never sees a loser. Through the real
// Hedge, a slow primary must mark its host, and the next request's hedge
// must not land there for that method.
//
// Three endpoints, not two, and the healthy ones are slow enough to force a
// hedge on request 2. With two endpoints the primary's own filter already
// removed the slow host on request 2 and the healthy host answered inside the
// hedge delay, so no hedge arm ever ran and the "next hedge avoids" claim was
// untested; worse, the hedge that would have run had only the blocked host
// left and would have taken the bypass path straight back onto it.
//
// Timings are one order of magnitude apart on purpose: 5ms hedge delay, 20ms
// healthy, 100ms slow. The old 5ms-versus-30ms margin was thin under -race.
func TestMethodBlocks_LosingHedgeArmMarksAndNextHedgeAvoids(t *testing.T) {
	const (
		hedgeDelay  = 5 * time.Millisecond
		healthyWork = 20 * time.Millisecond
		slowWork    = 100 * time.Millisecond
	)

	store := methodblock.New()
	eps := testEndpoints(3)
	slow := eps[0]

	var mu sync.Mutex
	var attempts []domain.EndpointAddr
	seen := func() []domain.EndpointAddr {
		mu.Lock()
		defer mu.Unlock()
		return append([]domain.EndpointAddr(nil), attempts...)
	}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		// "select": first available endpoint.
		ctx.Endpoint = ctx.Endpoints[0]
		if ctx.SelectedEndpoint != nil {
			ep := ctx.Endpoint
			ctx.SelectedEndpoint.Store(&ep)
		}
		mu.Lock()
		attempts = append(attempts, ctx.Endpoint)
		mu.Unlock()
		if ctx.Endpoint == slow {
			time.Sleep(slowWork)
			ctx.HeuristicResult = &heuristic.AnalysisResult{
				MethodBlocking: true,
				Attribution:    heuristic.AttrSupplier,
				Reason:         "transport_timeout",
			}
			return retryableErr("timeout")
		}
		// Healthy, but slower than the hedge delay, so a hedge arm must fire.
		time.Sleep(healthyWork)
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	cfg := func(domain.ServiceID) config.RetryConfig { return config.RetryConfig{HedgeDelay: hedgeDelay} }
	chain := Hedge(newFlags("hedge", "method_blocks"), cfg)(
		MethodBlocks(store, registryWith(t), nil, newFlags("hedge", "method_blocks"), nil, nil)(inner))

	// Request 1: primary picks slow, hedge picks a healthy one and wins; the
	// slow arm finishes later and marks its host.
	if err := chain.HandleRelay(methodCtx("eth_getLogs", eps)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !store.Blocked("eth", slow.Domain(), "eth_getLogs") {
		if time.Now().After(deadline) {
			t.Fatalf("losing arm's timeout never marked the host; attempts: %v", seen())
		}
		time.Sleep(time.Millisecond)
	}

	// Request 2: the primary's filter drops the slow host, so the primary is
	// healthy-but-slow and a hedge MUST fire. The hedge arm is steered off the
	// primary's endpoint and must land on the third host, never back on the
	// blocked one.
	mu.Lock()
	attempts = nil
	mu.Unlock()
	if err := chain.HandleRelay(methodCtx("eth_getLogs", eps)); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for len(seen()) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("hedge never fired; attempts: %v", seen())
		}
		time.Sleep(time.Millisecond)
	}
	got := seen()
	if len(got) != 2 {
		t.Fatalf("want exactly two attempts (primary + hedge), got %v", got)
	}
	for _, a := range got {
		if a == slow {
			t.Fatalf("blocked host was attempted again: %v", got)
		}
	}
	if got[0] == got[1] {
		t.Fatalf("hedge arm did not move off the primary's endpoint: %v", got)
	}
}

// stubProvider hands out a fixed endpoint list, standing in for the protocol
// when nothing upstream (circuit_break with its flag off, or a chain without
// it) has populated ctx.Endpoints.
type stubProvider struct{ eps domain.EndpointAddrList }

func (p stubProvider) AvailableEndpoints(context.Context, domain.ServiceID, domain.RPCType) (domain.EndpointAddrList, error) {
	return p.eps, nil
}

// TestMethodBlocks_FetchesEndpointsWhenUpstreamDidNot: the filter must not
// depend on circuit_break having run. With circuit_breaker flagged off (or
// absent from the chain) nothing populates ctx.Endpoints before this
// middleware, and a block that only applies to a pre-populated list is a block
// that silently stops applying the moment an admin flips that flag.
func TestMethodBlocks_FetchesEndpointsWhenUpstreamDidNot(t *testing.T) {
	eps := testEndpoints(3)
	store := methodblock.New()
	store.Mark("eth", eps[0].Domain(), "eth_getLogs", true)

	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		return nil
	})
	h := MethodBlocks(store, registryWith(t), stubProvider{eps}, newFlags("method_blocks"), nil, nil)(inner)

	ctx := methodCtx("eth_getLogs", nil) // nothing upstream populated it
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatalf("expected the marked host filtered out of a self-fetched list, inner saw %v", seen)
	}
	for _, ep := range seen {
		if ep == eps[0] {
			t.Fatalf("marked host %s reached selection", eps[0])
		}
	}
}

// TestMethodBlocks_OtherBucketIsNeverMarkedOrFiltered: every uncatalogued
// method shares qos.MethodOther, so a mark on it is a mark on ALL of them. One
// client sending a bogus method name to each host would otherwise divert every
// legitimate uncatalogued method for every client for a TTL.
func TestMethodBlocks_OtherBucketIsNeverMarkedOrFiltered(t *testing.T) {
	eps := testEndpoints(3)
	store := methodblock.New()
	// A pre-existing mark on the bucket must not filter either.
	store.Mark("eth", eps[0].Domain(), qos.MethodOther, true)

	var seen domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seen = ctx.Endpoints
		ctx.Endpoint = eps[1]
		ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true, Attribution: heuristic.AttrClient}
		return nil
	})
	h := MethodBlocks(store, registryWith(t), nil, newFlags("method_blocks"), nil, nil)(inner)

	ctx := methodCtx(qos.MethodOther, eps)
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 {
		t.Fatalf("the _other bucket must not filter; inner saw %v", seen)
	}
	if store.Blocked("eth", eps[1].Domain(), qos.MethodOther) {
		t.Fatal("a MethodBlocking verdict on an uncatalogued method must not mark the _other bucket")
	}
}

// familyPlugin is normPlugin plus a family: any eth_ method belongs with the
// other two, the way the cosmos plugin answers for the EVM face of kava.
type familyPlugin struct{ normPlugin }

func (familyPlugin) MethodFamily(method string) []string {
	if strings.HasPrefix(method, "eth_") {
		return []string{"eth_blockNumber", "eth_call", "eth_getLogs"}
	}
	return nil
}

// A -32601 on a catalogued method marks the host for the whole family the
// plugin names, so the pool stops paying one failed relay per host and
// method to learn what one answer already said. A supplier-attributed
// verdict on the same method marks that method alone, and the family marks
// never escalate to a host-wide block.
func TestMethodBlocks_MethodNotFoundMarksTheFamily(t *testing.T) {
	reg := qos.NewRegistry()
	if err := reg.Register("kava", familyPlugin{}); err != nil {
		t.Fatal(err)
	}
	store := methodblock.New()
	events := &spyEvents{}
	eps := testEndpoints(1)
	host := eps[0].Domain()

	notFound := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			MethodBlocking: true, Attribution: heuristic.AttrClient, Reason: heuristic.ReasonMethodNotFound,
		}
		return retryableErr("method not found")
	})
	ctx := methodCtx("eth_blockNumber", eps)
	ctx.ServiceID = "kava"
	_ = MethodBlocks(store, reg, nil, newFlags("method_blocks"), nil, events)(notFound).HandleRelay(ctx)

	for _, m := range []string{"eth_blockNumber", "eth_call", "eth_getLogs"} {
		if !store.Blocked("kava", host, m) {
			t.Fatalf("%s should be blocked on the host after one -32601 on eth_blockNumber", m)
		}
	}
	if store.Blocked("kava", host, "status") {
		t.Fatal("a CometBFT method is not in the EVM family and must stay open")
	}
	events.mu.Lock()
	got := append([]string(nil), events.events...)
	events.mu.Unlock()
	if !slices.Contains(got, "mark:eth_blockNumber") || !slices.Contains(got, "family:eth_blockNumber") {
		t.Fatalf("events = %v, want a mark and one family event", got)
	}

	// Supplier-attributed on another host: that method only.
	timeout := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = testEndpoints(2)[1]
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			MethodBlocking: true, Attribution: heuristic.AttrSupplier, Reason: "transport_timeout",
		}
		return retryableErr("timeout")
	})
	ctx = methodCtx("eth_call", testEndpoints(2))
	ctx.ServiceID = "kava"
	_ = MethodBlocks(store, reg, nil, newFlags("method_blocks"), nil, events)(timeout).HandleRelay(ctx)
	other := testEndpoints(2)[1].Domain()
	if !store.Blocked("kava", other, "eth_call") || store.Blocked("kava", other, "eth_getLogs") {
		t.Fatal("a timeout marks the one method, never the family")
	}
}

// A -32601 retry exists to reach another host. When the mark leaves no host
// open for the method, the verdict is dropped and the answer delivered; with
// another host still open, the retry stands.
func TestMethodBlocks_MethodNotFoundRetryNeedsAnOpenHost(t *testing.T) {
	reg := qos.NewRegistry()
	if err := reg.Register("kava", familyPlugin{}); err != nil {
		t.Fatal(err)
	}
	notFound := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = ctx.Endpoints[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			MethodBlocking: true, ShouldRetry: true, Attribution: heuristic.AttrClient, Reason: heuristic.ReasonMethodNotFound,
		}
		ctx.Err = retryableErr("method not found")
		return ctx.Err
	})

	ctx := methodCtx("eth_blockNumber", testEndpoints(1))
	ctx.ServiceID = "kava"
	err := MethodBlocks(methodblock.New(), reg, nil, newFlags("method_blocks"), nil, nil)(notFound).HandleRelay(ctx)
	if err != nil || ctx.Err != nil || ctx.HeuristicResult.ShouldRetry {
		t.Fatalf("one host, now blocked: err = %v, ctx.Err = %v, retry = %v; want the answer delivered", err, ctx.Err, ctx.HeuristicResult.ShouldRetry)
	}

	ctx = methodCtx("eth_blockNumber", testEndpoints(2))
	ctx.ServiceID = "kava"
	err = MethodBlocks(methodblock.New(), reg, nil, newFlags("method_blocks"), nil, nil)(notFound).HandleRelay(ctx)
	if err == nil || !ctx.HeuristicResult.ShouldRetry {
		t.Fatalf("a second host is open: err = %v, retry = %v; want the retry to stand", err, ctx.HeuristicResult.ShouldRetry)
	}
}

// resolvingProvider is an endpoint provider that also says which host a face
// is dialed from: the REST face of eps[0] lives on another host than the
// address names.
type resolvingProvider struct{ eps domain.EndpointAddrList }

func (r resolvingProvider) AvailableEndpoints(context.Context, domain.ServiceID, domain.RPCType) (domain.EndpointAddrList, error) {
	return r.eps, nil
}

func (r resolvingProvider) EndpointURLFor(ep domain.EndpointAddr, rt domain.RPCType) (string, bool) {
	if ep == r.eps[0] && rt == domain.RPCTypeREST {
		return "https://rest.other.example:8443/v1", true
	}
	return "", false
}

// A mark is kept against the host the face is dialed from, and the pre-relay
// filter looks it up the same way, so a REST refusal on a split-host
// operator blocks the REST host and leaves the address's JSON-RPC host alone.
func TestMethodBlocks_HostFollowsTheDialedURL(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	provider := resolvingProvider{eps: eps}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true, Attribution: heuristic.AttrSupplier}
		return retryableErr("timeout")
	})
	ctx := methodCtx("/cosmos/bank/v1beta1/supply", eps)
	ctx.RPCType = domain.RPCTypeREST
	_ = MethodBlocks(store, registryWith(t), provider, newFlags("method_blocks"), nil, nil)(inner).HandleRelay(ctx)

	if !store.Blocked("eth", "rest.other.example", "/cosmos/bank/v1beta1/supply") {
		t.Fatal("the mark must be kept against the dialed REST host")
	}
	if store.Blocked("eth", eps[0].Domain(), "/cosmos/bank/v1beta1/supply") {
		t.Fatal("the address's own host must not carry a REST mark")
	}

	// The filter resolves the same way: eps[0] is now excluded for that
	// method on the REST face, and stays in for json_rpc.
	ctx = methodCtx("/cosmos/bank/v1beta1/supply", eps)
	ctx.RPCType = domain.RPCTypeREST
	var seen domain.EndpointAddrList
	probe := relay.HandlerFunc(func(ctx *relay.Context) error { seen = ctx.Endpoints; return nil })
	_ = MethodBlocks(store, registryWith(t), provider, newFlags("method_blocks"), nil, nil)(probe).HandleRelay(ctx)
	if len(seen) != 1 || seen[0] != eps[1] {
		t.Fatalf("REST candidates = %v, want only %v", seen, eps[1])
	}
	ctx = methodCtx("/cosmos/bank/v1beta1/supply", eps)
	ctx.RPCType = domain.RPCTypeJSONRPC
	_ = MethodBlocks(store, registryWith(t), provider, newFlags("method_blocks"), nil, nil)(probe).HandleRelay(ctx)
	if len(seen) != 2 {
		t.Fatalf("json_rpc candidates = %v, want both", seen)
	}
}
