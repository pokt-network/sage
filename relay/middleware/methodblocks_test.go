package middleware

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/methodblock"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
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

// A timeout on eth_getLogs marks the host for eth_getLogs and nothing else.
func TestMethodBlocks_TimeoutMarksOnlyThatMethod(t *testing.T) {
	store := methodblock.New()
	eps := testEndpoints(2)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true, Reason: "transport_timeout"}
		return retryableErr("timeout")
	})
	h := MethodBlocks(store, registryWith(t), newFlags("method_blocks"), nil)(inner)
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
	h := MethodBlocks(store, registryWith(t), newFlags("method_blocks"), nil)(inner)

	_ = h.HandleRelay(methodCtx("eth_getLogs", eps))
	if len(seen) != 1 || seen[0] != eps[1] {
		t.Fatalf("eth_getLogs saw %v, want only %v", seen, eps[1])
	}

	_ = h.HandleRelay(methodCtx("eth_call", eps))
	if len(seen) != 2 {
		t.Fatalf("eth_call saw %v, want both hosts", seen)
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
	h := MethodBlocks(store, registryWith(t), newFlags("method_blocks"), events)(inner)

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
	h := MethodBlocks(store, registryWith(t), newFlags("method_blocks"), events)(inner)
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
		h := MethodBlocks(store, registryWith(t), newFlags("method_blocks"), nil)(inner)
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
	h := MethodBlocks(store, qos.NewRegistry(), newFlags("method_blocks"), nil)(inner)
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
	h := MethodBlocks(store, registryWith(t), newFlags(), nil)(inner)
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
		MethodBlocks(store, registryWith(t), newFlags("hedge", "method_blocks"), nil)(inner))

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
