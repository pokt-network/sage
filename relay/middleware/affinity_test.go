package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// makeRequest builds a minimal *http.Request with a given remote address.
func makeRequest(remoteAddr string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = remoteAddr
	return req
}

func TestAffinity_WriteSetAffinity(t *testing.T) {
	eps := testEndpoints(3)
	// Handler simulates a successful "eth_sendRawTransaction" (write) call.
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		if ctx.Endpoint == "" && len(ctx.Endpoints) > 0 {
			ctx.Endpoint = ctx.Endpoints[0]
		}
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := SupplierAffinity(newFlags("supplier_affinity"), 1*time.Minute)
	h := mw(inner)

	ctx := baseContext()
	ctx.HTTPRequest = makeRequest("192.0.2.1:54321")
	ctx.Endpoints = eps
	ctx.Payloads = []domain.Payload{
		domain.NewPayload(nil, domain.RPCTypeJSONRPC, "eth_sendRawTransaction"),
	}

	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	chosenEndpoint := ctx.Endpoint
	if chosenEndpoint == "" {
		t.Fatal("expected endpoint to be selected")
	}

	// Second request (read) should prefer the same endpoint.
	ctx2 := baseContext()
	ctx2.HTTPRequest = makeRequest("192.0.2.1:54322") // same IP, different port
	ctx2.Endpoints = eps
	ctx2.Payloads = []domain.Payload{
		domain.NewPayload(nil, domain.RPCTypeJSONRPC, "eth_getTransactionReceipt"),
	}

	var innerSawEndpoints domain.EndpointAddrList
	snoopInner := relay.HandlerFunc(func(ctx *relay.Context) error {
		innerSawEndpoints = append(innerSawEndpoints[:0:0], ctx.Endpoints...)
		ctx.Endpoint = ctx.Endpoints[0]
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	h2 := mw(snoopInner)
	if err := h2.HandleRelay(ctx2); err != nil {
		t.Fatalf("unexpected error on second request: %v", err)
	}

	// The affinity endpoint should be at the front of the list passed to inner.
	if len(innerSawEndpoints) == 0 {
		t.Fatal("inner handler did not receive any endpoints")
	}
	if innerSawEndpoints[0] != chosenEndpoint {
		t.Errorf("expected affinity endpoint %q at front, got %q", chosenEndpoint, innerSawEndpoints[0])
	}
}

func TestAffinity_SubsequentReadUsesPreferredEndpoint(t *testing.T) {
	eps := testEndpoints(4)
	preferred := eps[2] // pick the third endpoint as the "stored" affinity

	// Use two separate SupplierAffinity middlewares sharing the same store
	// is not possible with the current closure design; instead we do two
	// calls through the same middleware instance.
	mw := SupplierAffinity(newFlags("supplier_affinity"), 1*time.Minute)

	// First request: write with preferred endpoint pre-selected.
	ctx1 := baseContext()
	ctx1.HTTPRequest = makeRequest("10.0.0.1:1234")
	ctx1.Endpoints = eps
	ctx1.Endpoint = preferred // pre-select so inner records it
	ctx1.Payloads = []domain.Payload{
		domain.NewPayload(nil, domain.RPCTypeJSONRPC, "eth_sendTransaction"),
	}
	// Simulate inner always setting the endpoint that was pre-selected.
	innerKeepsEndpoint := relay.HandlerFunc(func(ctx *relay.Context) error {
		// keep ctx.Endpoint as-is (already preferred)
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	h2 := mw(innerKeepsEndpoint)
	if err := h2.HandleRelay(ctx1); err != nil {
		t.Fatal(err)
	}

	// Second request: read — should see preferred endpoint at front.
	var frontEndpoint domain.EndpointAddr
	readInner := relay.HandlerFunc(func(ctx *relay.Context) error {
		if len(ctx.Endpoints) > 0 {
			frontEndpoint = ctx.Endpoints[0]
		}
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	h3 := mw(readInner)

	ctx2 := baseContext()
	ctx2.HTTPRequest = makeRequest("10.0.0.1:5678") // same IP
	ctx2.Endpoints = eps
	ctx2.Payloads = []domain.Payload{
		domain.NewPayload(nil, domain.RPCTypeJSONRPC, "eth_call"),
	}
	if err := h3.HandleRelay(ctx2); err != nil {
		t.Fatal(err)
	}

	if frontEndpoint != preferred {
		t.Errorf("expected preferred endpoint %q at front, got %q", preferred, frontEndpoint)
	}
}

func TestAffinity_AffinityExpires(t *testing.T) {
	eps := testEndpoints(3)
	preferred := eps[1]

	innerKeepsEndpoint := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	// Very short TTL.
	mw := SupplierAffinity(newFlags("supplier_affinity"), 10*time.Millisecond)
	h := mw(innerKeepsEndpoint)

	// Establish affinity via a write.
	ctx1 := baseContext()
	ctx1.HTTPRequest = makeRequest("172.16.0.1:9999")
	ctx1.Endpoints = eps
	ctx1.Endpoint = preferred
	ctx1.Payloads = []domain.Payload{
		domain.NewPayload(nil, domain.RPCTypeJSONRPC, "eth_sendRawTransaction"),
	}
	if err := h.HandleRelay(ctx1); err != nil {
		t.Fatal(err)
	}

	// Wait for TTL to expire.
	time.Sleep(20 * time.Millisecond)

	// Read request: affinity should be gone; any endpoint at front is fine.
	var frontEndpoint domain.EndpointAddr
	readInner := relay.HandlerFunc(func(ctx *relay.Context) error {
		if len(ctx.Endpoints) > 0 {
			frontEndpoint = ctx.Endpoints[0]
		}
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	h2 := mw(readInner)

	ctx2 := baseContext()
	ctx2.HTTPRequest = makeRequest("172.16.0.1:8888")
	ctx2.Endpoints = eps
	// Move a different endpoint to the front to verify affinity isn't applied.
	ctx2.Endpoints = domain.EndpointAddrList{eps[2], eps[0], eps[1]}
	ctx2.Payloads = []domain.Payload{
		domain.NewPayload(nil, domain.RPCTypeJSONRPC, "eth_getBalance"),
	}
	if err := h2.HandleRelay(ctx2); err != nil {
		t.Fatal(err)
	}

	// After expiry, the expired preferred endpoint should NOT be prioritized.
	// The front should remain eps[2] (no reordering).
	if frontEndpoint == preferred {
		t.Errorf("expected affinity to expire, but preferred endpoint %q was still at front", preferred)
	}
}

func TestAffinity_FlagDisabled_PassesThrough(t *testing.T) {
	eps := testEndpoints(3)
	preferred := eps[2]

	// Establish "affinity" — should have no effect.
	innerKeepsEndpoint := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	mw := SupplierAffinity(newFlags( /* no "supplier_affinity" */ ), 1*time.Minute)
	h := mw(innerKeepsEndpoint)

	ctx1 := baseContext()
	ctx1.HTTPRequest = makeRequest("192.168.1.100:11111")
	ctx1.Endpoints = eps
	ctx1.Endpoint = preferred
	ctx1.Payloads = []domain.Payload{
		domain.NewPayload(nil, domain.RPCTypeJSONRPC, "eth_sendRawTransaction"),
	}
	if err := h.HandleRelay(ctx1); err != nil {
		t.Fatal(err)
	}

	// Second read: endpoints should NOT be reordered.
	var frontEndpoint domain.EndpointAddr
	readInner := relay.HandlerFunc(func(ctx *relay.Context) error {
		if len(ctx.Endpoints) > 0 {
			frontEndpoint = ctx.Endpoints[0]
		}
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	h2 := mw(readInner)

	ctx2 := baseContext()
	ctx2.HTTPRequest = makeRequest("192.168.1.100:22222")
	ctx2.Endpoints = eps // eps[0] at front
	ctx2.Payloads = []domain.Payload{
		domain.NewPayload(nil, domain.RPCTypeJSONRPC, "eth_call"),
	}
	if err := h2.HandleRelay(ctx2); err != nil {
		t.Fatal(err)
	}

	// With flag disabled no affinity is applied, so the original order is kept.
	if frontEndpoint != eps[0] {
		t.Errorf("expected original first endpoint %q, got %q", eps[0], frontEndpoint)
	}
}
