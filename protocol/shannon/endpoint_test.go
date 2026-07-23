package shannon

import (
	"testing"

	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/domain"
)

func TestEndpoint_GetURL(t *testing.T) {
	ep := &endpoint{
		supplierAddr: "pokt1abc",
		urls: map[domain.RPCType]string{
			domain.RPCTypeJSONRPC:   "https://example.com/jsonrpc",
			domain.RPCTypeWebSocket: "wss://example.com/ws",
		},
		session: &sessiontypes.Session{},
	}

	url, err := ep.GetURL(domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if url != "https://example.com/jsonrpc" {
		t.Errorf("got %q, want %q", url, "https://example.com/jsonrpc")
	}

	_, err = ep.GetURL(domain.RPCTypeREST)
	if err == nil {
		t.Error("expected error for unsupported RPC type")
	}
}

func TestEndpoint_Addr(t *testing.T) {
	ep := &endpoint{
		supplierAddr: "pokt1abc",
		urls: map[domain.RPCType]string{
			domain.RPCTypeJSONRPC: "https://node.example.com",
		},
		session: &sessiontypes.Session{},
	}

	addr := ep.Addr()
	expected := domain.EndpointAddr("pokt1abc-https://node.example.com")
	if addr != expected {
		t.Errorf("Addr() = %q, want %q", addr, expected)
	}
}

func TestEndpoint_Supplier(t *testing.T) {
	ep := &endpoint{supplierAddr: "pokt1supplier123"}
	if ep.Supplier() != "pokt1supplier123" {
		t.Errorf("Supplier() = %q", ep.Supplier())
	}
}

func TestEndpoint_IsFallback(t *testing.T) {
	ep := &endpoint{isFallback: false}
	if ep.IsFallback() {
		t.Error("expected IsFallback() = false")
	}

	epFallback := &endpoint{isFallback: true}
	if !epFallback.IsFallback() {
		t.Error("expected IsFallback() = true")
	}
}

func TestEndpoint_PublicURL_Empty(t *testing.T) {
	ep := &endpoint{
		supplierAddr: "pokt1abc",
		urls:         map[domain.RPCType]string{},
	}
	if ep.PublicURL() != "" {
		t.Errorf("expected empty PublicURL for endpoint with no URLs, got %q", ep.PublicURL())
	}
}

func TestEndpoint_PublicURL_ReturnsFirst(t *testing.T) {
	ep := &endpoint{
		supplierAddr: "pokt1abc",
		urls: map[domain.RPCType]string{
			domain.RPCTypeJSONRPC: "https://example.com",
		},
	}
	if ep.PublicURL() != "https://example.com" {
		t.Errorf("PublicURL() = %q", ep.PublicURL())
	}
}

// Addr is a reputation key, and it is built from PublicURL. Ranging over the
// URL map returns Go's randomised order, so a supplier staking several
// transports used to get a different address on every process start —
// scattering its history across keys that look like separate endpoints.
func TestPublicURL_IsStableAcrossCalls(t *testing.T) {
	newEP := func() *endpoint {
		return &endpoint{
			supplierAddr: "pokt1supplier",
			urls: map[domain.RPCType]string{
				domain.RPCTypeWebSocket: "wss://rm.example",
				domain.RPCTypeCometBFT:  "https://rm.example",
				domain.RPCTypeGRPC:      "https://rm.example:8082",
				domain.RPCTypeREST:      "https://rm.example",
				domain.RPCTypeJSONRPC:   "https://rm.example:8081",
			},
		}
	}

	want := newEP().PublicURL()
	if want == "" {
		t.Fatal("PublicURL returned nothing")
	}
	// Fresh maps each time: identical content, different internal iteration
	// order. A map-order-dependent implementation fails this quickly.
	for i := 0; i < 50; i++ {
		if got := newEP().PublicURL(); got != want {
			t.Fatalf("PublicURL varies between endpoints with identical URLs: %q then %q", want, got)
		}
		if got := newEP().Addr(); got != domain.EndpointAddr("pokt1supplier-"+want) {
			t.Fatalf("Addr varies: %q", got)
		}
	}
}

// Whatever the preference list says, an endpoint offering only an unlisted
// transport still needs one stable answer rather than a random one.
func TestPublicURL_UnknownRPCTypeIsStillDeterministic(t *testing.T) {
	newEP := func() *endpoint {
		return &endpoint{
			supplierAddr: "pokt1supplier",
			urls: map[domain.RPCType]string{
				domain.RPCType("future-transport"): "https://b.example",
				domain.RPCType("other-transport"):  "https://a.example",
			},
		}
	}

	for i := 0; i < 50; i++ {
		if got := newEP().PublicURL(); got != "https://a.example" {
			t.Fatalf("PublicURL = %q, want the lowest URL deterministically", got)
		}
	}
}
