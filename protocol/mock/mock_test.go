package mock

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func TestMock_AvailableEndpoints_FreshSlicePerCall(t *testing.T) {
	m := New([]domain.ServiceID{"eth"}, 5, 0, "", nil)

	a, err := m.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(a) != 5 {
		t.Fatalf("expected 5 endpoints, got %d", len(a))
	}

	// Mutate the returned slice (as the circuit breaker filter does in place)
	// and verify a second call is unaffected.
	a[0] = "corrupted"
	b, _ := m.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if b[0] == "corrupted" {
		t.Fatal("AvailableEndpoints shares its backing array across calls")
	}
}

func TestMock_AvailableEndpoints_UnknownService(t *testing.T) {
	m := New([]domain.ServiceID{"eth"}, 5, 0, "", nil)
	if _, err := m.AvailableEndpoints(context.Background(), "poly", domain.RPCTypeJSONRPC); err == nil {
		t.Fatal("expected error for unconfigured service")
	}
}

func TestMock_SendRelay_CannedResponse(t *testing.T) {
	m := New([]domain.ServiceID{"eth"}, 1, 0, "", nil)
	payload := domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber")

	resp, err := m.SendRelay(context.Background(), "eth", "pokt1mock000-https://supplier-000.mock.local", payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.HTTPStatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.HTTPStatusCode)
	}
	if string(resp.Body) != defaultResponseBody {
		t.Fatalf("unexpected body: %s", resp.Body)
	}
}

func TestMock_SendRelay_ContextCancelDuringLatency(t *testing.T) {
	m := New([]domain.ServiceID{"eth"}, 1, time.Second, "", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	payload := domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	if _, err := m.SendRelay(ctx, "eth", "ep", payload); err == nil {
		t.Fatal("expected context error during simulated latency")
	}
}

func TestMock_EndpointAddrFormat(t *testing.T) {
	m := New([]domain.ServiceID{"eth"}, 2, 0, "", nil)
	eps, _ := m.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	for _, ep := range eps {
		if ep.Supplier() == "" || ep.Domain() == "" {
			t.Fatalf("endpoint %q must yield supplier and domain", ep)
		}
	}
	if eps[0].Domain() == eps[1].Domain() {
		t.Fatal("endpoints must have distinct domains for circuit breaking")
	}
}
