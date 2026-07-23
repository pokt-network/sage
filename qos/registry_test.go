package qos

import (
	"context"
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// mockPlugin is a minimal Plugin implementation for testing.
type mockPlugin struct{}

func (m *mockPlugin) ParseRequest(_ context.Context, _ *http.Request, _ []byte, _ domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}

func (m *mockPlugin) SelectEndpoints(endpoints domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return endpoints, nil
}

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry()
	p := &mockPlugin{}

	if err := r.Register("eth", p); err != nil {
		t.Fatalf("first Register: %v", err)
	}

	// Duplicate registration should fail.
	if err := r.Register("eth", p); err == nil {
		t.Fatal("expected error on duplicate Register")
	}

	// Different service should succeed.
	if err := r.Register("solana", p); err != nil {
		t.Fatalf("Register different service: %v", err)
	}
}

func TestRegistry_Get(t *testing.T) {
	r := NewRegistry()
	p := &mockPlugin{}
	_ = r.Register("eth", p)

	got := r.Get("eth")
	if got != p {
		t.Fatal("Get returned wrong plugin")
	}

	if r.Get("missing") != nil {
		t.Fatal("Get should return nil for unregistered service")
	}
}

func TestRegistry_Plugins(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("eth", &mockPlugin{})
	_ = r.Register("poly", &mockPlugin{})

	all := r.Plugins()
	if len(all) != 2 {
		t.Fatalf("expected 2 plugins, got %d", len(all))
	}

	// Mutating the returned map should not affect the registry.
	delete(all, "eth")
	if r.Count() != 2 {
		t.Fatal("Plugins() did not return a copy")
	}
}

func TestRegistry_Count(t *testing.T) {
	r := NewRegistry()
	if r.Count() != 0 {
		t.Fatalf("expected 0, got %d", r.Count())
	}
	_ = r.Register("eth", &mockPlugin{})
	if r.Count() != 1 {
		t.Fatalf("expected 1, got %d", r.Count())
	}
}
