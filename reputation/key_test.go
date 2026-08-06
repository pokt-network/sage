package reputation

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
)

func TestKeyFnFor_Granularities(t *testing.T) {
	ep := domain.EndpointAddr("pokt1abc-https://rpc-1.example.net:8545/v1")

	tests := []struct {
		granularity string
		want        string
	}{
		{KeyPerURL, "https://rpc-1.example.net:8545/v1|json_rpc"},
		{KeyPerEndpoint, string(ep) + "|json_rpc"},
		{KeyPerDomain, "rpc-1.example.net|json_rpc"},
		{KeyPerSupplier, "pokt1abc|json_rpc"},
		{"", "https://rpc-1.example.net:8545/v1|json_rpc"}, // unset defaults to per-URL
	}
	for _, tt := range tests {
		t.Run(tt.granularity, func(t *testing.T) {
			if got := keyFnFor(tt.granularity)(ep, domain.RPCTypeJSONRPC); got != tt.want {
				t.Errorf("key = %q, want %q", got, tt.want)
			}
		})
	}
}

// The point of per-URL: one physical backend behind several staked supplier
// addresses is scored once, not once per registration.
func TestKeyPerURL_SharedBackendSharesAScore(t *testing.T) {
	a := domain.EndpointAddr("pokt1aaa-https://rpc.example.net")
	b := domain.EndpointAddr("pokt1bbb-https://rpc.example.net")
	other := domain.EndpointAddr("pokt1aaa-https://rpc-2.example.net")

	key := func(ep domain.EndpointAddr) string { return keyFnFor(KeyPerURL)(ep, domain.RPCTypeJSONRPC) }
	if key(a) != key(b) {
		t.Errorf("suppliers on the same URL should share a key: %q vs %q", key(a), key(b))
	}
	// ...but distinct URLs stay separate, unlike per-domain.
	if key(a) == key(other) {
		t.Errorf("distinct URLs must not share a key: both %q", key(a))
	}
}

// A malformed address degrades to per-endpoint for that address alone. Falling
// back to the empty key would score every malformed address as one host.
func TestKeyFn_MalformedAddressDegradesToItself(t *testing.T) {
	bad := domain.EndpointAddr("noseparatorhere")
	for _, g := range []string{KeyPerURL, KeyPerDomain, KeyPerSupplier} {
		if got := keyFnFor(g)(bad, domain.RPCTypeJSONRPC); got != string(bad)+"|json_rpc" {
			t.Errorf("%s: key = %q, want the address itself", g, got)
		}
	}
}

func TestValidKeyGranularity(t *testing.T) {
	for _, ok := range []string{"", KeyPerURL, KeyPerEndpoint, KeyPerDomain, KeyPerSupplier} {
		if !ValidKeyGranularity(ok) {
			t.Errorf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"per_url", "url", "PER-URL", "nonsense"} {
		if ValidKeyGranularity(bad) {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

// End-to-end through the service: a failure recorded against one supplier is
// visible to its co-tenant on the same backend, because the backend is what is
// being scored.
func TestService_PerURLGranularitySharesPenalties(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, ServiceConfig{KeyGranularity: KeyPerURL})
	svc.Start()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	a := domain.EndpointAddr("pokt1aaa-https://rpc.example.net")
	b := domain.EndpointAddr("pokt1bbb-https://rpc.example.net")

	if err := svc.RecordSignal(ctx, svcID, a, domain.RPCTypeJSONRPC, NewMinorErrorSignal("retry", 0)); err != nil {
		t.Fatal(err)
	}

	scoreA, _ := svc.GetScore(ctx, svcID, a, domain.RPCTypeJSONRPC)
	scoreB, _ := svc.GetScore(ctx, svcID, b, domain.RPCTypeJSONRPC)
	if scoreA != scoreB {
		t.Errorf("co-tenants on one backend should share a score: %v vs %v", scoreA, scoreB)
	}
	if scoreA != 97 {
		t.Errorf("score = %v, want 97 (100 - 3)", scoreA)
	}
}

func TestService_PerEndpointGranularityKeepsPenaltiesSeparate(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, ServiceConfig{KeyGranularity: KeyPerEndpoint})
	svc.Start()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	a := domain.EndpointAddr("pokt1aaa-https://rpc.example.net")
	b := domain.EndpointAddr("pokt1bbb-https://rpc.example.net")

	if err := svc.RecordSignal(ctx, svcID, a, domain.RPCTypeJSONRPC, NewMinorErrorSignal("retry", 0)); err != nil {
		t.Fatal(err)
	}

	scoreA, _ := svc.GetScore(ctx, svcID, a, domain.RPCTypeJSONRPC)
	scoreB, _ := svc.GetScore(ctx, svcID, b, domain.RPCTypeJSONRPC)
	if scoreA == scoreB {
		t.Errorf("per-endpoint scores should be independent, both %v", scoreA)
	}
}

// The RPC type is part of the key at every granularity. A Shannon supplier
// stakes one service for several RPC types, and the relay miner routes each to
// a different backend — a dead WebSocket backend says nothing about REST. The
// coarser the granularity, the wider the damage of blending them: at per-URL,
// one key would otherwise cover every transport of every supplier on the URL.
func TestKeyFn_RPCTypeAlwaysSeparatesScores(t *testing.T) {
	ep := domain.EndpointAddr("pokt1abc-https://rpc.example.net")
	for _, g := range []string{KeyPerURL, KeyPerEndpoint, KeyPerDomain, KeyPerSupplier} {
		key := keyFnFor(g)
		if key(ep, domain.RPCTypeWebSocket) == key(ep, domain.RPCTypeREST) {
			t.Errorf("%s: websocket and rest must not share a key", g)
		}
	}
}

func TestService_WebSocketFailureDoesNotPenalizeREST(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, ServiceConfig{})
	svc.Start()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("pnf-pocket-beta")
	ep := domain.EndpointAddr("pokt1abc-https://rm.example.net")

	for i := 0; i < 5; i++ {
		if err := svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeWebSocket, NewCriticalErrorSignal("ws dead", 0)); err != nil {
			t.Fatal(err)
		}
	}

	ws, _ := svc.GetScore(ctx, svcID, ep, domain.RPCTypeWebSocket)
	rest, _ := svc.GetScore(ctx, svcID, ep, domain.RPCTypeREST)
	if ws >= rest {
		t.Errorf("websocket score %v should be below the untouched REST score %v", ws, rest)
	}
	if rest != 100 {
		t.Errorf("REST score = %v, want the untouched initial 100", rest)
	}
}

// Resetting an endpoint means the endpoint, not one of the protocols it serves.
func TestService_ResetScoreClearsEveryRPCType(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, ServiceConfig{})
	svc.Start()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	ep := domain.EndpointAddr("pokt1abc-https://rm.example.net")

	for _, rt := range domain.AllRPCTypes() {
		_ = svc.RecordSignal(ctx, svcID, ep, rt, NewCriticalErrorSignal("bad", 0))
	}
	if err := svc.ResetScore(ctx, svcID, ep); err != nil {
		t.Fatal(err)
	}
	for _, rt := range domain.AllRPCTypes() {
		if score, _ := svc.GetScore(ctx, svcID, ep, rt); score != 100 {
			t.Errorf("%s score = %v after reset, want 100", rt, score)
		}
	}
}
