package reputation

import (
	"context"
	"strconv"
	"sync"
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

func TestMemoize_ReturnsSameKeysAsUnderlyingFn(t *testing.T) {
	raw := keyFnFor(KeyPerURL)
	memo := memoize(raw)

	addrs := []domain.EndpointAddr{
		"pokt1supplier-https://eth.example.com/v1",
		"pokt1other-https://eth.example.com/v1",
		"malformed",
		"",
	}
	for _, addr := range addrs {
		for _, rpcType := range []domain.RPCType{domain.RPCTypeJSONRPC, domain.RPCTypeWebSocket} {
			want := raw(addr, rpcType)
			// Twice: the second call is the one served from the memo.
			for i := 0; i < 2; i++ {
				if got := memo(addr, rpcType); got != want {
					t.Errorf("memo(%q, %v) call %d = %q, want %q", addr, rpcType, i, got, want)
				}
			}
		}
	}
}

func TestMemoize_FormatsEachKeyOnce(t *testing.T) {
	calls := 0
	memo := memoize(func(ep domain.EndpointAddr, rpcType domain.RPCType) string {
		calls++
		return string(ep) + "|" + string(rpcType)
	})

	// Selection scores the whole pool on every relay, so the same few keys are
	// asked for over and over.
	for relay := 0; relay < 100; relay++ {
		for _, addr := range []domain.EndpointAddr{"a-http://x", "b-http://y", "c-http://z"} {
			memo(addr, domain.RPCTypeJSONRPC)
		}
	}

	if calls != 3 {
		t.Errorf("underlying fn called %d times for 3 distinct keys over 100 relays, want 3", calls)
	}
}

// The memo must not distinguish keys only by endpoint: the RPC type is part of
// the identity a score is stored under.
func TestMemoize_KeyIncludesRPCType(t *testing.T) {
	memo := memoize(keyFnFor(KeyPerURL))

	const addr = domain.EndpointAddr("pokt1a-https://eth.example.com")
	if a, b := memo(addr, domain.RPCTypeJSONRPC), memo(addr, domain.RPCTypeWebSocket); a == b {
		t.Errorf("different RPC types produced the same key: %q", a)
	}
}

func TestMemoize_DropsCacheOnceItOutgrowsTheCap(t *testing.T) {
	raw := keyFnFor(KeyPerEndpoint)
	calls := 0
	memo := memoize(func(ep domain.EndpointAddr, rpcType domain.RPCType) string {
		calls++
		return raw(ep, rpcType)
	})

	const hot = domain.EndpointAddr("hot-http://hot.example.com")
	memo(hot, domain.RPCTypeJSONRPC)

	for i := 0; i <= maxKeyCacheEntries; i++ {
		memo(domain.EndpointAddr("addr-"+strconv.Itoa(i)), domain.RPCTypeJSONRPC)
	}

	// The hot key survived as a value, but its memo entry did not: the cap was
	// passed, so the whole map was dropped and it costs one re-format.
	before := calls
	if got, want := memo(hot, domain.RPCTypeJSONRPC), raw(hot, domain.RPCTypeJSONRPC); got != want {
		t.Errorf("key after cache drop = %q, want %q", got, want)
	}
	if calls == before {
		t.Error("expected the dropped entry to be recomputed")
	}
}

func TestMemoize_ConcurrentUse(t *testing.T) {
	memo := memoize(keyFnFor(KeyPerURL))
	addrs := []domain.EndpointAddr{"a-http://x", "b-http://y", "c-http://z", "d-http://w"}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				addr := addrs[j%len(addrs)]
				if got := memo(addr, domain.RPCTypeJSONRPC); got == "" {
					t.Errorf("empty key for %q", addr)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// BenchmarkKeyFn measures what selection pays per candidate endpoint per relay.
func BenchmarkKeyFn(b *testing.B) {
	const addr = domain.EndpointAddr("pokt1supplier-https://eth.example.com/v1")
	raw := keyFnFor(KeyPerURL)
	memo := memoize(raw)

	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = raw(addr, domain.RPCTypeJSONRPC)
		}
	})
	b.Run("memoized", func(b *testing.B) {
		_ = memo(addr, domain.RPCTypeJSONRPC)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = memo(addr, domain.RPCTypeJSONRPC)
		}
	})
}
