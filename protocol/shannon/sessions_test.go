package shannon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	sdk "github.com/pokt-network/shannon-sdk"

	"github.com/pokt-network/sage/domain"
)

// stubFullNode satisfies fullNodeIface for testing sessionManager.
type stubFullNode struct {
	session   *sessiontypes.Session
	height    int64
	sessErr   error
	heightErr error
}

func (m *stubFullNode) GetSession(_ context.Context, _ string, _ string) (*sessiontypes.Session, error) {
	if m.sessErr != nil {
		return nil, m.sessErr
	}
	return m.session, nil
}

func (m *stubFullNode) GetApp(_ context.Context, _ string) (*apptypes.Application, error) {
	return &apptypes.Application{}, nil
}

func (m *stubFullNode) GetCurrentBlockHeight(_ context.Context) (int64, error) {
	if m.heightErr != nil {
		return 0, m.heightErr
	}
	return m.height, nil
}

func (m *stubFullNode) GetSharedParams(_ context.Context) (*sharedtypes.Params, error) {
	return &sharedtypes.Params{}, nil
}

func (m *stubFullNode) ValidateRelayResponse(_ string, _ []byte) (*servicetypes.RelayResponse, error) {
	return &servicetypes.RelayResponse{}, nil
}

func (m *stubFullNode) AccountClient() *sdk.AccountClient {
	return nil
}

// buildTestSession creates a minimal session with one supplier endpoint.
func buildTestSession(sessionID string, supplierAddr string, url string) *sessiontypes.Session {
	return &sessiontypes.Session{
		SessionId: sessionID,
		Header: &sessiontypes.SessionHeader{
			SessionId:               sessionID,
			ServiceId:               "eth",
			SessionStartBlockHeight: 100,
			SessionEndBlockHeight:   110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
		Suppliers: []*sharedtypes.Supplier{
			{
				OperatorAddress: supplierAddr,
				Services: []*sharedtypes.SupplierServiceConfig{
					{
						ServiceId: "eth",
						Endpoints: []*sharedtypes.SupplierEndpoint{
							{
								Url:     url,
								RpcType: sharedtypes.RPCType_JSON_RPC,
							},
						},
					},
				},
			},
		},
	}
}

func TestSessionManager_GetEndpoints(t *testing.T) {
	session := buildTestSession("session1", "pokt1supplier", "https://relay.example.com")

	fn := &stubFullNode{session: session, height: 200}
	sm := newSessionManager(
		fn,
		map[domain.ServiceID]struct{}{"eth": {}},
		newTestLogger(),
	)
	// Inject mock by calling getOrCreateEndpoints directly.
	endpoints := sm.getOrCreateEndpoints(session)

	if len(endpoints) == 0 {
		t.Fatal("expected at least one endpoint")
	}

	for _, ep := range endpoints {
		if ep.Supplier() != "pokt1supplier" {
			t.Errorf("supplier = %q, want %q", ep.Supplier(), "pokt1supplier")
		}
		url, err := ep.GetURL(domain.RPCTypeJSONRPC)
		if err != nil {
			t.Fatalf("GetURL: %v", err)
		}
		if url != "https://relay.example.com" {
			t.Errorf("url = %q, want %q", url, "https://relay.example.com")
		}
	}

}

func TestSessionManager_EndpointCaching(t *testing.T) {
	session := buildTestSession("session-cache", "pokt1supplier", "https://example.com")

	sm := newSessionManager(
		&stubFullNode{height: 200},
		map[domain.ServiceID]struct{}{"eth": {}},
		newTestLogger(),
	)

	// First call populates cache.
	ep1 := sm.getOrCreateEndpoints(session)
	// Second call should return the same map from cache.
	ep2 := sm.getOrCreateEndpoints(session)

	if len(ep1) != len(ep2) {
		t.Errorf("cache mismatch: first call got %d, second got %d", len(ep1), len(ep2))
	}

	// Verify it's actually the same underlying map via pointer.
	for addr := range ep1 {
		if ep1[addr] != ep2[addr] {
			t.Error("expected same pointer from cache")
		}
	}
}

func TestSessionEndpoints_RPCTypeSupport(t *testing.T) {
	// RPC-type filtering happens inline in Protocol.AvailableEndpoints via
	// endpoint.GetURL; assert the underlying support check here.
	session := buildTestSession("session-filter", "pokt1supplier", "https://example.com")

	sm := newSessionManager(
		&stubFullNode{height: 200},
		map[domain.ServiceID]struct{}{"eth": {}},
		newTestLogger(),
	)

	endpoints := sm.getOrCreateEndpoints(session)
	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpoints))
	}
	for _, ep := range endpoints {
		// JSON-RPC should be supported.
		if _, err := ep.GetURL(domain.RPCTypeJSONRPC); err != nil {
			t.Errorf("expected JSON-RPC support, got error: %v", err)
		}
		// REST should not be supported.
		if _, err := ep.GetURL(domain.RPCTypeREST); err == nil {
			t.Error("expected REST to be unsupported")
		}
	}
}

func TestSessionManager_LatestBlockHeight_RetainsLastGoodOnPollError(t *testing.T) {
	fn := &stubFullNode{
		session: buildTestSession("session-1", "pokt1supplier", "https://relay.example.com"),
		height:  50,
	}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())

	if got := sm.LatestBlockHeight(); got != 0 {
		t.Errorf("height before first poll = %d, want 0", got)
	}

	sm.pollBlockHeight(context.Background())
	if got := sm.LatestBlockHeight(); got != 50 {
		t.Fatalf("height after poll = %d, want 50", got)
	}

	// A failing poll must leave the last good height in place: zeroing it would
	// read as "not yet polled" to every bridge at once.
	fn.heightErr = errors.New("full node unreachable")
	sm.pollBlockHeight(context.Background())
	if got := sm.LatestBlockHeight(); got != 50 {
		t.Errorf("height after failed poll = %d, want 50 retained", got)
	}
}

func TestSessionManager_ConfiguredServices(t *testing.T) {
	services := map[domain.ServiceID]struct{}{
		"eth":  {},
		"poly": {},
	}
	sm := newSessionManager(&stubFullNode{height: 200}, services, newTestLogger())
	got := sm.ConfiguredServices()
	if len(got) != 2 {
		t.Errorf("ConfiguredServices() = %d, want 2", len(got))
	}
	if _, ok := got["eth"]; !ok {
		t.Error("expected 'eth' in configured services")
	}
}

// sessionAtHeight is buildTestSession with a chosen session end height, so a
// test can roll sessions forward the way the chain does.
func sessionAtHeight(sessionID string, end int64) *sessiontypes.Session {
	s := buildTestSession(sessionID, "pokt1supplier", "https://relay.example.com")
	s.Header.SessionStartBlockHeight = end - 10
	s.Header.SessionEndBlockHeight = end
	return s
}

func cacheLen(sm *sessionManager) int {
	n := 0
	sm.endpointCache.Range(func(any, any) bool { n++; return true })
	return n
}

// endpointCache is keyed on the session ID, which is a new value every
// rollover, so without eviction it grows for the life of the process: each
// entry holds a whole session's endpoint map, per service, per app. Nothing
// fails — memory just climbs — which is why this needs an explicit test rather
// than showing up in any other one.
func TestSessionManager_EndpointCacheDoesNotGrowWithRollovers(t *testing.T) {
	sm := newSessionManager(
		&stubFullNode{height: 200},
		map[domain.ServiceID]struct{}{"eth": {}},
		newTestLogger(),
	)

	for i := range 50 {
		end := int64(110 + i*10)
		sm.getOrCreateEndpoints(sessionAtHeight(fmt.Sprintf("session-%d", i), end))
	}

	// Current plus previous is the intended residency; anything that grows with
	// the loop count means nothing is being evicted.
	if got := cacheLen(sm); got > 2 {
		t.Errorf("endpointCache holds %d sessions after 50 rollovers, want at most 2 (current + previous)", got)
	}
}

// The previous session must survive: a relay that selected its endpoint just
// before the boundary is still in flight, and the grace period keeps the old
// session briefly valid.
func TestSessionManager_EvictionKeepsPreviousSession(t *testing.T) {
	sm := newSessionManager(
		&stubFullNode{height: 200},
		map[domain.ServiceID]struct{}{"eth": {}},
		newTestLogger(),
	)

	sm.getOrCreateEndpoints(sessionAtHeight("older", 110))
	sm.getOrCreateEndpoints(sessionAtHeight("previous", 120))
	sm.getOrCreateEndpoints(sessionAtHeight("current", 130))

	if _, ok := sm.endpointCache.Load("current"); !ok {
		t.Error("the current session was evicted")
	}
	if _, ok := sm.endpointCache.Load("previous"); !ok {
		t.Error("the previous session was evicted; in-flight relays would miss the cache")
	}
	if _, ok := sm.endpointCache.Load("older"); ok {
		t.Error("a session two rollovers old was retained")
	}
}

// A session that arrives out of order must not evict newer entries.
func TestSessionManager_LateSessionDoesNotEvictNewer(t *testing.T) {
	sm := newSessionManager(
		&stubFullNode{height: 200},
		map[domain.ServiceID]struct{}{"eth": {}},
		newTestLogger(),
	)

	sm.getOrCreateEndpoints(sessionAtHeight("current", 200))
	sm.getOrCreateEndpoints(sessionAtHeight("late", 150))

	if _, ok := sm.endpointCache.Load("current"); !ok {
		t.Error("a late session evicted the current one")
	}
}

// countingFullNode counts GetSession calls and delays each, so a thundering
// herd shows up as a call count > 1.
type countingFullNode struct {
	stubFullNode
	mu    sync.Mutex
	calls int
	delay time.Duration
}

func (m *countingFullNode) GetSession(_ context.Context, _ string, _ string) (*sessiontypes.Session, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	time.Sleep(m.delay)
	return m.session, nil
}

// At a session boundary every concurrent relay for a service sees the cached
// session as expired and refreshes. Because num_blocks_per_session aligns all
// services to one boundary, this is a fleet-wide stampede of full-node
// GetSession calls that overruns the node and hangs relays to the 10s timeout.
// The refresh must coalesce: one GetSession per (service, app), its result
// shared with everyone waiting.
func TestGetSession_CoalescesConcurrentRefresh(t *testing.T) {
	newSession := &sessiontypes.Session{
		SessionId: "s2",
		Header:    &sessiontypes.SessionHeader{SessionId: "s2", ServiceId: "eth", SessionEndBlockHeight: 200},
	}
	fn := &countingFullNode{stubFullNode: stubFullNode{session: newSession}, delay: 50 * time.Millisecond}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())

	// Seed an expired session and a height past its end.
	expired := &sessiontypes.Session{SessionId: "s1", Header: &sessiontypes.SessionHeader{SessionId: "s1", ServiceId: "eth", SessionEndBlockHeight: 100}}
	sm.sessionCache.Store(sessionCacheKey("eth", "pokt1app"), expired)
	sm.latestBlockHeight.Store(150)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := sm.getSession(context.Background(), "eth", "pokt1app")
			if err != nil || s.SessionId != "s2" {
				t.Errorf("expected the refreshed session s2, got %v err=%v", s, err)
			}
		}()
	}
	wg.Wait()

	fn.mu.Lock()
	defer fn.mu.Unlock()
	if fn.calls > 2 {
		t.Fatalf("GetSession called %d times for 50 concurrent refreshes; the herd is not coalesced", fn.calls)
	}
}

// Within the protocol grace period (sessionEnd .. sessionEnd+grace) relays for
// the just-ended session are still valid, so getSession keeps serving the
// cached session and refreshes the next one in the BACKGROUND — no relay
// blocks at the boundary. SAGE previously expired at sessionEnd exactly, a
// grace-window too early, forcing a synchronous refresh.
func TestGetSession_ServesThroughGraceAndRefreshesInBackground(t *testing.T) {
	newSession := &sessiontypes.Session{SessionId: "s2",
		Header: &sessiontypes.SessionHeader{SessionId: "s2", ServiceId: "eth", SessionEndBlockHeight: 200}}
	fn := &countingFullNode{stubFullNode: stubFullNode{session: newSession}, delay: 20 * time.Millisecond}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	sm.SetGraceBlocks(10)

	expired := &sessiontypes.Session{SessionId: "s1",
		Header: &sessiontypes.SessionHeader{SessionId: "s1", ServiceId: "eth", SessionEndBlockHeight: 100}}
	sm.sessionCache.Store(sessionCacheKey("eth", "pokt1app"), expired)
	sm.latestBlockHeight.Store(105) // 100 < 105 < 100+10: in grace

	// The call returns the CACHED (still-valid) session immediately, without
	// waiting on a fetch.
	got, err := sm.getSession(context.Background(), "eth", "pokt1app")
	if err != nil || got.SessionId != "s1" {
		t.Fatalf("in grace, expected the cached session s1 served without blocking, got %v err=%v", got, err)
	}

	// A background refresh replaces the cache with the new session.
	deadline := time.After(2 * time.Second)
	for {
		if v, ok := sm.sessionCache.Load(sessionCacheKey("eth", "pokt1app")); ok && v.(*sessiontypes.Session).SessionId == "s2" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("background refresh never cached the new session s2")
		case <-time.After(10 * time.Millisecond):
		}
	}
	fn.mu.Lock()
	calls := fn.calls
	fn.mu.Unlock()
	if calls == 0 {
		t.Fatal("expected a background GetSession during grace, got none")
	}
}

// Past the grace period the session is truly expired and getSession refreshes
// synchronously.
func TestGetSession_PastGraceRefreshesSynchronously(t *testing.T) {
	newSession := &sessiontypes.Session{SessionId: "s2",
		Header: &sessiontypes.SessionHeader{SessionId: "s2", ServiceId: "eth", SessionEndBlockHeight: 200}}
	fn := &countingFullNode{stubFullNode: stubFullNode{session: newSession}}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	sm.SetGraceBlocks(10)

	expired := &sessiontypes.Session{SessionId: "s1",
		Header: &sessiontypes.SessionHeader{SessionId: "s1", ServiceId: "eth", SessionEndBlockHeight: 100}}
	sm.sessionCache.Store(sessionCacheKey("eth", "pokt1app"), expired)
	sm.latestBlockHeight.Store(120) // 120 > 100+10: past grace

	got, err := sm.getSession(context.Background(), "eth", "pokt1app")
	if err != nil || got.SessionId != "s2" {
		t.Fatalf("past grace, expected a synchronous refresh to s2, got %v err=%v", got, err)
	}
}

// At the session's exact end block the session is still the current one — the
// next session does not exist on chain yet — so getSession serves it with no
// refresh. The switch happens at end+1, when the new session is queryable.
func TestGetSession_AtEndBlockServesCurrentNoRefresh(t *testing.T) {
	fn := &countingFullNode{stubFullNode: stubFullNode{session: &sessiontypes.Session{SessionId: "s2",
		Header: &sessiontypes.SessionHeader{SessionId: "s2", ServiceId: "eth", SessionEndBlockHeight: 200}}}}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	sm.SetGraceBlocks(10)
	cur := &sessiontypes.Session{SessionId: "s1",
		Header: &sessiontypes.SessionHeader{SessionId: "s1", ServiceId: "eth", SessionEndBlockHeight: 100}}
	sm.sessionCache.Store(sessionCacheKey("eth", "pokt1app"), cur)
	sm.latestBlockHeight.Store(100) // exactly at end

	got, err := sm.getSession(context.Background(), "eth", "pokt1app")
	if err != nil || got.SessionId != "s1" {
		t.Fatalf("at end block expected the current session s1, got %v err=%v", got, err)
	}
	time.Sleep(60 * time.Millisecond)
	fn.mu.Lock()
	calls := fn.calls
	fn.mu.Unlock()
	if calls != 0 {
		t.Fatalf("no refresh should fire at the end block; got %d GetSession calls", calls)
	}
}
