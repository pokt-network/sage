package shannon

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/domain"
)

// prefetchFullNode records how many session fetches are in flight at once and
// how many happened in total, which is what the pacing and concurrency bounds
// are actually about.
type prefetchFullNode struct {
	mockRelayFullNode
	hold time.Duration

	mu       sync.Mutex
	inFlight int
	peak     int
	calls    atomic.Int64
	failFor  map[string]bool
}

func (c *prefetchFullNode) GetSession(_ context.Context, serviceID string, _ string) (*sessiontypes.Session, error) {
	c.calls.Add(1)
	c.mu.Lock()
	c.inFlight++
	if c.inFlight > c.peak {
		c.peak = c.inFlight
	}
	c.mu.Unlock()

	if c.hold > 0 {
		time.Sleep(c.hold)
	}

	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()

	if c.failFor[serviceID] {
		return nil, errors.New("no suppliers staked")
	}
	return &sessiontypes.Session{
		SessionId: "session-" + serviceID,
		Header: &sessiontypes.SessionHeader{
			SessionId:             "session-" + serviceID,
			ServiceId:             serviceID,
			SessionEndBlockHeight: 110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
	}, nil
}

func newPrefetchProtocol(t *testing.T, fn *prefetchFullNode, serviceIDs ...domain.ServiceID) *Protocol {
	t.Helper()
	services := make(map[domain.ServiceID]struct{}, len(serviceIDs))
	apps := make(map[domain.ServiceID][]string, len(serviceIDs))
	for _, id := range serviceIDs {
		services[id] = struct{}{}
		apps[id] = []string{"pokt1app"}
	}
	return &Protocol{
		fullNode:   fn,
		sessions:   newSessionManager(fn, services, newTestLogger()),
		signer:     &mockSigner{},
		bl:         newBlacklist(),
		ownedApps:  apps,
		httpClient: http.DefaultClient,
		metrics:    noopSupplierMetrics{},
		logger:     newTestLogger(),
	}
}

// The point: after prefetch, a relay must not have to fetch a session.
func TestPrefetchSessions_WarmsEveryService(t *testing.T) {
	fn := &prefetchFullNode{}
	p := newPrefetchProtocol(t, fn, "eth", "poly", "kava", "sei")

	res := p.PrefetchSessions(context.Background(), PrefetchConfig{MinInterval: -1})

	if len(res.Ready) != 4 {
		t.Fatalf("ready = %v, want all 4 services", res.Ready)
	}
	if res.Failed != 0 {
		t.Errorf("failed = %d, want 0", res.Failed)
	}
	before := fn.calls.Load()

	// A relay-path lookup now hits the cache instead of the full node.
	if _, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC); err != nil {
		t.Fatal(err)
	}
	if got := fn.calls.Load(); got != before {
		t.Errorf("AvailableEndpoints fetched again (%d -> %d); prefetch did not warm the cache", before, got)
	}
}

// A service with no staked suppliers has no session to fetch and must not hold
// up a pod that can serve the others.
func TestPrefetchSessions_FailuresDoNotBlockTheRest(t *testing.T) {
	fn := &prefetchFullNode{failFor: map[string]bool{"dead": true}}
	p := newPrefetchProtocol(t, fn, "eth", "poly", "dead")

	res := p.PrefetchSessions(context.Background(), PrefetchConfig{MinInterval: -1})

	if res.Failed != 1 {
		t.Errorf("failed = %d, want 1", res.Failed)
	}
	if len(res.Ready) != 2 {
		t.Errorf("ready = %v, want the 2 live services", res.Ready)
	}
	for _, svc := range res.Ready {
		if svc == "dead" {
			t.Error("a service whose fetch failed must not be reported ready")
		}
	}
}

// The full node is shared and rate-limited: a rolling fleet must not arrive as
// a burst.
func TestPrefetchSessions_RespectsConcurrencyAndPace(t *testing.T) {
	t.Run("concurrency caps in-flight fetches", func(t *testing.T) {
		fn := &prefetchFullNode{hold: 20 * time.Millisecond}
		services := make([]domain.ServiceID, 0, 12)
		for i := range 12 {
			services = append(services, domain.ServiceID("svc-"+string(rune('a'+i))))
		}
		p := newPrefetchProtocol(t, fn, services...)

		p.PrefetchSessions(context.Background(), PrefetchConfig{Concurrency: 3, MinInterval: -1})

		fn.mu.Lock()
		peak := fn.peak
		fn.mu.Unlock()
		if peak > 3 {
			t.Errorf("peak in-flight = %d, want at most 3", peak)
		}
	})

	t.Run("pace floors the gap between fetches", func(t *testing.T) {
		fn := &prefetchFullNode{}
		p := newPrefetchProtocol(t, fn, "eth", "poly", "kava", "sei", "bsc")

		const interval = 20 * time.Millisecond
		res := p.PrefetchSessions(context.Background(), PrefetchConfig{Concurrency: 5, MinInterval: interval})

		// Five fetches at one per interval cannot finish faster than four
		// intervals, however many workers are free.
		if min := 4 * interval; res.Elapsed < min {
			t.Errorf("elapsed %v < %v: pacing did not hold with 5 workers free", res.Elapsed, min)
		}
		if len(res.Ready) != 5 {
			t.Errorf("ready = %v, want all 5", res.Ready)
		}
	})
}

// Running out of time is not an error: what was warmed stays warmed, and the
// startup must not hang.
func TestPrefetchSessions_StopsOnContextEnd(t *testing.T) {
	fn := &prefetchFullNode{hold: 50 * time.Millisecond}
	services := make([]domain.ServiceID, 0, 40)
	for i := range 40 {
		services = append(services, domain.ServiceID("svc-"+string(rune('a'+i))))
	}
	p := newPrefetchProtocol(t, fn, services...)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	done := make(chan PrefetchResult, 1)
	go func() { done <- p.PrefetchSessions(ctx, PrefetchConfig{Concurrency: 2, MinInterval: -1}) }()

	select {
	case res := <-done:
		if len(res.Ready) >= 40 {
			t.Errorf("finished all %d services inside the deadline; test is not exercising cancellation", len(res.Ready))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PrefetchSessions did not return after its context ended")
	}
}

// No configured services is a valid deployment, not a division by zero.
func TestPrefetchSessions_NoServices(t *testing.T) {
	fn := &prefetchFullNode{}
	p := newPrefetchProtocol(t, fn)

	res := p.PrefetchSessions(context.Background(), PrefetchConfig{})

	if len(res.Ready) != 0 || res.Failed != 0 {
		t.Errorf("got %+v, want an empty result", res)
	}
	if fn.calls.Load() != 0 {
		t.Errorf("fetched %d sessions for no configured services", fn.calls.Load())
	}
}
