package reputation

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// stored writes one state directly into storage the way the write-behind
// would, with an explicit age.
func stored(t *testing.T, store *MemoryStorage, serviceID domain.ServiceID, repKey string, score float64, age time.Duration) {
	t.Helper()
	st := State{Score: score, Attempts: 3, UpdatedAt: time.Now().Add(-age).Unix()}
	if err := store.SetState(context.Background(), scoreKey(serviceID, repKey), st); err != nil {
		t.Fatal(err)
	}
}

// The point of the whole exercise: a fresh process must inherit the fleet's
// scores instead of assuming every endpoint is perfect.
func TestHydrate_LoadsPersistedScores(t *testing.T) {
	store := NewMemoryStorage()
	stored(t, store, "eth", "https://node1.example.com|json_rpc", 42, time.Minute)
	stored(t, store, "eth", "https://node2.example.com|json_rpc", 71, time.Minute)
	stored(t, store, "poly", "https://node3.example.com|json_rpc", 13, time.Minute)

	svc := NewService(store, nil, DefaultServiceConfig())

	// Before hydration a cold service claims every endpoint is perfect.
	if got, _ := svc.scoreForSelector(context.Background(), "eth", "supA-https://node1.example.com", domain.RPCTypeJSONRPC); got != svc.cfg.InitialScore {
		t.Fatalf("precondition: cold score = %v, want InitialScore %v", got, svc.cfg.InitialScore)
	}

	res, err := svc.Hydrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Keys != 3 {
		t.Errorf("loaded %d keys, want 3 (skipped %d)", res.Keys, res.Skipped)
	}
	if len(res.Services) != 2 {
		t.Errorf("services = %v, want 2 distinct", res.Services)
	}

	// The score a selector reads must now be the persisted one.
	got, _ := svc.scoreForSelector(context.Background(), "eth", "supA-https://node1.example.com", domain.RPCTypeJSONRPC)
	if got != 42 {
		t.Errorf("hydrated score = %v, want 42", got)
	}
	// Service scoping survives the round trip: poly's key must not land on eth.
	if got, _ := svc.scoreForSelector(context.Background(), "eth", "supA-https://node3.example.com", domain.RPCTypeJSONRPC); got != svc.cfg.InitialScore {
		t.Errorf("poly's key leaked into eth: score = %v", got)
	}
}

// A state the storage sweep is about to delete must not be adopted, and one
// written before UpdatedAt existed is stale by the same rule.
func TestHydrate_SkipsStaleAndUnstamped(t *testing.T) {
	store := NewMemoryStorage()
	stored(t, store, "eth", "https://fresh.example.com|json_rpc", 42, time.Minute)
	stored(t, store, "eth", "https://stale.example.com|json_rpc", 7, 2*DefaultIdleTTL)
	if err := store.SetState(context.Background(),
		scoreKey("eth", "https://unstamped.example.com|json_rpc"),
		State{Score: 9},
	); err != nil {
		t.Fatal(err)
	}

	svc := NewService(store, nil, DefaultServiceConfig())
	res, err := svc.Hydrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Keys != 1 {
		t.Errorf("loaded %d keys, want only the fresh one", res.Keys)
	}
	if res.Skipped != 2 {
		t.Errorf("skipped %d, want 2 (stale + unstamped)", res.Skipped)
	}
	for _, host := range []string{"https://stale.example.com", "https://unstamped.example.com"} {
		got, _ := svc.scoreForSelector(context.Background(), "eth", domain.EndpointAddr("supA-"+host), domain.RPCTypeJSONRPC)
		if got != svc.cfg.InitialScore {
			t.Errorf("%s adopted a score of %v; stale state must not be loaded", host, got)
		}
	}
}

// A signal recorded while the read was in flight is newer than storage.
func TestHydrate_DoesNotOverwriteLiveState(t *testing.T) {
	store := NewMemoryStorage()
	stored(t, store, "eth", "https://node1.example.com|json_rpc", 42, time.Minute)

	svc := NewService(store, nil, DefaultServiceConfig())
	ep := domain.EndpointAddr("supA-https://node1.example.com")
	if err := svc.RecordSignal(context.Background(), "eth", ep, domain.RPCTypeJSONRPC,
		Signal{Type: SignalMajorError, Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	live, _ := svc.scoreForSelector(context.Background(), "eth", ep, domain.RPCTypeJSONRPC)

	res, err := svc.Hydrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Keys != 0 {
		t.Errorf("loaded %d keys over live state, want 0", res.Keys)
	}
	got, _ := svc.scoreForSelector(context.Background(), "eth", ep, domain.RPCTypeJSONRPC)
	if got != live {
		t.Errorf("score = %v, want the live %v — storage must not clobber it", got, live)
	}
}

// A field that scoreKey did not write is not ours to interpret.
func TestSplitScoreKey(t *testing.T) {
	cases := []struct {
		field   string
		svc     domain.ServiceID
		key     string
		wantOK  bool
		whatFor string
	}{
		{field: "eth:https://node.example.com|json_rpc", svc: "eth", key: "https://node.example.com|json_rpc", wantOK: true, whatFor: "a URL's own colons stay in the key"},
		{field: "eth:host:443|json_rpc", svc: "eth", key: "host:443|json_rpc", wantOK: true, whatFor: "split on the first colon only"},
		{field: "nocolon", wantOK: false, whatFor: "not written by scoreKey"},
		{field: ":key", wantOK: false, whatFor: "empty service id"},
		{field: "eth:", wantOK: false, whatFor: "empty reputation key"},
	}
	for _, tc := range cases {
		t.Run(tc.whatFor, func(t *testing.T) {
			svc, key, ok := splitScoreKey(tc.field)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if svc != tc.svc || key != tc.key {
				t.Errorf("got (%q, %q), want (%q, %q)", svc, key, tc.svc, tc.key)
			}
		})
	}
}
