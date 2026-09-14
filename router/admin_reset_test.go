package router

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/override"
	"github.com/pokt-network/sage/reputation"
)

func TestResetAnnouncementKey_RoundTrip(t *testing.T) {
	for _, target := range []string{"rm02.kalorius.tech", "https://rm02.kalorius.tech|rest", "pokt1abc-https://rm02.kalorius.tech:443/v1"} {
		key := resetAnnouncementKey("osmosis", target)
		svc, got, ok := parseResetAnnouncementKey(key)
		if !ok || svc != "osmosis" || got != target {
			t.Fatalf("%q -> %q -> (%q, %q, %v)", target, key, svc, got, ok)
		}
	}
	for _, key := range []string{ReputationResetPrefix, ReputationResetPrefix + "osmosis", ReputationResetPrefix + "osmosis/", ReputationResetPrefix + "/abc", "other/osmosis/abc"} {
		if _, _, ok := parseResetAnnouncementKey(key); ok {
			t.Fatalf("%q parsed as an announcement", key)
		}
	}
}

// A reset taken on one replica's admin port is applied on every other
// replica through the override store, by the same matching; one announced
// before a replica started is not replayed onto it.
func TestAdminResetReputation_FansOutToOtherReplicas(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := override.NewMemoryStore()

	// Replica B: a real reputation service with two penalised hosts.
	svcB := reputation.NewService(reputation.NewMemoryStorage(), nil, reputation.DefaultServiceConfig())
	svcB.Start()
	defer svcB.Stop()
	fresh := domain.EndpointAddr("pokt1abc-https://supplier1-example.com")
	stale := domain.EndpointAddr("pokt1old-https://old.example")
	for _, ep := range []domain.EndpointAddr{fresh, stale} {
		if err := svcB.RecordSignal(ctx, "eth", ep, domain.RPCTypeJSONRPC, reputation.NewCriticalErrorSignal("bad", 0)); err != nil {
			t.Fatal(err)
		}
	}
	// Announced two minutes before B starts: B must leave old.example alone.
	if err := store.Set(ctx, resetAnnouncementKey("eth", "old.example"), time.Now().Add(-2*time.Minute).UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	replicaB := &AdminAPI{repService: svcB, overrides: store, logger: slog.Default(), resetWatchInterval: 20 * time.Millisecond}
	replicaB.WatchReputationResets(ctx)

	// Replica A takes the reset.
	replicaA, srv := newAdminServer(t)
	defer srv.Close()
	replicaA.SetOverrides(store)
	status, body := doTuning(t, srv.URL, http.MethodPost, "/admin/reputation/reset/eth/supplier1-example.com", "")
	if status != http.StatusOK || body["persisted"] != false {
		t.Fatalf("reset on A: status %d body %v", status, body)
	}
	if _, ok, _ := store.Get(ctx, resetAnnouncementKey("eth", "supplier1-example.com")); !ok {
		t.Fatal("the reset was not announced in the override store")
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		score, _ := svcB.GetScore(ctx, "eth", fresh, domain.RPCTypeJSONRPC)
		if score == reputation.DefaultServiceConfig().InitialScore {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica B never applied the reset: score %v", score)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if score, _ := svcB.GetScore(ctx, "eth", stale, domain.RPCTypeJSONRPC); score == reputation.DefaultServiceConfig().InitialScore {
		t.Fatal("an announcement older than the replica was replayed onto it")
	}

	// A second reset of the same target is a new announcement (new value)
	// and is applied again.
	if err := svcB.RecordSignal(ctx, "eth", fresh, domain.RPCTypeJSONRPC, reputation.NewCriticalErrorSignal("bad", 0)); err != nil {
		t.Fatal(err)
	}
	if status, _ = doTuning(t, srv.URL, http.MethodPost, "/admin/reputation/reset/eth/supplier1-example.com", ""); status != http.StatusOK {
		t.Fatalf("second reset: status %d", status)
	}
	deadline = time.Now().Add(3 * time.Second)
	for {
		score, _ := svcB.GetScore(ctx, "eth", fresh, domain.RPCTypeJSONRPC)
		if score == reputation.DefaultServiceConfig().InitialScore {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica B never applied the second reset: score %v", score)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
