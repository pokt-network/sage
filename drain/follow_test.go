package drain

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// canary-sage following mainnet: it applies mainnet's auto drains, not the
// drains a person set there for mainnet's own reasons, and never writes to
// mainnet's hash. Its own drains stay its own.
func TestFollowPeer_AppliesOnlyThePeersAutoDrains(t *testing.T) {
	ctx := context.Background()
	peerRedis := newFakeRedis()
	mainnet := NewRedisStore(peerRedis)
	auto := Key{ServiceID: "sei", Operator: "rpcgate.xyz", RPCType: domain.RPCTypeJSONRPC}
	manual := Key{ServiceID: "base", Operator: "stakeandrelax.net", RPCType: domain.RPCTypeJSONRPC}
	_ = mainnet.Set(ctx, Entry{Key: auto, Until: time.Now().Add(time.Hour), Reason: "auto: 40 collapse picks"})
	_ = mainnet.Set(ctx, Entry{Key: manual, Until: time.Now().Add(time.Hour), Reason: "ops: base relief"})

	follower := NewRedisStore(peerRedis, FollowPeer("auto:"))
	follower.refresh(ctx)
	if !follower.Drained(auto.ServiceID, auto.Operator, auto.RPCType) {
		t.Fatal("the peer's auto drain does not apply")
	}
	if follower.Drained(manual.ServiceID, manual.Operator, manual.RPCType) {
		t.Fatal("the peer's manual drain applied; only auto: drains are followed")
	}

	local := NewMemoryStore()
	store := Merge(local, follower)
	own := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}
	before := len(peerRedis.data)
	if err := store.Set(ctx, Entry{Key: own, Until: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if len(peerRedis.data) != before {
		t.Fatal("a local drain was written to the peer's hash")
	}
	if !store.Drained(own.ServiceID, own.Operator, own.RPCType) || !store.Drained(auto.ServiceID, auto.Operator, auto.RPCType) {
		t.Fatal("the merged store must answer for both its own and the peer's drains")
	}
	if got := store.Active(ctx, "sei"); len(got) != 1 || got[0].Key != auto {
		t.Fatalf("Active(sei) = %+v, want the peer's auto drain listed", got)
	}
}
