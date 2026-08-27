package drain

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func TestMemoryStore_SetDrainedRelease(t *testing.T) {
	s := NewMemoryStore()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}
	if err := s.Set(context.Background(), Entry{Key: k, Until: time.Now().Add(time.Minute), Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	if !s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("scoped drain must match its rpc type")
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeWebSocket) {
		t.Fatal("scoped drain must not match another rpc type")
	}
	if s.Drained("poly", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("drain is per service")
	}
	if err := s.Release(context.Background(), k); err != nil {
		t.Fatal(err)
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("released drain still matches")
	}
}

func TestMemoryStore_UnscopedDrainCoversEveryRPCType(t *testing.T) {
	s := NewMemoryStore()
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "slow.example"}, Until: time.Now().Add(time.Minute)})
	for _, rt := range []domain.RPCType{domain.RPCTypeJSONRPC, domain.RPCTypeWebSocket, domain.RPCTypeREST} {
		if !s.Drained("eth", "slow.example", rt) {
			t.Fatalf("unscoped drain must cover %s", rt)
		}
	}
}

func TestMemoryStore_ExpiryIsLazy(t *testing.T) {
	s := NewMemoryStore()
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "slow.example"}, Until: time.Now().Add(20 * time.Millisecond)})
	time.Sleep(30 * time.Millisecond)
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("expired drain still matches")
	}
	if got := s.Active(context.Background(), "eth"); got != nil {
		t.Fatalf("expired drain still listed: %+v", got)
	}
}

func TestMemoryStore_ActiveSortedAndLiveOnly(t *testing.T) {
	s := NewMemoryStore()
	now := time.Now()
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "b.example"}, Until: now.Add(time.Minute)})
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "a.example", RPCType: domain.RPCTypeREST}, Until: now.Add(time.Minute)})
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "z.example"}, Until: now.Add(-time.Second)})
	got := s.Active(context.Background(), "eth")
	if len(got) != 2 || got[0].Operator != "a.example" || got[1].Operator != "b.example" {
		t.Fatalf("Active = %+v", got)
	}
}

func TestMemoryStore_PastUntilIsARelease(t *testing.T) {
	s := NewMemoryStore()
	k := Key{ServiceID: "eth", Operator: "slow.example"}
	_ = s.Set(context.Background(), Entry{Key: k, Until: time.Now().Add(time.Minute)})
	_ = s.Set(context.Background(), Entry{Key: k, Until: time.Now().Add(-time.Second)})
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("Set with a past Until must release")
	}
}
