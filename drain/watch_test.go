package drain

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// A drain that expired or was released between two checks is reported once;
// one still live, or one set since, is not.
func TestWatchEnds_ReportsDrainsThatEnded(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	gone := Key{ServiceID: "sei", Operator: "opa.example", RPCType: domain.RPCTypeJSONRPC}
	stays := Key{ServiceID: "sei", Operator: "opb.example", RPCType: domain.RPCTypeJSONRPC}
	_ = store.Set(ctx, Entry{Key: gone, Until: time.Now().Add(time.Hour)})
	_ = store.Set(ctx, Entry{Key: stays, Until: time.Now().Add(time.Hour)})
	services := []domain.ServiceID{"sei"}

	prev := live(ctx, store, services)
	_ = store.Release(ctx, gone)
	fresh := Key{ServiceID: "sei", Operator: "opc.example", RPCType: domain.RPCTypeJSONRPC}
	_ = store.Set(ctx, Entry{Key: fresh, Until: time.Now().Add(time.Hour)})
	cur := live(ctx, store, services)

	got := ended(prev, cur)
	if len(got) != 1 || got[0].Key != gone {
		t.Fatalf("ended = %+v, want only the released drain", got)
	}
	if len(ended(cur, live(ctx, store, services))) != 0 {
		t.Fatal("nothing ended since the last check, but something was reported")
	}
}
