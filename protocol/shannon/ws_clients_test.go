package shannon

import (
	"context"
	"fmt"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/featureflag"
)

// The pattern the ledger exists for: a client that drops every connection
// that did not land on the operator it wants, then holds the one that did.
func TestWSClientLedger_ShowsSupplierShopping(t *testing.T) {
	l := newWSClientLedger(nil)
	wanted := wsSupplierKey{service: "robinhood", operator: "favoured.example", owner: "pokt1owner"}
	for i := 0; i < 5; i++ {
		l.opened("203.0.113.7")
		other := wsSupplierKey{service: "robinhood", operator: fmt.Sprintf("peer%d.example", i), owner: "pokt1peer"}
		l.tenure("203.0.113.7", other, 2*time.Second, 1, true)
	}
	l.opened("203.0.113.7")
	l.tenure("203.0.113.7", wanted, 3*time.Hour, 90000, true)

	l.opened("198.51.100.1")
	l.tenure("198.51.100.1", wanted, 10*time.Minute, 500, true)

	snap := l.snapshot("", 0)
	if len(snap.Clients) != 2 {
		t.Fatalf("clients = %d, want 2", len(snap.Clients))
	}
	top := snap.Clients[0]
	if top.ClientIP != "203.0.113.7" || top.Connections != 6 || top.QuickCloses != 5 || top.Frames != 90005 {
		t.Fatalf("top client = %+v, want the shopper first with 6 connections, 5 quick closes", top)
	}
	if s := top.Suppliers[0]; s.Operator != "favoured.example" || s.Owner != "pokt1owner" || s.TenureSeconds != 3*3600 {
		t.Errorf("shopper's first supplier = %+v, want the favoured one, busiest first", s)
	}
	// A long tenure the client ended is not a quick close.
	if snap.Clients[1].QuickCloses != 0 {
		t.Errorf("an ordinary client = %+v, want no quick closes", snap.Clients[1])
	}

	if only := l.snapshot("eth", 0); len(only.Clients) != 0 {
		t.Errorf("service filter: want no eth clients, got %+v", only.Clients)
	}
	if one := l.snapshot("", 1); len(one.Clients) != 1 {
		t.Errorf("limit 1 returned %d clients", len(one.Clients))
	}
}

// Two windows: a snapshot sees the previous hour and the current one, and a
// third hour ages the first out.
func TestWSClientLedger_Windows(t *testing.T) {
	now := time.Unix(0, 0)
	l := newWSClientLedger(func() time.Time { return now })
	key := wsSupplierKey{service: "eth", operator: "a.example", owner: "pokt1a"}

	l.tenure("192.0.2.1", key, time.Minute, 10, false)
	now = now.Add(wsLedgerWindow)
	l.tenure("192.0.2.1", key, time.Minute, 20, false)
	if s := l.snapshot("", 0); len(s.Clients) != 1 || s.Clients[0].Frames != 30 {
		t.Fatalf("across two windows = %+v, want 30 frames", s.Clients)
	}
	now = now.Add(wsLedgerWindow)
	if s := l.snapshot("", 0); len(s.Clients) != 1 || s.Clients[0].Frames != 20 {
		t.Fatalf("after a third window = %+v, want only the second window's 20 frames", s.Clients)
	}
}

// The ledger is keyed by client address, which a caller chooses: past the cap
// new clients are counted as dropped, never stored.
func TestWSClientLedger_Bounded(t *testing.T) {
	l := newWSClientLedger(nil)
	key := wsSupplierKey{service: "eth", operator: "a.example", owner: "pokt1a"}
	for i := 0; i < wsLedgerMaxClients+10; i++ {
		l.tenure(fmt.Sprintf("10.0.%d.%d", i/256, i%256), key, time.Second, 1, false)
	}
	s := l.snapshot("", wsLedgerMaxClients+100)
	if len(s.Clients) != wsLedgerMaxClients || s.Dropped != 10 {
		t.Fatalf("clients=%d dropped=%d, want %d and 10", len(s.Clients), s.Dropped, wsLedgerMaxClients)
	}

	for i := 0; i < wsLedgerMaxSuppliers+5; i++ {
		l2 := wsSupplierKey{service: "eth", operator: fmt.Sprintf("op%d.example", i), owner: "pokt1a"}
		l.tenure("10.0.0.0", l2, time.Second, 1, false)
	}
	for _, c := range l.snapshot("", wsLedgerMaxClients).Clients {
		if c.ClientIP == "10.0.0.0" && len(c.Suppliers) > wsLedgerMaxSuppliers {
			t.Fatalf("suppliers per client = %d, want at most %d", len(c.Suppliers), wsLedgerMaxSuppliers)
		}
	}
}

// bindSupplier and releaseSupplier are the glue Open and the rebind handler
// call: the gauge goes up and down, the tenure lands in the ledger with the
// supplier's frames.
func TestWSRelayer_SupplierTenureAccounting(t *testing.T) {
	spy := &spyWSMetrics{}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{logger: newTestLogger()}, Reputation: &spyRepSvc{}, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(), Metrics: spy,
	})
	proc := newWSMessageProcessor(context.Background(), r.deps.Protocol,
		&sessiontypes.SessionHeader{ServiceId: "eth"}, "pokt1op", "pokt1op-https://rel001.op-alpha.example",
		&apptypes.Application{}, nil).withSupplier(spy, "pokt1owner")
	proc.endpointFrames.Add(7)

	r.bindSupplier("eth", proc)
	r.releaseSupplier("eth", "203.0.113.9", proc, true)

	spy.mu.Lock()
	if len(spy.bound) != 1 || spy.bound[0] != "op-alpha.example|pokt1owner" || len(spy.released) != 1 {
		t.Fatalf("bound=%v released=%v", spy.bound, spy.released)
	}
	spy.mu.Unlock()
	snap := r.Clients("", 0)
	if len(snap.Clients) != 1 || snap.Clients[0].Frames != 7 || snap.Clients[0].QuickCloses != 1 {
		t.Fatalf("ledger = %+v, want the tenure's 7 frames and one quick close", snap.Clients)
	}
}
