package shannon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// A supplier that refused for over-servicing is out for that session only,
// for that service only, and by supplier address: another supplier behind the
// same URL, or the same supplier in the next session, is not.
func TestOverServed_ScopedToSupplierServiceAndSession(t *testing.T) {
	o := newOverServed()
	if !o.mark("eth", "pokt1a", 110) || o.mark("eth", "pokt1a", 110) {
		t.Fatal("mark reports new only the first time")
	}
	switch {
	case !o.excluded("eth", "pokt1a", 110):
		t.Error("marked supplier not excluded in its session")
	case o.excluded("eth", "pokt1b", 110):
		t.Error("another supplier excluded")
	case o.excluded("base", "pokt1a", 110):
		t.Error("another service excluded")
	case o.excluded("eth", "pokt1a", 120):
		t.Error("the next session excluded")
	}
	// A later session's mark forgets the earlier ones for the service.
	o.mark("eth", "pokt1b", 120)
	if o.excluded("eth", "pokt1a", 110) || len(o.m) != 1 {
		t.Fatalf("older session kept: %v", o.m)
	}
}

// The selector stops seeing an over-served supplier for the rest of the
// session it refused in.
func TestAvailableEndpoints_ExcludesOverServedForTheSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	session := buildRelayTestSession("pokt1spent", server.URL)
	fn := &mockRelayFullNode{session: session}
	rec := &recordingMetrics{}
	p := &Protocol{
		fullNode:   fn,
		sessions:   newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger()),
		signer:     &mockSigner{},
		bl:         newBlacklist(),
		overServed: newOverServed(),
		metrics:    rec,
		ownedApps:  map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		httpClient: server.Client(),
		logger:     newTestLogger(),
	}
	count := func() int {
		eps, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
		if err != nil {
			t.Fatal(err)
		}
		return len(eps)
	}
	if count() == 0 {
		t.Fatal("no endpoint before the refusal")
	}
	end := session.Header.SessionEndBlockHeight
	p.markOverServed("eth", "pokt1spent", end)
	p.markOverServed("eth", "pokt1spent", end)
	if n := count(); n != 0 {
		t.Fatalf("%d endpoints after the refusal, want the supplier gone for the session", n)
	}
	if rec.overServed != 1 {
		t.Fatalf("counted %d exclusions, want one per supplier and session", rec.overServed)
	}
	p.overServed.mark("eth", "pokt1other", end+10)
	if count() == 0 {
		t.Fatal("still excluded once its session is forgotten")
	}
}
