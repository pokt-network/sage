package qos

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func newTestConsensus(t *testing.T) *BlockConsensus {
	t.Helper()
	return NewBlockConsensus(nil, 0)
}

// The point: the spread and the freshness of what selection is filtering on.
func TestChainView_ReportsSpreadAndFreshness(t *testing.T) {
	bc := newTestConsensus(t)
	bc.AddObservation("supA-https://a.example.com", 1000)
	bc.AddObservation("supB-https://b.example.com", 1004)
	bc.AddObservation("supC-https://c.example.com", 998)

	view := bc.ChainView()

	if view.Endpoints != 3 {
		t.Errorf("Endpoints = %d, want 3", view.Endpoints)
	}
	if view.Highest != 1004 || view.Lowest != 998 {
		t.Errorf("bounds = [%d, %d], want [998, 1004]", view.Lowest, view.Highest)
	}
	if got := view.Spread(); got != 6 {
		t.Errorf("Spread = %d, want 6", got)
	}
	if view.Newest.IsZero() {
		t.Error("Newest is zero with three observations in the window")
	}
	if time.Since(view.Newest) > time.Minute {
		t.Errorf("Newest is %v old; these were just added", time.Since(view.Newest))
	}
	if view.Perceived == 0 {
		t.Error("Perceived = 0 with observations present")
	}
}

// One endpoint reporting repeatedly is one endpoint, not many. The count is
// what tells an operator a "spread of 0" is agreement rather than silence.
func TestChainView_CountsDistinctEndpoints(t *testing.T) {
	bc := newTestConsensus(t)
	for range 5 {
		bc.AddObservation("supA-https://a.example.com", 1000)
	}

	view := bc.ChainView()
	if view.Endpoints != 1 {
		t.Errorf("Endpoints = %d, want 1 — five reports from one endpoint are one endpoint", view.Endpoints)
	}
	if view.Spread() != 0 {
		t.Errorf("Spread = %d, want 0", view.Spread())
	}
}

// A service nobody is reporting for must say so, rather than reporting its
// last known spread forever. This is the state the whole metric exists to make
// visible: a chain view that has gone quiet.
func TestChainView_EmptyWindowIsNotStaleData(t *testing.T) {
	bc := newTestConsensus(t)
	bc.AddObservation("supA-https://a.example.com", 1000)
	bc.AddObservation("supB-https://b.example.com", 1050)

	// Age every observation out of the window.
	bc.mu.Lock()
	for i := range bc.observations {
		bc.observations[i].Timestamp = time.Now().Add(-2 * bc.windowDuration)
	}
	bc.mu.Unlock()

	view := bc.ChainView()
	if view.Endpoints != 0 {
		t.Errorf("Endpoints = %d, want 0 for a window with nothing in it", view.Endpoints)
	}
	if view.Spread() != 0 {
		t.Errorf("Spread = %d, want 0", view.Spread())
	}
	if !view.Newest.IsZero() {
		t.Errorf("Newest = %v, want zero so the exporter omits staleness rather than reporting the epoch", view.Newest)
	}
	// Perceived survives: selection is still filtering on it, which is exactly
	// why an operator needs to see that nothing confirms it.
	if view.Perceived == 0 {
		t.Error("Perceived = 0; the view should still report what selection uses")
	}
}

// Reading the view must not disturb what a concurrent write is computing.
func TestChainView_DoesNotPrune(t *testing.T) {
	bc := newTestConsensus(t)
	bc.AddObservation("supA-https://a.example.com", 1000)

	bc.mu.RLock()
	before := len(bc.observations)
	bc.mu.RUnlock()

	for range 3 {
		bc.ChainView()
	}

	bc.mu.RLock()
	after := len(bc.observations)
	bc.mu.RUnlock()
	if before != after {
		t.Errorf("observations went %d -> %d across three reads; ChainView must not mutate", before, after)
	}
}

var _ ChainViewer = (*BlockConsensus)(nil)

// Registry answers for a plugin that tracks a chain, and declines for one that
// does not, so a heightless service is absent from the metric rather than zero.
func TestRegistry_ChainViewFor(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register("noop", stubPlugin{}); err != nil {
		t.Fatal(err)
	}

	if _, ok := reg.ChainViewFor("noop"); ok {
		t.Error("reported a chain view for a plugin that tracks none")
	}
	if _, ok := reg.ChainViewFor("never-registered"); ok {
		t.Error("reported a chain view for an unregistered service")
	}
}

// stubPlugin implements the core Plugin interface and nothing else, so it is
// the shape of a service that tracks no chain.
type stubPlugin struct{}

func (stubPlugin) ParseRequest(_ context.Context, _ *http.Request, _ []byte, _ domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}

func (stubPlugin) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}
