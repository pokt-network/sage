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

// The block rate is derived, not configured, so it has to come out of movement
// and be honestly absent when there is none.
func TestBlockRate(t *testing.T) {
	t.Run("unknown with no movement", func(t *testing.T) {
		bc := newTestConsensus(t)
		if _, ok := bc.BlockRate(); ok {
			t.Error("reported a rate with no observations")
		}
		// A chain observed repeatedly at one height is stalled, not slow.
		for range 5 {
			bc.AddObservation("supA-https://a.example.com", 1000)
		}
		if rate, ok := bc.BlockRate(); ok {
			t.Errorf("reported %v blocks/s for a chain that has not moved", rate)
		}
	})

	t.Run("derived from how far the height moved", func(t *testing.T) {
		bc := newTestConsensus(t)
		// Two samples a known distance apart, written directly so the test
		// controls the clock rather than sleeping.
		bc.mu.Lock()
		bc.rateSamples = []rateSample{
			{height: 1000, at: time.Now().Add(-10 * time.Second)},
			{height: 1040, at: time.Now()},
		}
		bc.mu.Unlock()

		rate, ok := bc.BlockRate()
		if !ok {
			t.Fatal("no rate from two moving samples")
		}
		if rate < 3.9 || rate > 4.1 {
			t.Errorf("rate = %v, want about 4 blocks/s (40 blocks in 10s)", rate)
		}
	})

	t.Run("repeats do not fill the history", func(t *testing.T) {
		bc := newTestConsensus(t)
		for range 50 {
			bc.AddObservation("supA-https://a.example.com", 1000)
		}
		bc.mu.RLock()
		n := len(bc.rateSamples)
		bc.mu.RUnlock()
		if n != 1 {
			t.Errorf("history holds %d samples for one height, want 1 — repeats would derive a rate of zero", n)
		}
	})

	t.Run("history is bounded", func(t *testing.T) {
		bc := newTestConsensus(t)
		for i := range maxRateSamples * 3 {
			bc.AddObservation("supA-https://a.example.com", uint64(1000+i))
		}
		bc.mu.RLock()
		n := len(bc.rateSamples)
		bc.mu.RUnlock()
		if n > maxRateSamples {
			t.Errorf("history holds %d samples, want at most %d", n, maxRateSamples)
		}
	})

	t.Run("a reset discards the rate", func(t *testing.T) {
		bc := newTestConsensus(t)
		bc.mu.Lock()
		bc.rateSamples = []rateSample{
			{height: 1000, at: time.Now().Add(-time.Second)},
			{height: 1010, at: time.Now()},
		}
		bc.mu.Unlock()
		if _, ok := bc.BlockRate(); !ok {
			t.Fatal("precondition: no rate to discard")
		}

		bc.Reset()

		if rate, ok := bc.BlockRate(); ok {
			t.Errorf("rate %v survived a reset; a poisoned chain's cadence must not outlive its heights", rate)
		}
	})
}

// The whole point of the seconds figure: two chains with wildly different
// block counts can have identical spreads in time, and the block figure alone
// reads backwards.
func TestChainView_SpreadSecondsMakesChainsComparable(t *testing.T) {
	fast := ChainView{Highest: 1534, Lowest: 1000, Endpoints: 3, BlockRate: 4, BlockRateKnown: true}
	slow := ChainView{Highest: 1011, Lowest: 1000, Endpoints: 3, BlockRate: 1.0 / 12, BlockRateKnown: true}

	if fast.Spread() != 534 || slow.Spread() != 11 {
		t.Fatalf("preconditions: spreads are %d and %d blocks", fast.Spread(), slow.Spread())
	}

	fastSecs, ok := fast.SpreadSeconds()
	if !ok {
		t.Fatal("no seconds figure for the fast chain")
	}
	slowSecs, ok := slow.SpreadSeconds()
	if !ok {
		t.Fatal("no seconds figure for the slow chain")
	}

	// 534 blocks at 4/s is 133.5s; 11 blocks at one per 12s is 132s.
	if diff := fastSecs - slowSecs; diff > 3 || diff < -3 {
		t.Errorf("fast %.1fs vs slow %.1fs: these should be within seconds of each other, "+
			"which is the fact the block figure hides", fastSecs, slowSecs)
	}
	if fastSecs < 130 || fastSecs > 137 {
		t.Errorf("fast chain spread = %.1fs, want about 133.5s", fastSecs)
	}
}

// No rate means no seconds figure, rather than a zero or an infinity.
func TestChainView_SpreadSecondsAbsentWithoutARate(t *testing.T) {
	for _, v := range []ChainView{
		{Highest: 1100, Lowest: 1000, Endpoints: 2},
		{Highest: 1100, Lowest: 1000, Endpoints: 2, BlockRateKnown: true, BlockRate: 0},
		{Highest: 1100, Lowest: 1000, Endpoints: 2, BlockRateKnown: true, BlockRate: -1},
	} {
		if secs, ok := v.SpreadSeconds(); ok {
			t.Errorf("%+v gave %v seconds; an unknown rate must stay unknown", v, secs)
		}
	}
}
