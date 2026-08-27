package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// hedgeVerdictChain composes Observe(Hedge(Heuristic(inner))) — the real
// arrangement: Observe sits OUTSIDE Hedge, so the only verdict it can see is
// the one mergeContext copies out of the winning arm.
//
// HedgeDelay is deliberately long and inner returns at once, so the primary
// arm wins through the pre-delay branch and the race never has to be timed.
func hedgeVerdictChain(inner relay.Handler) (relay.Handler, *trackingRepService) {
	repSvc := &trackingRepService{}
	flags := newFlags("hedge", "heuristic")
	cfg := func(domain.ServiceID) config.RetryConfig {
		return config.RetryConfig{HedgeDelay: 500 * time.Millisecond}
	}
	chain := Observe(flags, nil, repSvc, nil)(Hedge(flags, cfg)(Heuristic(flags)(inner)))
	return chain, repSvc
}

// With hedging on, a transport timeout must still reach reputation as a major
// error. Before mergeContext carried HeuristicResult, Observe saw no verdict
// here and fell back to the old MinorError path — so the timeout grading
// worked only when hedge_delay was 0.
func TestHedge_MergesTimeoutVerdictIntoReputation(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = testEndpoints(1)[0]
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.DeadlineExceeded, true)
	})
	chain, repSvc := hedgeVerdictChain(inner)

	if err := chain.HandleRelay(baseContext()); err == nil {
		t.Fatal("want the arm's error to propagate")
	}

	repSvc.mu.Lock()
	defer repSvc.mu.Unlock()
	if !repSvc.called {
		t.Fatal("a supplier timeout must be recorded")
	}
	if repSvc.last.Type != reputation.SignalMajorError {
		t.Fatalf("signal = %q, want %q", repSvc.last.Type, reputation.SignalMajorError)
	}
}

// A client hang-up is nobody's signal, hedging or not.
func TestHedge_MergesClientCancelVerdictSoNothingIsRecorded(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = testEndpoints(1)[0]
		// Stand in for the client hanging up mid-attempt: what the Heuristic
		// middleware grades on is the attempt context's Err().
		cancelled, cancel := context.WithCancel(ctx.Ctx)
		cancel()
		ctx.Ctx = cancelled
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.Canceled, true)
	})
	chain, repSvc := hedgeVerdictChain(inner)

	if err := chain.HandleRelay(baseContext()); err == nil {
		t.Fatal("want the arm's error to propagate")
	}

	repSvc.mu.Lock()
	defer repSvc.mu.Unlock()
	if repSvc.called {
		t.Fatalf("a client cancel was scored against the supplier: %+v", repSvc.last)
	}
}
