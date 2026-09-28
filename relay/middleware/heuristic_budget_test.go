package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// A timeout on an attempt left with under half its budget is the time others
// spent, not the host: retried, but not scored and not method-blocked. One
// that had its budget is still the host's, and says how much it had.
func TestHeuristic_TimeoutGradedByTheBudgetTheAttemptHad(t *testing.T) {
	timeoutErr := domain.NewRelayError(domain.ErrTransport, "upstream timed out", context.DeadlineExceeded, true)
	attempt := func(left time.Duration) *relay.Context {
		ctx := baseContext()
		ctx.ServiceID = "sei"
		c, cancel := context.WithTimeout(context.Background(), left)
		t.Cleanup(cancel)
		ctx.Ctx = c
		inner := relay.HandlerFunc(func(*relay.Context) error { return timeoutErr })
		mw := Heuristic(nil, nil, WithAttemptTimeout(func(domain.ServiceID) time.Duration { return 5 * time.Second }))
		_ = mw(inner).HandleRelay(ctx)
		return ctx
	}

	short := attempt(time.Second).HeuristicResult
	if short == nil || short.Reason != "short_budget_timeout" || short.ShouldPenalize || short.MethodBlocking || !short.ShouldRetry {
		t.Fatalf("1s of a 5s budget: got %+v, want short_budget_timeout, retried, unpenalised, not method-blocking", short)
	}

	fair := attempt(4 * time.Second).HeuristicResult
	if fair == nil || fair.Reason != "transport_timeout" || !fair.ShouldPenalize {
		t.Fatalf("4s of a 5s budget: got %+v, want a penalised transport_timeout", fair)
	}
	if want := "of 5s)"; len(fair.Details) < len(want) || fair.Details[len(fair.Details)-len(want):] != want {
		t.Errorf("details %q do not carry the budget", fair.Details)
	}
}
