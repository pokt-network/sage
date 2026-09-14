package middleware

import (
	"slices"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// The operator-aware retry prefers another operator, but not one reputation
// does not vouch for while the tried operator still has a vouched endpoint:
// the retry then stays on the healthy operator's other host. With the other
// operator vouched, the preference applies as before.
func TestRetry_OperatorPreferenceNeverNarrowsIntoJunk(t *testing.T) {
	a1 := domain.EndpointAddr("s1-https://h1.alpha.example.com")
	a2 := domain.EndpointAddr("s2-https://h2.alpha.example.com")
	junk := domain.EndpointAddr("s3-https://r1.junk.example.xyz")

	run := func(scores map[domain.EndpointAddr]float64) domain.EndpointAddrList {
		var second domain.EndpointAddrList
		attempt := 0
		h := relay.HandlerFunc(func(ctx *relay.Context) error {
			attempt++
			if attempt == 1 {
				ctx.Endpoint = a1
				return retryableErr("first attempt failed")
			}
			second = append(domain.EndpointAddrList(nil), ctx.Endpoints...)
			ctx.Endpoint = ctx.Endpoints[0]
			return nil
		})
		ctx := baseContext()
		ctx.Endpoints = domain.EndpointAddrList{a1, a2, junk}
		mw := RetryWithRecorder(newFlags("retry", "operator_aware_selection"), retryCfg(1, 0), nil,
			RetryVouchedBy(&stubRepService{scores: scores}))
		if err := mw(h).HandleRelay(ctx); err != nil {
			t.Fatal(err)
		}
		return second
	}

	got := run(map[domain.EndpointAddr]float64{a1: 100, a2: 100})
	if !slices.Contains(got, a2) {
		t.Fatalf("retry pool = %v: the other operator is unvouched, so the vouched host of the tried operator must stay", got)
	}

	got = run(map[domain.EndpointAddr]float64{a1: 100, a2: 100, junk: 100})
	if !slices.Equal(got, domain.EndpointAddrList{junk}) {
		t.Fatalf("retry pool = %v: a vouched other operator must still be preferred", got)
	}
}

// keepPlugin is a qos.Plugin whose filter keeps a fixed list.
type keepPlugin struct {
	normPlugin
	keep domain.EndpointAddrList
}

func (p keepPlugin) SelectEndpoints(domain.EndpointAddrList, []domain.Payload) (domain.EndpointAddrList, error) {
	return p.keep, nil
}

// The QoS filter may not narrow a request into endpoints reputation does not
// vouch for while it removed ones it does (base's archival filter leaving
// only two relay miners that never answer, 2026-09-14): selection falls back
// to the whole pool, degraded. A vouched survivor keeps the filter's answer.
func TestSelectEndpoint_FilterNeverNarrowsIntoJunk(t *testing.T) {
	good := domain.EndpointAddr("s1-https://full.node.example.com")
	junk := domain.EndpointAddr("s2-https://r1.junk.example.xyz")

	pick := func(scores map[domain.EndpointAddr]float64) (domain.EndpointAddr, bool) {
		plugin := keepPlugin{keep: domain.EndpointAddrList{junk}}
		ctx := baseContext()
		ctx.Endpoints = domain.EndpointAddrList{good, junk}
		ctx.Plugin = plugin
		var chosen domain.EndpointAddr
		mw := SelectEndpoint(&stubRepService{scores: scores}, nil, nil, newFlags())
		if err := mw(relay.HandlerFunc(func(c *relay.Context) error { chosen = c.Endpoint; return nil })).HandleRelay(ctx); err != nil {
			t.Fatal(err)
		}
		return chosen, ctx.Degraded
	}

	if ep, degraded := pick(map[domain.EndpointAddr]float64{good: 100}); ep != good || !degraded {
		t.Fatalf("chose %s (degraded %v): the filter left only an unvouched endpoint, want the vouched one, degraded", ep, degraded)
	}
	if ep, degraded := pick(map[domain.EndpointAddr]float64{good: 100, junk: 100}); ep != junk || degraded {
		t.Fatalf("chose %s (degraded %v): a vouched survivor must keep the filter's answer", ep, degraded)
	}
}

// relay_timeout bounds one attempt, so the request deadline is relay_timeout
// × (max_retries + 1); zero stays zero, and no retry leaves it unscaled.
func TestAttemptScaledTimeout(t *testing.T) {
	per := func(domain.ServiceID) time.Duration { return 5 * time.Second }
	retries := func(n int) func(domain.ServiceID) config.RetryConfig {
		return func(domain.ServiceID) config.RetryConfig { return config.RetryConfig{MaxRetries: n} }
	}
	if got := AttemptScaledTimeout(per, retries(1))("sei"); got != 10*time.Second {
		t.Errorf("5s × 2 attempts = %s, want 10s", got)
	}
	if got := AttemptScaledTimeout(per, retries(0))("sei"); got != 5*time.Second {
		t.Errorf("no retry: %s, want 5s", got)
	}
	zero := func(domain.ServiceID) time.Duration { return 0 }
	if got := AttemptScaledTimeout(zero, retries(3))("sei"); got != 0 {
		t.Errorf("no timeout configured: %s, want 0", got)
	}
}
