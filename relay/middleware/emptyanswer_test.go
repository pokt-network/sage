package middleware

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// emptyPlugin is a qos.Plugin whose qos.EmptyAnswerFilter knows the listed
// hosts answer every request empty, and whose own filter keeps keep when set.
type emptyPlugin struct {
	normPlugin
	empty   map[domain.EndpointAddr]bool
	keep    domain.EndpointAddrList
	counted *[]string
}

func (p emptyPlugin) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	if p.keep != nil {
		return p.keep, nil
	}
	return eps, nil
}

func (p emptyPlugin) WithoutEmptyAnswerers(eps domain.EndpointAddrList, _ []domain.Payload) domain.EndpointAddrList {
	return filterEndpoints(eps, func(ep domain.EndpointAddr) bool { return !p.empty[ep] })
}

func (p emptyPlugin) CountEmptyAnswerers(outcome string) {
	if p.counted != nil {
		*p.counted = append(*p.counted, outcome)
	}
}

func (emptyPlugin) SetEmptyAnswerHooks(func() bool, func(string)) {}

// A host that answers the request empty is out of every selection path:
// the plain pick, the guard against narrowing into unvouched hosts, and the
// degraded fallback. ctx.Endpoints, the pool Retry and Hedge read, is left
// alone.
func TestSelectEndpoint_NeverPicksAnEmptyAnswerer(t *testing.T) {
	empty := domain.EndpointAddr("s1-https://r1.empty.example.net")
	good := domain.EndpointAddr("s2-https://full.node.example.com")
	junk := domain.EndpointAddr("s3-https://r1.junk.example.xyz")
	scores := map[domain.EndpointAddr]float64{empty: 100, good: 100}

	for name, keep := range map[string]domain.EndpointAddrList{
		"plain":        nil,
		"junk guard":   {junk},
		"empty filter": {},
	} {
		t.Run(name, func(t *testing.T) {
			var counted []string
			ctx := baseContext()
			ctx.Endpoints = domain.EndpointAddrList{empty, good, junk}
			ctx.Plugin = emptyPlugin{empty: map[domain.EndpointAddr]bool{empty: true}, keep: keep, counted: &counted}
			var chosen domain.EndpointAddr
			mw := SelectEndpoint(&stubRepService{scores: scores}, nil)
			if err := mw(relay.HandlerFunc(func(c *relay.Context) error { chosen = c.Endpoint; return nil })).HandleRelay(ctx); err != nil {
				t.Fatal(err)
			}
			if chosen == empty {
				t.Fatalf("chose %s, the host that answers empty", chosen)
			}
			if !slices.Equal(ctx.Endpoints, domain.EndpointAddrList{empty, good, junk}) {
				t.Fatalf("ctx.Endpoints = %v, want it unchanged", ctx.Endpoints)
			}
			if !slices.Equal(counted, []string{qos.EmptyKeptOut}) {
				t.Fatalf("counted %v", counted)
			}
		})
	}
}

// With only such hosts left the request still goes to one, counted as a
// fallback; a hedge is not sent at all.
func TestSelectEndpoint_OnlyEmptyAnswerersLeft(t *testing.T) {
	empty := domain.EndpointAddr("s1-https://r1.empty.example.net")
	for _, kind := range []string{"", relay.AttemptRetry, relay.AttemptHedge} {
		var counted []string
		ctx := baseContext()
		ctx.AttemptKind = kind
		ctx.Endpoints = domain.EndpointAddrList{empty}
		ctx.Plugin = emptyPlugin{empty: map[domain.EndpointAddr]bool{empty: true}, counted: &counted}
		var chosen domain.EndpointAddr
		err := SelectEndpoint(&stubRepService{}, nil)(relay.HandlerFunc(func(c *relay.Context) error { chosen = c.Endpoint; return nil })).HandleRelay(ctx)
		if kind == relay.AttemptHedge {
			if !errors.Is(err, errHedgeRuledOut) || chosen != "" || !slices.Equal(counted, []string{qos.EmptyKeptOut}) {
				t.Fatalf("hedge: err %v, chose %q, counted %v; want no hedge, kept_out", err, chosen, counted)
			}
			continue
		}
		if err != nil || chosen != empty || !slices.Equal(counted, []string{qos.EmptyFallback}) {
			t.Fatalf("%q: err %v, chose %q, counted %v; want sent anyway, fallback", kind, err, chosen, counted)
		}
	}
}

// The retry's operator preference yields rather than leave the retry only
// hosts that answer it empty while the tried operator has another host.
func TestRetry_OperatorPreferenceNeverNarrowsIntoEmpty(t *testing.T) {
	a1 := domain.EndpointAddr("s1-https://h1.alpha.example.com")
	a2 := domain.EndpointAddr("s2-https://h2.alpha.example.com")
	empty := domain.EndpointAddr("s3-https://r1.empty.example.net")

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
	ctx.Endpoints = domain.EndpointAddrList{a1, a2, empty}
	ctx.Plugin = emptyPlugin{empty: map[domain.EndpointAddr]bool{empty: true}}
	scores := map[domain.EndpointAddr]float64{a1: 100, a2: 100, empty: 100}
	mw := Retry(newFlags("retry", "operator_aware_selection"), retryCfg(1, 0), nil, RetryOptions{Reputation: &stubRepService{scores: scores}})
	if err := mw(h).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(second, a2) {
		t.Fatalf("retry pool = %v: the other operator answers empty, so the tried operator's other host must stay", second)
	}
}

// The hedge's operator preference yields the same way.
func TestHedge_NeverNarrowsIntoEmpty(t *testing.T) {
	a1 := domain.EndpointAddr("s1-https://h1.alpha.example.com")
	a2 := domain.EndpointAddr("s2-https://h2.alpha.example.com")
	empty := domain.EndpointAddr("s3-https://r1.empty.example.net")

	var mu sync.Mutex
	var picked []domain.EndpointAddr
	slow := relay.HandlerFunc(func(ctx *relay.Context) error {
		ep := ctx.Endpoints[0]
		ctx.Endpoint = ep
		if ctx.SelectedEndpoint != nil {
			ctx.SelectedEndpoint.Store(&ep)
		}
		mu.Lock()
		picked = append(picked, ep)
		mu.Unlock()
		time.Sleep(60 * time.Millisecond)
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	ctx := baseContext()
	ctx.Endpoints = domain.EndpointAddrList{a1, a2, empty}
	ctx.Plugin = emptyPlugin{empty: map[domain.EndpointAddr]bool{empty: true}}
	h := Hedge(newFlags("hedge", "operator_aware_selection"), hedgeCfg(10*time.Millisecond), nil, nil)(slow)
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(picked, []domain.EndpointAddr{a1, a2}) {
		t.Fatalf("picks = %v: the hedge must yield the operator preference and go to the primary operator's other host", picked)
	}
}

// A host that answers empty does not vouch for the operators left: with it
// and an unvouched host the only other choices, the retry stays on the tried
// operator's vouched host rather than land on the unvouched one.
func TestRetry_EmptyAnswererVouchesForNobody(t *testing.T) {
	a1 := domain.EndpointAddr("s1-https://h1.alpha.example.com")
	a2 := domain.EndpointAddr("s2-https://h2.alpha.example.com")
	empty := domain.EndpointAddr("s3-https://r1.empty.example.net")
	junk := domain.EndpointAddr("s4-https://r1.junk.example.xyz")

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
	ctx.Endpoints = domain.EndpointAddrList{a1, a2, empty, junk}
	ctx.Plugin = emptyPlugin{empty: map[domain.EndpointAddr]bool{empty: true}}
	scores := map[domain.EndpointAddr]float64{a1: 100, a2: 100, empty: 100}
	mw := Retry(newFlags("retry", "operator_aware_selection"), retryCfg(1, 0), nil, RetryOptions{Reputation: &stubRepService{scores: scores}})
	if err := mw(h).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(second, a2) {
		t.Fatalf("retry pool = %v: only an empty answerer vouches for the other operators, so the tried operator's other host must stay", second)
	}
}
