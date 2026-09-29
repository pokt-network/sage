package shannon

import (
	"context"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
)

// vouchSet is a spyRepSvc whose Vouched answers from a set.
type vouchSet struct {
	spyRepSvc
	vouched map[domain.EndpointAddr]bool
}

func (v *vouchSet) Vouched(_ context.Context, _ domain.ServiceID, ep domain.EndpointAddr, _ domain.RPCType) bool {
	return v.vouched[ep]
}

const (
	heavyA = domain.EndpointAddr("a1-https://r1.heavy.xyz")
	heavyB = domain.EndpointAddr("a2-https://r2.heavy.xyz")
	lightA = domain.EndpointAddr("b1-https://n1.light.net")
	thirdA = domain.EndpointAddr("c1-https://m1.third.io")
)

// staleHosts is a qos.Plugin whose StaleChecker calls the listed endpoints
// far behind the head.
type staleHosts struct {
	stale map[domain.EndpointAddr]bool
}

func (staleHosts) ParseRequest(context.Context, *http.Request, []byte, domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}
func (staleHosts) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}

func (s staleHosts) AllStale(eps domain.EndpointAddrList) bool {
	for _, ep := range eps {
		if !s.stale[ep] {
			return false
		}
	}
	return len(eps) > 0
}

func shareRelayer(t *testing.T, on bool, vouched ...domain.EndpointAddr) (*WSRelayer, *spyWSMetrics) {
	t.Helper()
	flags := featureflag.NewMemoryStore(map[string]bool{featureflag.FlagWSShareCap: on})
	v := &vouchSet{vouched: map[domain.EndpointAddr]bool{}}
	for _, ep := range vouched {
		v.vouched[ep] = true
	}
	spy := &spyWSMetrics{}
	return &WSRelayer{deps: WSRelayerDeps{Flags: flags, Reputation: v, Metrics: spy}}, spy
}

// addLive registers a bridge on ep carrying frames over the last 100s.
func addLive(r *WSRelayer, ep domain.EndpointAddr, frames int64) *wsLive {
	var cur atomic.Pointer[domain.EndpointAddr]
	cur.Store(&ep)
	l := &wsLive{service: "base", current: &cur, proc: &atomic.Pointer[wsMessageProcessor]{}, opened: time.Now().Add(-100 * time.Second)}
	l.retired.Store(frames)
	r.live.Store(l, l)
	return l
}

func TestCapShare(t *testing.T) {
	pool := domain.EndpointAddrList{heavyA, heavyB, lightA}
	ctx := context.Background()

	t.Run("a new connection avoids a party over half", func(t *testing.T) {
		r, spy := shareRelayer(t, true, heavyA, heavyB, lightA)
		addLive(r, heavyA, 6000)
		addLive(r, lightA, 3000)
		if got := r.capShare(ctx, "base", pool, nil); !slices.Equal(got, domain.EndpointAddrList{lightA}) {
			t.Fatalf("got %v, want only the light party", got)
		}
		if !slices.Equal(spy.shareCaps, []string{"bound"}) {
			t.Errorf("outcomes %v, want [bound]", spy.shareCaps)
		}
	})
	t.Run("a party all far behind the head is not an alternative", func(t *testing.T) {
		r, spy := shareRelayer(t, true, heavyA, heavyB, lightA)
		reg := qos.NewRegistry()
		if err := reg.Register("base", staleHosts{stale: map[domain.EndpointAddr]bool{lightA: true}}); err != nil {
			t.Fatal(err)
		}
		r.deps.QoS = reg
		addLive(r, heavyA, 6000)
		addLive(r, lightA, 3000)
		if got := r.capShare(ctx, "base", pool, nil); !slices.Equal(got, pool) {
			t.Fatalf("got %v, want the pool: the only other party is stale", got)
		}
		if !slices.Equal(spy.shareCaps, []string{"open"}) {
			t.Errorf("outcomes %v, want [open]", spy.shareCaps)
		}
	})
	t.Run("flag off: unchanged", func(t *testing.T) {
		r, _ := shareRelayer(t, false, heavyA, heavyB, lightA)
		addLive(r, heavyA, 6000)
		addLive(r, lightA, 3000)
		if got := r.capShare(ctx, "base", pool, nil); !slices.Equal(got, pool) {
			t.Fatalf("got %v, want the pool", got)
		}
	})
	t.Run("one vouched party: fails open", func(t *testing.T) {
		r, _ := shareRelayer(t, true, heavyA, heavyB)
		addLive(r, heavyA, 6000)
		addLive(r, lightA, 3000)
		if got := r.capShare(ctx, "base", pool, nil); !slices.Equal(got, pool) {
			t.Fatalf("got %v, want the pool", got)
		}
	})
	t.Run("a rebind counts itself only where it lands", func(t *testing.T) {
		r, _ := shareRelayer(t, true, heavyA, heavyB, lightA, thirdA)
		self := addLive(r, heavyA, 2000)
		addLive(r, heavyB, 3000)
		addLive(r, lightA, 2000)
		addLive(r, thirdA, 2000)
		// Without self the heavy party holds 3000 of 9000; with it back,
		// 5000: over half. The light and third parties take it to 4000: under.
		got := r.capShare(ctx, "base", domain.EndpointAddrList{heavyA, heavyB, lightA, thirdA}, self)
		slices.Sort(got)
		want := domain.EndpointAddrList{lightA, thirdA}
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("a connection heavier than half goes where selection sends it", func(t *testing.T) {
		r, _ := shareRelayer(t, true, heavyA, heavyB, lightA)
		self := addLive(r, heavyA, 8000)
		addLive(r, lightA, 1000)
		if got := r.capShare(ctx, "base", pool, self); !slices.Equal(got, pool) {
			t.Fatalf("got %v, want the pool", got)
		}
	})
	t.Run("no traffic yet: unchanged", func(t *testing.T) {
		r, _ := shareRelayer(t, true, heavyA, heavyB, lightA)
		if got := r.capShare(ctx, "base", pool, nil); !slices.Equal(got, pool) {
			t.Fatalf("got %v, want the pool", got)
		}
	})
}
