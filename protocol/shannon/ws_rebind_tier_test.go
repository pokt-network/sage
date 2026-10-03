package shannon

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/websockets"
)

// tieredRep scores endpoints by operator and selects like the reputation
// service: the best tier present (>= 80, then >= 50, then anything), a
// random pick within it.
type tieredRep struct {
	*spyRepSvc
	score map[string]float64 // operator -> score
}

func (r tieredRep) tier(ep domain.EndpointAddr) int {
	switch s := r.score[ep.Operator()]; {
	case s >= 80:
		return 1
	case s >= 50:
		return 2
	}
	return 3
}

func (r tieredRep) TopTier(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType) domain.EndpointAddrList {
	best := 4
	for _, ep := range eps {
		best = min(best, r.tier(ep))
	}
	var out domain.EndpointAddrList
	for _, ep := range eps {
		if r.tier(ep) == best {
			out = append(out, ep)
		}
	}
	return out
}

func (r tieredRep) SelectSpread(ctx context.Context, svc domain.ServiceID, eps domain.EndpointAddrList, rt domain.RPCType, _ map[domain.EndpointAddr]int) domain.EndpointAddr {
	top := r.TopTier(ctx, svc, eps, rt)
	if len(top) == 0 {
		return ""
	}
	return top[rand.IntN(len(top))]
}

var _ reputation.TopTierer = tieredRep{}

// rebindFixture is a relayer whose "eth" session holds one WebSocket
// supplier per operator host, scored by tieredRep.
func rebindFixture(t *testing.T, hosts map[string]float64, flags map[string]bool) (*WSRelayer, map[string]domain.EndpointAddr) {
	t.Helper()
	session := &sessiontypes.Session{
		SessionId: "s1",
		Header: &sessiontypes.SessionHeader{
			SessionId: "s1", ServiceId: "eth", ApplicationAddress: "pokt1app",
			SessionStartBlockHeight: 100, SessionEndBlockHeight: 110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
	}
	byHost := map[string]domain.EndpointAddr{}
	score := map[string]float64{}
	i := 0
	for host, s := range hosts {
		i++
		supplier := "pokt1supplier" + string(rune('a'+i))
		url := "wss://ws." + host
		session.Suppliers = append(session.Suppliers, &sharedtypes.Supplier{
			OperatorAddress: supplier,
			Services: []*sharedtypes.SupplierServiceConfig{{ServiceId: "eth", Endpoints: []*sharedtypes.SupplierEndpoint{
				{Url: url, RpcType: sharedtypes.RPCType_WEBSOCKET},
			}}},
		})
		ep := domain.EndpointAddr(supplier + "-" + url)
		byHost[host] = ep
		score[ep.Operator()] = s
	}
	fn := &mockRelayFullNode{session: session, app: &apptypes.Application{Address: "pokt1app"}, validateResponse: &servicetypes.RelayResponse{}}
	p := &Protocol{
		fullNode: fn, sessions: newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger()),
		signer: &mockSigner{}, bl: newBlacklist(), ownedApps: map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		metrics: noopSupplierMetrics{}, logger: newTestLogger(),
	}
	p.sessions.latestBlockHeight.Store(105)
	reg := qos.NewRegistry()
	if err := reg.Register("eth", evm.NewPlugin(nil, evm.Config{})); err != nil {
		t.Fatal(err)
	}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: p, Reputation: tieredRep{&spyRepSvc{}, score}, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(flags), Logger: newTestLogger(), QoS: reg,
	})
	return r, byHost
}

// The mainnet robinhood case (2026-10-03): three tier-1 operators and a
// trust-penalised owner at 60, a connection rebinding at every session end.
// tried used to keep every endpoint the connection had been bound to and
// untried was preferred before the tier, so within a few session ends the
// only untried operator left was the owner, and the connection landed there.
// A session-end rebind is no failure; across any number of them the
// connection stays in tier 1.
func TestWSRebind_SessionEndsNeverLeaveTheTopTier(t *testing.T) {
	hosts := map[string]float64{"alpha-example.com": 95, "beta-example.com": 88, "gamma-example.com": 100, "owner-example.com": 60, "delta-example.com": 45}
	for _, capOn := range []bool{false, true} {
		flags := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagOperatorAwareSelection: true, featureflag.FlagWSShareCap: capOn}
		r, byHost := rebindFixture(t, hosts, flags)
		if capOn {
			// One tier-1 operator carries most of the pod's frames: the cap
			// takes it out, and the fallback must stay in tier 1.
			heavy := byHost["gamma-example.com"]
			l := &wsLive{service: "eth", current: &atomic.Pointer[domain.EndpointAddr]{}, proc: &atomic.Pointer[wsMessageProcessor]{}, opened: time.Now().Add(-time.Second)}
			l.current.Store(&heavy)
			l.retired.Store(1000)
			r.live.Store(new(websockets.Bridge), l)
		}
		tried := map[domain.EndpointAddr]bool{}
		current, _, err := r.resolveEndpoint(context.Background(), "eth", tried, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 12; i++ {
			noteRebind(tried, current.addr, websockets.ErrBridgeSessionExpired)
			next, reason, err := r.resolveEndpoint(context.Background(), "eth", tried, nil)
			if err != nil {
				t.Fatalf("rebind %d: %s: %v", i, reason, err)
			}
			if h := next.addr; h == byHost["owner-example.com"] || h == byHost["delta-example.com"] {
				t.Fatalf("share cap %v, session-end rebind %d landed on %s, below tier 1", capOn, i+1, h)
			}
			current = next
		}
	}
}

// Real losses are avoided within the tier, and the next tier is reached only
// once every tier-1 endpoint failed on this connection.
func TestWSRebind_LossesCascadeOnlyWhenTheTierIsSpent(t *testing.T) {
	hosts := map[string]float64{"alpha-example.com": 95, "beta-example.com": 88, "owner-example.com": 60}
	flags := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagOperatorAwareSelection: true}
	r, byHost := rebindFixture(t, hosts, flags)
	tried := map[domain.EndpointAddr]bool{}

	noteRebind(tried, byHost["alpha-example.com"], websockets.ErrBridgeStalled)
	next, _, err := r.resolveEndpoint(context.Background(), "eth", tried, nil)
	if err != nil || next.addr != byHost["beta-example.com"] {
		t.Fatalf("after alpha failed: %v, %v; want the other tier-1 endpoint", next, err)
	}
	noteRebind(tried, byHost["beta-example.com"], websockets.ErrBridgeStalled)
	next, _, err = r.resolveEndpoint(context.Background(), "eth", tried, nil)
	if err != nil || next.addr != byHost["owner-example.com"] {
		t.Fatalf("after every tier-1 endpoint failed: %v, %v; want the next tier", next, err)
	}
	noteRebind(tried, next.addr, websockets.ErrBridgeSessionExpired)
	if len(tried) != 0 {
		t.Fatalf("a session-end rebind left tried = %v; want it cleared", tried)
	}
}
