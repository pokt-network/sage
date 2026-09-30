package shannon

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
)

// wsShareCap is the largest share of a service's WebSocket traffic on a pod
// one party may be placed into while another vouched party is available.
const wsShareCap = 0.5

// wsLive is one open bridge as the share cap sees it: which service, which
// supplier it is on now, and how much traffic it carries.
type wsLive struct {
	service domain.ServiceID
	current *atomic.Pointer[domain.EndpointAddr]
	proc    *atomic.Pointer[wsMessageProcessor]
	opened  time.Time
	// retired counts the frames of the suppliers this bridge has left.
	retired atomic.Int64
}

// rate is the bridge's supplier frames a second over its life. A heavy
// subscriber is heavy for as long as it is connected, so the lifetime mean
// ranks it without a decay constant to tune.
func (l *wsLive) rate(now time.Time) float64 {
	frames := l.retired.Load()
	if p := l.proc.Load(); p != nil {
		frames += p.endpointFrames.Load()
	}
	return float64(frames) / max(now.Sub(l.opened).Seconds(), 1)
}

// capShare narrows WebSocket candidates to the parties that stay at or under
// wsShareCap of the service's traffic on this pod once this connection's own
// traffic is placed with them (featureflag.FlagWSShareCap). self is the
// connection being placed, nil for a new one, whose traffic is not known yet.
//
// It binds only when it can: at least two parties with a vouched candidate
// not all known to be far behind the head, and at least one of them under
// the cap with this connection added. A
// single connection heavier than half the service cannot be split, so it
// goes where selection would have sent it, and the cap keeps everything else
// off that party instead. The party a connection currently uses counts only
// the other connections on it, so a rebind can land back where it was.
func (r *WSRelayer) capShare(ctx context.Context, serviceID domain.ServiceID, candidates domain.EndpointAddrList, self *wsLive) domain.EndpointAddrList {
	if !r.deps.Flags.IsEnabled(ctx, featureflag.FlagWSShareCap, serviceID) {
		return candidates
	}
	byParty := map[string]domain.EndpointAddrList{}
	for _, ep := range candidates {
		if r.deps.Reputation.Vouched(ctx, serviceID, ep, domain.RPCTypeWebSocket) {
			byParty[ep.Party()] = append(byParty[ep.Party()], ep)
		}
	}
	// A party whose every endpoint is known to be far behind the head is no
	// place to send traffic, whatever its share: freshest has already let it
	// through only if nothing better was left (qos.StaleChecker).
	if r.deps.QoS != nil {
		if stale, ok := r.deps.QoS.Get(serviceID).(qos.StaleChecker); ok {
			for party, eps := range byParty {
				if stale.AllStale(eps) {
					delete(byParty, party)
				}
			}
		}
	}
	if len(byParty) < 2 {
		r.shareCapOutcome(serviceID, "open")
		return candidates
	}
	now := time.Now()
	load := map[string]float64{}
	var total, own float64
	r.live.Range(func(_, v any) bool {
		l := v.(*wsLive)
		if l.service != serviceID {
			return true
		}
		w := l.rate(now)
		if l == self {
			own = w
		} else if cur := l.current.Load(); cur != nil {
			load[cur.Party()] += w
		}
		total += w
		return true
	})
	if total == 0 {
		r.shareCapOutcome(serviceID, "open")
		return candidates
	}
	var allowed domain.EndpointAddrList
	bound := false
	for party, eps := range byParty {
		if (load[party]+own)/total <= wsShareCap {
			allowed = append(allowed, eps...)
		} else {
			bound = true
		}
	}
	switch {
	case len(allowed) == 0:
		r.shareCapOutcome(serviceID, "open")
		return candidates
	case bound:
		r.shareCapOutcome(serviceID, "bound")
	default:
		r.shareCapOutcome(serviceID, "clear")
	}
	return allowed
}

// shareCapOutcome counts one share-cap decision, when metrics are wired.
func (r *WSRelayer) shareCapOutcome(serviceID domain.ServiceID, outcome string) {
	if r.deps.Metrics != nil {
		r.deps.Metrics.ShareCap(serviceID, outcome)
	}
}
