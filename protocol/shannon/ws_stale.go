package shannon

import (
	"context"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
)

// WebSocket staleness: the HTTP path's stale_response, on a connection.
//
// A WebSocket supplier was graded on the shape of its frames alone. On mainnet
// robinhood (2026-10-04) a public checker read a head 3,985 blocks behind from
// a long-lived connection, nodefleet pushed newHeads more than 100 blocks
// behind in bursts of over 100k a half hour, and every WebSocket key still
// scored 95-100: a probe passed on any answer, and the head tracker only
// measured. Behind the stale_response flag, like HTTP:
//
//   - an answer naming the head (eth_blockNumber, getBlockByNumber("latest"),
//     whatever the plugin's HeadLagReader reads) that the head has moved past
//     is ws_stale_response, a major penalty, and the request goes again to the
//     next supplier the way a rate limit does (wsMessageProcessor.reissue);
//   - wsStaleHeadsToRebind newHeads pushes in a row that the head has moved
//     past are ws_stale_head, one major penalty, and the bridge rebinds:
//     notifications cannot be asked again, so the client is moved instead;
//   - a recovery probe whose answer is stale is ws_probe_behind (ws_probe.go).
//
// Stale is the consensus's own judgement (qos.BlockConsensus.AnswerLag): past
// max(2 blocks, 10 seconds of blocks) behind the head projected to now, and
// never while the head's rate is unknown or it has stopped moving.

const (
	reasonWSStaleResponse = "ws_stale_response"
	reasonWSStaleHead     = "ws_stale_head"
)

// wsStaleHeadsToRebind is how many stale newHeads pushes in a row move a
// connection: one late push is propagation, a run of them is a node behind.
const wsStaleHeadsToRebind = 3

// wsStaleness is what a processor needs to grade staleness: the plugin's lag
// readers (either may be nil), the flag, and the supplier's account.
type wsStaleness struct {
	enabled  func() bool
	answers  qos.HeadLagReader
	heights  qos.HeightLagReader
	penalize func(reason string)
}

// staleness builds the check for one supplier on serviceID, or nil when the
// service's plugin measures no head.
func (r *WSRelayer) staleness(serviceID domain.ServiceID, addr domain.EndpointAddr) *wsStaleness {
	if r.deps.QoS == nil {
		return nil
	}
	plugin := r.deps.QoS.Get(serviceID)
	answers, _ := plugin.(qos.HeadLagReader)
	heights, _ := plugin.(qos.HeightLagReader)
	if answers == nil && heights == nil {
		return nil
	}
	return &wsStaleness{
		enabled: func() bool {
			return r.deps.Flags.IsEnabled(context.Background(), featureflag.FlagStaleResponse, serviceID)
		},
		answers: answers,
		heights: heights,
		penalize: func(reason string) {
			_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, addr, domain.RPCTypeWebSocket,
				reputation.NewSignal(reputation.SignalMajorError, reason, 0))
		},
	}
}

// staleAnswer reports whether payload answers a request in flight with a head
// the chain has moved past. Only what the registry tracks in flight is read:
// a rebind's replayed subscribe carries a gateway id the registry keeps in its
// replay table, so its ack is never taken for an answer here.
func (p *wsMessageProcessor) staleAnswer(payload []byte) bool {
	s := p.staleness
	if s == nil || s.answers == nil || !s.enabled() {
		return false
	}
	req := p.subs.InFlight(payload)
	if req == nil {
		return false
	}
	_, stale, ok := s.answers.HeadLag(domain.NewPayload(req, domain.RPCTypeWebSocket, qos.JSONRPCMethod(req)), payload, time.Now())
	return ok && stale
}

// staleHead counts one newHeads push at height and reports whether it ends a
// run of wsStaleHeadsToRebind stale ones on a bridge that can rebind. The run
// is charged once, when it ends. Bridge loop only.
func (p *wsMessageProcessor) staleHead(height uint64) bool {
	s := p.staleness
	if s == nil || s.heights == nil || !s.enabled() {
		return false
	}
	if _, stale, ok := s.heights.HeightLag(height, time.Now()); !ok || !stale {
		p.staleHeads = 0
		return false
	}
	p.staleHeads++
	if p.staleHeads < wsStaleHeadsToRebind {
		return false
	}
	p.staleHeads = 0
	s.penalize(reasonWSStaleHead)
	return p.canRebind != nil && p.canRebind()
}
