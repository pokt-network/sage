package shannon

import (
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// WebSocket head signals.
//
// A newHeads feed is the one subscription whose content the gateway can check
// without a relay of its own: every operator pushes the same block, so the
// operators check each other. Per block, the tracker keeps when this pod first
// saw it and which operator pushed which hash, and reports for each operator's
// first push of a block:
//
//   - lag: blocks behind the head this pod knows. That head is the higher of
//     the service's block consensus and the newest head any supplier pushed:
//     the consensus is refreshed on a probe cycle and trails a live feed, so
//     on its own it would read every current supplier as ahead.
//   - delay: time since the first operator pushed the same block. Only when
//     two or more operators pushed it — an operator alone on a service has no
//     one to be compared with.
//   - mismatch: judged wsHeadFinalizeDepth blocks later, when the hash with
//     the most operators behind it is the block's hash and every operator that
//     pushed another one is counted. Judged by majority rather than by who was
//     first, so one wrong first push does not blame everyone after it.
//
// Per pod; nothing is persisted.
const (
	wsHeadFinalizeDepth = 8
	wsHeadKeepDepth     = 64
)

type wsHeadPusher struct{ operator, owner string }

type wsHeadBlock struct {
	firstAt time.Time
	// order is the hashes in first-pushed order, for a stable tie-break.
	order  []string
	byHash map[string][]wsHeadPusher
	judged bool
}

type wsServiceHeads struct {
	max    uint64
	blocks map[uint64]*wsHeadBlock
}

// wsHeadTracker holds the recent heads of every service.
// ponytail: one mutex for all services; per-service locks if WS head volume
// ever makes it show in a profile.
type wsHeadTracker struct {
	mu       sync.Mutex
	services map[domain.ServiceID]*wsServiceHeads
}

func newWSHeadTracker() *wsHeadTracker {
	return &wsHeadTracker{services: make(map[domain.ServiceID]*wsServiceHeads)}
}

// wsHeadReading is what one push says about its operator.
type wsHeadReading struct {
	lag        uint64
	delay      time.Duration
	delayKnown bool
	// mismatched lists the pushers judged wrong on blocks this push
	// finalized, whoever they are.
	mismatched []wsHeadPusher
}

// observe records one head push. ok is false for an operator's repeat of a
// block it already pushed (the same header to another client), which says
// nothing new.
func (t *wsHeadTracker) observe(serviceID domain.ServiceID, p wsHeadPusher, number uint64, hash string, now time.Time, consensus uint64) (r wsHeadReading, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// A head far past the service's consensus is not a reading. Nothing
	// lowers the newest head once one push raises it, so one supplier's huge
	// block number would read every honest push as far behind and forget
	// every held block before it was judged: the check switched off by the
	// party it judges.
	if consensus > 0 && number > consensus+wsHeadKeepDepth {
		return wsHeadReading{}, false
	}
	s := t.services[serviceID]
	if s == nil {
		s = &wsServiceHeads{blocks: make(map[uint64]*wsHeadBlock)}
		t.services[serviceID] = s
	}
	b := s.blocks[number]
	if b == nil {
		if s.max > wsHeadKeepDepth && number < s.max-wsHeadKeepDepth {
			// Too old to hold; the lag is still a reading.
			return wsHeadReading{lag: s.max - number}, true
		}
		b = &wsHeadBlock{firstAt: now, byHash: make(map[string][]wsHeadPusher, 1)}
		s.blocks[number] = b
	}
	for _, pushers := range b.byHash {
		for _, q := range pushers {
			if q.operator == p.operator {
				return wsHeadReading{}, false
			}
		}
	}
	if _, seen := b.byHash[hash]; !seen {
		b.order = append(b.order, hash)
	}
	b.byHash[hash] = append(b.byHash[hash], p)

	advanced := number > s.max
	if advanced {
		s.max = number
	}
	head := max(s.max, consensus)
	if head > number {
		r.lag = head - number
	}
	if pushers := operatorsOn(b); pushers > 1 {
		r.delay, r.delayKnown = now.Sub(b.firstAt), true
	}
	if advanced {
		r.mismatched = s.settle()
	}
	return r, true
}

func operatorsOn(b *wsHeadBlock) int {
	n := 0
	for _, pushers := range b.byHash {
		n += len(pushers)
	}
	return n
}

// settle judges the blocks now wsHeadFinalizeDepth behind the newest and
// forgets those past wsHeadKeepDepth. Caller holds mu.
func (s *wsServiceHeads) settle() []wsHeadPusher {
	var wrong []wsHeadPusher
	for n, b := range s.blocks {
		if s.max >= wsHeadKeepDepth && n < s.max-wsHeadKeepDepth {
			delete(s.blocks, n)
			continue
		}
		if b.judged || n+wsHeadFinalizeDepth > s.max {
			continue
		}
		b.judged = true
		if len(b.order) < 2 {
			continue
		}
		winner := b.order[0]
		for _, h := range b.order[1:] {
			if len(b.byHash[h]) > len(b.byHash[winner]) {
				winner = h
			}
		}
		for _, h := range b.order {
			if h != winner {
				wrong = append(wrong, b.byHash[h]...)
			}
		}
	}
	return wrong
}

// observeHead feeds one newHeads notification to the tracker and the
// metrics. The header is params.result: number and hash.
func (p *wsMessageProcessor) observeHead(serviceID domain.ServiceID, payload []byte) {
	if p.heads == nil || p.metrics == nil {
		return
	}
	res := gjson.GetBytes(payload, "params.result")
	number, ok := hexUint(res.Get("number"))
	hash := res.Get("hash").String()
	if !ok || hash == "" {
		return
	}
	var consensus uint64
	if p.consensusHead != nil {
		consensus = p.consensusHead()
	}
	r, fresh := p.heads.observe(serviceID, wsHeadPusher{p.operator, p.owner}, number, hash, time.Now(), consensus)
	if !fresh {
		return
	}
	p.metrics.SupplierHead(serviceID, p.operator, p.owner, r.lag, r.delay, r.delayKnown)
	for _, w := range r.mismatched {
		p.metrics.SupplierHeadMismatch(serviceID, w.operator, w.owner)
	}
}
