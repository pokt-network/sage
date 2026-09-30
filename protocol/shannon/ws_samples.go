package shannon

import (
	"sort"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// WebSocket notification samples.
//
// Every notification a supplier pushes is a relay it is paid for, and the push
// rate is the supplier's choice. From inside the gateway a busy feed and an
// inflated one look alike: both are distinct, well-formed frames for an open
// subscription. On mainnet gnosis (2026-09-28) one client received ~136
// notifications a second from one owner and ~6.5 from another operator for the
// same replayed subscriptions, and nothing SAGE counts can say which one is
// telling the truth. The chain can: a pending transaction that was real is
// mined or at least known to a node, a log belongs to a transaction receipt, a
// head is a block. So a sample of what each supplier pushed — the hash that
// names it — is kept for an offline check against the chain, without SAGE
// spending a relay on it.
//
// One notification in wsSampleEvery per (service, operator, owner, topic),
// the last wsSampleKeep of each. Per pod; nothing is persisted.
const (
	wsSampleEvery = 100
	wsSampleKeep  = 200
	// wsSampleMaxKeys bounds the table: services × operators × owners ×
	// topics is hundreds on mainnet. A new key past it resets the table.
	wsSampleMaxKeys = 2048
)

// WSNotificationSample is one sampled notification.
type WSNotificationSample struct {
	ServiceID string `json:"service_id"`
	Operator  string `json:"operator"`
	Owner     string `json:"owner"`
	Topic     string `json:"topic"`
	// Kind says what Hash names: "tx" (a pending transaction's hash, or the
	// transaction a log belongs to) or "block" (a new head).
	Kind string    `json:"kind"`
	Hash string    `json:"hash"`
	At   time.Time `json:"at"`
}

type wsSampleKey struct{ service, operator, owner, topic string }

type wsSampleRing struct {
	seen uint64
	buf  []WSNotificationSample
	next int
}

// wsNotificationSamples keeps the samples.
type wsNotificationSamples struct {
	mu    sync.Mutex
	rings map[wsSampleKey]*wsSampleRing
}

func newWSNotificationSamples() *wsNotificationSamples {
	return &wsNotificationSamples{rings: make(map[wsSampleKey]*wsSampleRing)}
}

// observe counts one graded notification and keeps every wsSampleEvery-th
// one's hash. A frame with no recognisable hash is counted and not kept.
func (s *wsNotificationSamples) observe(serviceID domain.ServiceID, operator, owner, topic string, payload []byte) {
	if s == nil || topic == "" {
		return
	}
	k := wsSampleKey{string(serviceID), operator, owner, topic}
	s.mu.Lock()
	r := s.rings[k]
	if r == nil {
		if len(s.rings) >= wsSampleMaxKeys {
			clear(s.rings) // Start over rather than never sample a key seen after the table filled.
		}
		r = &wsSampleRing{}
		s.rings[k] = r
	}
	r.seen++
	take := r.seen%wsSampleEvery == 1
	s.mu.Unlock()
	if !take {
		return
	}
	kind, hash := notificationHash(payload)
	if hash == "" {
		return
	}
	sample := WSNotificationSample{
		ServiceID: k.service, Operator: operator, Owner: owner, Topic: topic,
		Kind: kind, Hash: hash, At: time.Now().UTC(),
	}
	s.mu.Lock()
	if len(r.buf) < wsSampleKeep {
		r.buf = append(r.buf, sample)
	} else {
		r.buf[r.next] = sample
		r.next = (r.next + 1) % wsSampleKeep
	}
	s.mu.Unlock()
}

// notificationHash names what a subscription notification is about: the
// result of eth_subscription is a transaction hash (newPendingTransactions),
// a full transaction, a log, or a block header.
func notificationHash(payload []byte) (kind, hash string) {
	res := gjson.GetBytes(payload, "params.result")
	switch {
	case res.Type == gjson.String:
		return "tx", res.String()
	case res.Get("transactionHash").Exists():
		return "tx", res.Get("transactionHash").String()
	case res.Get("parentHash").Exists():
		return "block", res.Get("hash").String()
	case res.Get("hash").Exists():
		return "tx", res.Get("hash").String()
	}
	return "", ""
}

// snapshot returns the samples, optionally for one service, oldest first
// within each key and keys in a stable order.
func (s *wsNotificationSamples) snapshot(serviceID domain.ServiceID) []WSNotificationSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]wsSampleKey, 0, len(s.rings))
	for k := range s.rings {
		if serviceID == "" || k.service == string(serviceID) {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.service != b.service {
			return a.service < b.service
		}
		if a.operator != b.operator {
			return a.operator < b.operator
		}
		if a.owner != b.owner {
			return a.owner < b.owner
		}
		return a.topic < b.topic
	})
	out := []WSNotificationSample{}
	for _, k := range keys {
		r := s.rings[k]
		out = append(out, r.buf[r.next:]...)
		out = append(out, r.buf[:r.next]...)
	}
	return out
}
