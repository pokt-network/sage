package shannon

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pokt-network/sage/domain"
)

// sessionLoad counts the relays this replica sent each supplier in each
// service's current session: an HTTP relay, a WebSocket answer or
// notification. Every supplier in a session holds the same allocation, so
// when one refuses for over-servicing, what it took against its session
// peers says how plausible the refusal is (sage_over_served_load_ratio).
type sessionLoad struct {
	mu sync.RWMutex
	m  map[domain.ServiceID]*sessionCounts
}

// sessionCounts is one service's counts for the session ending at end.
type sessionCounts struct {
	end int64
	n   map[string]*atomic.Int64
}

func newSessionLoad() *sessionLoad {
	return &sessionLoad{m: make(map[domain.ServiceID]*sessionCounts)}
}

// add counts one relay to supplier in the session ending at end. A relay for
// a session older than the one held is dropped; a newer session replaces it.
func (l *sessionLoad) add(serviceID domain.ServiceID, supplier string, end int64) {
	if l == nil || supplier == "" {
		return
	}
	l.mu.RLock()
	c := l.m[serviceID]
	var n *atomic.Int64
	if c != nil && c.end == end {
		n = c.n[supplier]
	}
	l.mu.RUnlock()
	if n == nil {
		l.mu.Lock()
		c = l.m[serviceID]
		switch {
		case c == nil || end > c.end:
			c = &sessionCounts{end: end, n: make(map[string]*atomic.Int64)}
			l.m[serviceID] = c
		case end < c.end:
			l.mu.Unlock()
			return
		}
		if n = c.n[supplier]; n == nil {
			n = new(atomic.Int64)
			c.n[supplier] = n
		}
		l.mu.Unlock()
	}
	n.Add(1)
}

// ratio is supplier's relays in the session ending at end over the median of
// its session peers that took any; ok is false with fewer than three peers or
// none for that session.
func (l *sessionLoad) ratio(serviceID domain.ServiceID, supplier string, end int64) (float64, bool) {
	if l == nil {
		return 0, false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	c := l.m[serviceID]
	if c == nil || c.end != end {
		return 0, false
	}
	var peers []int64
	for s, n := range c.n {
		if s != supplier {
			if v := n.Load(); v > 0 {
				peers = append(peers, v)
			}
		}
	}
	if len(peers) < 3 {
		return 0, false
	}
	slices.Sort(peers)
	median := peers[len(peers)/2]
	var mine int64
	if n := c.n[supplier]; n != nil {
		mine = n.Load()
	}
	return float64(mine) / float64(median), true
}
