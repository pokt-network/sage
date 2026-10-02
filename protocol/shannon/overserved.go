package shannon

import (
	"maps"
	"sync"

	"github.com/pokt-network/sage/domain"
)

// overServed holds the suppliers that refused a relay for over-servicing,
// each for the one session it refused in.
//
// An application's stake buys each supplier in a session its own allocation
// of relays; a supplier that has spent it refuses every further relay of that
// session, correctly, and serves again in the next. So the exclusion is by
// supplier address, the only place SAGE limits by one: another supplier
// behind the same URL holds an allocation of its own and keeps serving. And
// it is by session end height, not by time: the next session's endpoints
// carry a later end, so the supplier is back the moment SAGE uses it, and a
// session that ends sooner or later than a fixed timer is followed exactly.
// Until 2026-10-02 the refusal failed response validation and blacklisted the
// supplier for 15 minutes, which is neither.
type overServed struct {
	mu sync.RWMutex
	m  map[overServedKey]struct{}
}

type overServedKey struct {
	serviceID  domain.ServiceID
	supplier   string
	sessionEnd int64
}

func newOverServed() *overServed {
	return &overServed{m: make(map[overServedKey]struct{})}
}

// mark excludes supplier from serviceID for the session ending at
// sessionEnd, and forgets every older session's entry for the service: all
// of a service's sessions end together. It reports whether the entry is new.
func (o *overServed) mark(serviceID domain.ServiceID, supplier string, sessionEnd int64) bool {
	k := overServedKey{serviceID, supplier, sessionEnd}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.m[k]; ok {
		return false
	}
	maps.DeleteFunc(o.m, func(e overServedKey, _ struct{}) bool {
		return e.serviceID == serviceID && e.sessionEnd < sessionEnd
	})
	o.m[k] = struct{}{}
	return true
}

// excluded reports whether supplier refused for over-servicing in the
// session ending at sessionEnd.
func (o *overServed) excluded(serviceID domain.ServiceID, supplier string, sessionEnd int64) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	_, ok := o.m[overServedKey{serviceID, supplier, sessionEnd}]
	return ok
}
