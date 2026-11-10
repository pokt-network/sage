package shannon

import (
	"cmp"
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

// A supplier is paid for every notification it pushes, and the stream stops
// only when the supplier is told to stop: a notification the gateway drops
// has already been billed. A connection's notification budget is therefore
// enforced by closing it — the supplier leg first — and a client closed for
// it is refused new connections to the service for a cooldown, on every
// replica, so a reconnect does not start a fresh budget.

// wsBudgetWindow is how long a connection's notifications are averaged over
// before they are held to the budget: long enough for a busy block's burst to
// pass, short enough that a feed far over it is closed within seconds.
const wsBudgetWindow = 20 * time.Second

// DefaultBudgetCooldown is WSRelayerDeps.BudgetCooldown's base, the
// websocket.budget_cooldown knob's.
const DefaultBudgetCooldown = time.Minute

// wsBudget meters the notifications one client connection draws. One per
// connection, shared by every processor it is given (a rebind keeps it), so
// neither a session rollover nor the replay that follows starts it over.
// Nil-safe: a probe has none.
type wsBudget struct {
	// rate is the service's budget in notifications a second, read at the
	// start of each window and again when the count crosses it; 0 is none.
	rate func() int
	// onTrip is called once, with the topic that pushed the most in the
	// window, from the bridge goroutine: it must not block.
	onTrip func(topic string)
	now    func() time.Time

	mu      sync.Mutex
	start   time.Time
	limit   int64
	count   int64
	topics  map[string]int64
	tripped bool
}

// spend counts one notification on topic and reports whether the connection
// is over its budget, from the one that crossed it on: none of those is
// forwarded, since the connection is closing.
func (b *wsBudget) spend(topic string) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	if b.tripped {
		b.mu.Unlock()
		return true
	}
	if now := b.now(); b.start.IsZero() || now.Sub(b.start) >= wsBudgetWindow {
		b.start, b.count = now, 0
		b.limit = int64(b.rate()) * int64(wsBudgetWindow/time.Second)
		clear(b.topics)
	}
	if b.limit <= 0 {
		b.mu.Unlock()
		return false
	}
	b.count++
	if len(topic) > 64 {
		topic = topic[:64]
	}
	if b.topics == nil {
		b.topics = make(map[string]int64, 2)
	}
	b.topics[topicSlot(b.topics, topic)]++
	if b.count <= b.limit {
		b.mu.Unlock()
		return false
	}
	// The crossing re-reads the budget: one turned off or raised since the
	// window started spares the connection now, not at the next window.
	if b.limit = int64(b.rate()) * int64(wsBudgetWindow/time.Second); b.limit <= 0 || b.count <= b.limit {
		b.mu.Unlock()
		return false
	}
	b.tripped = true
	var top string
	for t, n := range b.topics {
		if n > b.topics[top] || (n == b.topics[top] && t < top) {
			top = t
		}
	}
	b.mu.Unlock()
	b.onTrip(top)
	return true
}

// exceeded reports whether the connection went over its budget.
func (b *wsBudget) exceeded() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tripped
}

// notificationBudget is serviceID's budget in notifications a second per
// connection; 0 is none.
func (r *WSRelayer) notificationBudget(serviceID domain.ServiceID) int {
	if r.deps.NotificationBudget == nil {
		return 0
	}
	return r.deps.NotificationBudget(serviceID)
}

// budgetCooldown is how long a client closed over serviceID's budget is
// refused new connections to it.
func (r *WSRelayer) budgetCooldown(serviceID domain.ServiceID) time.Duration {
	if r.deps.BudgetCooldown == nil {
		return 0
	}
	return r.deps.BudgetCooldown(serviceID)
}

// budgetCloseReason is the reason of the 1008 close a connection over its
// budget gets: the service's deny message, or the built-in wording.
func (r *WSRelayer) budgetCloseReason(serviceID domain.ServiceID) string {
	return cutCloseReason(cmp.Or(r.denyMessage(serviceID), "subscription notification rate over this endpoint's limit"))
}

// cooldownKey is the cooldown a budget close holds: one client on one service.
func cooldownKey(serviceID domain.ServiceID, clientIP string) string {
	return string(serviceID) + "|" + clientIP
}

// holdCooldown starts clientIP's cooldown on serviceID after a budget close.
func (r *WSRelayer) holdCooldown(serviceID domain.ServiceID, clientIP string) {
	d := r.budgetCooldown(serviceID)
	if d <= 0 {
		return
	}
	if err := r.cooldowns.Hold(context.Background(), cooldownKey(serviceID, clientIP), d); err != nil {
		r.deps.Logger.Warn("ws budget: cooldown held on this replica only", "service_id", serviceID, "client_ip", clientIP, "err", err)
	}
}

// refuseCooled answers an upgrade from a client still in its cooldown on
// serviceID with a 429 and reports whether it did. Only read while the
// service has a budget: with none, a cooldown left over refuses nothing and
// the upgrade costs no Redis read.
func (r *WSRelayer) refuseCooled(serviceID domain.ServiceID, clientIP string, w http.ResponseWriter) bool {
	if r.notificationBudget(serviceID) <= 0 {
		return false
	}
	left, err := r.cooldowns.Remaining(context.Background(), cooldownKey(serviceID, clientIP))
	if err != nil {
		r.deps.Logger.Warn("ws budget: cooldown read failed, checked on this replica only", "service_id", serviceID, "err", err)
	}
	if left <= 0 {
		return false
	}
	if r.deps.Metrics != nil {
		r.deps.Metrics.Rejected(serviceID, "budget_cooldown")
	}
	w.Header().Set("Retry-After", strconv.Itoa(int((left+time.Second-1)/time.Second)))
	http.Error(w, r.budgetCloseReason(serviceID), http.StatusTooManyRequests)
	return true
}
