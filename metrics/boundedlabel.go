package metrics

import "sync"

const (
	// maxCodespaceLabels caps the distinct codespace values the relay miner
	// error counter will admit. Registered poktroll codespaces number in the
	// single digits; the cap is headroom, not an expected count.
	maxCodespaceLabels = 32

	// otherLabel replaces any value beyond a boundedLabel's capacity. As with
	// unknownServiceLabel, collapsing keeps the traffic visible instead of
	// dropping it — a rising __other__ series means something is emitting
	// values we do not recognize, which is worth looking at.
	otherLabel = "__other__"
)

// boundedLabel caps how many distinct values a label may take.
//
// It exists for label values written by someone else's software. A relay
// miner's error codespace is a free-form string arriving over the network from
// a supplier: cardinality is whatever the sender decides, and every new value
// mints a time series that lives until restart. sanitizeLabel makes such a
// value safe to pass to client_golang; it does nothing about how many of them
// there are. This is the second half of that problem.
//
// The first n distinct values seen keep their own series. Everything after
// collapses to otherLabel. Which values win is arrival order, which is fine for
// the intended use: the codespaces that matter show up in the first seconds of
// traffic, and the cap is far above their number.
type boundedLabel struct {
	mu   sync.RWMutex
	max  int
	seen map[string]struct{}
}

// newBoundedLabel returns a boundedLabel admitting at most max distinct values.
func newBoundedLabel(max int) *boundedLabel {
	return &boundedLabel{max: max, seen: make(map[string]struct{}, max)}
}

// value returns v sanitized if it is admitted, or otherLabel if the cap is
// already spent on other values.
func (b *boundedLabel) value(v string) string {
	v = sanitizeLabel(v)

	b.mu.RLock()
	_, ok := b.seen[v]
	full := len(b.seen) >= b.max
	b.mu.RUnlock()

	if ok {
		return v
	}
	if full {
		return otherLabel
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// Re-check under the write lock: another goroutine may have taken the last
	// slot, or added this same value, between the two critical sections.
	if _, ok := b.seen[v]; !ok {
		if len(b.seen) >= b.max {
			return otherLabel
		}
		b.seen[v] = struct{}{}
	}
	return v
}
