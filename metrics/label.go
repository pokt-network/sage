package metrics

import (
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/pokt-network/sage/domain"
)

// A Prometheus label value must come from a bounded set of valid UTF-8, or it
// is a memory leak with a network interface. SAGE takes label values from three
// places, and they fail differently:
//
//   - Values SAGE writes itself (a retry reason, a degradation tier). Closed
//     sets, safe by construction.
//   - Values from configuration (service IDs). Bounded, but the *request*
//     supplies the ID and Validate deliberately passes unknown ones through, so
//     what arrives is attacker-chosen and only the configured set is real.
//   - Values from someone else's software (a relay miner's error codespace).
//     Free-form strings from the network: cardinality is whatever the sender
//     decides.
//
// This file used to answer each of those separately — sanitizeLabel,
// serviceLabel, boundedLabel — each added by a different incident, each
// correct, none aware of the others, and an author adding a metric had to know
// which of the three applied. They are now one type with three constructors, so
// the question is "which policy", not "which mechanism", and sanitizing is not
// something a call site can forget: every policy does it.

const (
	// maxLabelLen bounds every externally-derived label value. Service IDs and
	// endpoint addresses are short in practice; the cap is a defensive ceiling,
	// not an expected length.
	maxLabelLen = 128

	// maxCodespaceLabels caps the distinct codespace values the relay miner
	// error counter will admit. Registered poktroll codespaces number in the
	// single digits; the cap is headroom, not an expected count.
	maxCodespaceLabels = 32

	// unknownLabel replaces a value outside the allowed set, and otherLabel one
	// beyond a cap. Both collapse rather than drop: the traffic stays visible,
	// and a rising __unknown__ or __other__ series is itself worth an alert.
	unknownLabel = "__unknown__"
	otherLabel   = "__other__"
)

// sanitizeLabel makes an externally-derived string safe to pass to
// client_golang, which panics inside WithLabelValues on a value that is not
// valid UTF-8. SAGE copies the attacker-controlled Target-Service-Id header
// verbatim into ctx.ServiceID (see relay/middleware/parse.go), so an invalid
// byte sequence or an embedded NUL would otherwise crash the request goroutine.
//
// Bound length first (byte-level truncation can split a multibyte rune), then
// replace any invalid sequence. Applied by every labelPolicy — it is the floor,
// not a choice.
func sanitizeLabel(s string) string {
	if len(s) > maxLabelLen {
		s = s[:maxLabelLen]
	}
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	return s
}

// labelPolicy decides which values of one label get their own time series.
//
// The zero value is usable and means "sanitize only" — correct for values SAGE
// writes itself, and the honest default for a label whose author has not
// thought about cardinality, since it still cannot panic the collector.
type labelPolicy struct {
	// allowed, when non-nil, is the only admitted set. Anything else becomes
	// unknownLabel.
	allowed map[string]struct{}

	// max, when > 0, admits the first max distinct values and collapses the
	// rest to otherLabel.
	max  int
	mu   sync.RWMutex
	seen map[string]struct{}
}

// openLabel sanitizes and admits everything. For values from SAGE's own closed
// sets.
func openLabel() *labelPolicy { return &labelPolicy{} }

// allowedLabel admits only the given values. For a label whose real set is
// known at startup — configured service IDs — where anything else is a client
// making one up.
func allowedLabel[T ~string](values []T) *labelPolicy {
	allowed := make(map[string]struct{}, len(values))
	for _, v := range values {
		allowed[string(v)] = struct{}{}
	}
	return &labelPolicy{allowed: allowed}
}

// cappedLabel admits the first max distinct values on a first-seen basis. For
// values written by someone else's software, where the real set is unknowable
// but small. Arrival order decides which values win, which suits the intended
// use: the codespaces that matter appear in the first seconds of traffic, well
// inside the cap.
func cappedLabel(max int) *labelPolicy {
	return &labelPolicy{max: max, seen: make(map[string]struct{}, max)}
}

// value returns the label to record for v.
func (p *labelPolicy) value(v string) string {
	v = sanitizeLabel(v)

	if p.allowed != nil {
		if _, ok := p.allowed[v]; !ok {
			return unknownLabel
		}
		return v
	}

	if p.max <= 0 {
		return v
	}

	p.mu.RLock()
	_, ok := p.seen[v]
	full := len(p.seen) >= p.max
	p.mu.RUnlock()

	if ok {
		return v
	}
	if full {
		return otherLabel
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// Re-check under the write lock: another goroutine may have taken the last
	// slot, or added this same value, between the two critical sections.
	if _, ok := p.seen[v]; !ok {
		if len(p.seen) >= p.max {
			return otherLabel
		}
		p.seen[v] = struct{}{}
	}
	return v
}

// serviceValue is value for a domain.ServiceID, which is the label almost every
// metric here carries.
func (p *labelPolicy) serviceValue(id domain.ServiceID) string {
	return p.value(string(id))
}

// values returns the admitted set, sorted, or nil for a policy that admits
// everything.
//
// It exists for a gauge that has to publish a zero for a service it did NOT
// hear about this round — where absence and zero mean different things and the
// caller cannot enumerate the services itself. A capped policy has no such set
// to return: its membership is decided by arrival order, so there is nothing
// it could honestly list.
func (p *labelPolicy) values() []string {
	if p.allowed == nil {
		return nil
	}
	out := make([]string, 0, len(p.allowed))
	for v := range p.allowed {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
