package methodblock

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/pokt-network/sage/internal/safego"
)

// Defaults. TTL is short on purpose: a method mark is unverified and cheap to
// be wrong about, and a host re-proves itself with one relay when it lapses.
const (
	DefaultTTL        = 5 * time.Minute
	DefaultEscalation = 3
)

// Block is one active block, for the collector and the admin API. Method is
// "" for a host-level block.
type Block struct {
	Host   string    `json:"host"`
	Method string    `json:"method,omitempty"`
	Expiry time.Time `json:"expiry"`
}

// methodMark is one host × method mark. escalates records whether the
// evidence behind it was supplier-attributed: only those count toward a
// host-level block. A client-attributed mark (a healthy node answering -32601
// to a method it does not implement) still keeps that method away from the
// host, but three of them must never remove the host for everything.
type methodMark struct {
	expiry    time.Time
	escalates bool
}

// hostState is one host's marks within one service.
type hostState struct {
	hostUntil time.Time             // host-level block; zero = none
	methods   map[string]methodMark // method -> mark
}

// Store holds method blocks for every service in the process.
type Store struct {
	ttl           time.Duration
	escalation    int
	logger        *slog.Logger
	sweepInterval time.Duration

	mu        sync.RWMutex
	byService map[string]map[string]*hostState // service -> host -> state
}

// Option configures a Store.
type Option func(*Store)

// WithTTL sets how long a mark lasts. Zero or negative disables marking.
func WithTTL(d time.Duration) Option { return func(s *Store) { s.ttl = d } }

// WithEscalation sets how many distinct methods must be marked on one host
// inside one TTL before the host is blocked for every method. Zero or
// negative never escalates.
func WithEscalation(n int) Option { return func(s *Store) { s.escalation = n } }

// WithLogger sets the logger used by the sweep goroutine.
func WithLogger(l *slog.Logger) Option { return func(s *Store) { s.logger = l } }

// New returns a Store with the defaults, then the options applied.
func New(opts ...Option) *Store {
	s := &Store{
		ttl:           DefaultTTL,
		escalation:    DefaultEscalation,
		logger:        slog.Default(),
		sweepInterval: DefaultTTL,
		byService:     make(map[string]map[string]*hostState),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Blocked reports whether host must not receive method for service right
// now: a live host-level block, or a live mark on exactly this method.
// Called per candidate per attempt, so it takes only the read lock and
// allocates nothing.
func (s *Store) Blocked(service, host, method string) bool {
	if s.ttl <= 0 {
		return false
	}
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := s.byService[service][host]
	if h == nil {
		return false
	}
	if now.Before(h.hostUntil) {
		return true
	}
	return now.Before(h.methods[method].expiry)
}

// Mark records that host could not answer method. It returns true when this
// mark escalated the host to a host-level block. A re-mark refreshes the
// expiry to one TTL from now and never extends past that.
//
// escalates says whether the evidence was supplier-attributed. The method
// mark is recorded either way, but only supplier-attributed marks are counted
// toward the host-level block, and only a supplier-attributed mark can
// trigger one. This is not a detail: -32601 ("method not found") is
// MethodBlocking and AttrClient, so a healthy node without debug_*/trace_*
// answers it to every catalogued method a client cares to ask for. Counting
// those would let any client remove a good host from every method — the exact
// failure this package exists to avoid.
//
// The flag is per method mark and sticky within its live window: a
// client-attributed re-mark does not un-count a supplier-attributed one.
func (s *Store) Mark(service, host, method string, escalates bool) (escalated bool) {
	if s.ttl <= 0 || host == "" || method == "" {
		return false
	}
	now := time.Now()
	expiry := now.Add(s.ttl)

	s.mu.Lock()
	defer s.mu.Unlock()

	hosts := s.byService[service]
	if hosts == nil {
		hosts = make(map[string]*hostState)
		s.byService[service] = hosts
	}
	h := hosts[host]
	if h == nil {
		h = &hostState{methods: make(map[string]methodMark)}
		hosts[host] = h
	}
	if now.Before(h.hostUntil) {
		// Already blocked wholesale; a method mark adds nothing.
		return false
	}
	counts := escalates
	if prev, ok := h.methods[method]; ok && now.Before(prev.expiry) && prev.escalates {
		counts = true
	}
	h.methods[method] = methodMark{expiry: expiry, escalates: counts}

	// Count LIVE distinct methods that carry supplier-attributed evidence.
	// Expired ones are dropped here rather than counted, so a host that was
	// marked on three methods over an hour is not escalated for it.
	live := 0
	for m, mk := range h.methods {
		if !now.Before(mk.expiry) {
			delete(h.methods, m)
			continue
		}
		if mk.escalates {
			live++
		}
	}
	if escalates && s.escalation > 0 && live >= s.escalation {
		h.hostUntil = expiry
		h.methods = make(map[string]methodMark)
		return true
	}
	return false
}

// Clear drops every block for a service and returns how many were live. It
// exists so an operator can undo a false positive; the escalation count goes
// with the marks, so the next mark is a first mark.
func (s *Store) Clear(service string) int {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, h := range s.byService[service] {
		if now.Before(h.hostUntil) {
			n++
		}
		for _, mk := range h.methods {
			if now.Before(mk.expiry) {
				n++
			}
		}
	}
	delete(s.byService, service)
	return n
}

// Active lists the live blocks for a service, host-level ones with an empty
// Method. Read at scrape time and by the admin API; not on the relay path.
func (s *Store) Active(service string) []Block {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Block
	for host, h := range s.byService[service] {
		if now.Before(h.hostUntil) {
			out = append(out, Block{Host: host, Expiry: h.hostUntil})
			continue
		}
		for m, mk := range h.methods {
			if now.Before(mk.expiry) {
				out = append(out, Block{Host: host, Method: m, Expiry: mk.expiry})
			}
		}
	}
	return out
}

// StartSweep runs a background pass every sweep interval (one TTL) that drops
// hosts with no live marks, so the map does not grow with every host that
// was ever slow. Expiry itself is lazy on read; this is only memory hygiene.
func (s *Store) StartSweep(ctx context.Context) {
	safego.GoCtx(ctx, s.logger, "methodblock.sweep", func(ctx context.Context) {
		t := time.NewTicker(s.sweepInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				safego.Run(s.logger, "methodblock.sweep.tick", s.sweep)
			}
		}
	})
}

func (s *Store) sweep() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for service, hosts := range s.byService {
		for host, h := range hosts {
			for m, mk := range h.methods {
				if !now.Before(mk.expiry) {
					delete(h.methods, m)
				}
			}
			if len(h.methods) == 0 && !now.Before(h.hostUntil) {
				delete(hosts, host)
			}
		}
		if len(hosts) == 0 {
			delete(s.byService, service)
		}
	}
}
