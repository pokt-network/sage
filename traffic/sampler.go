package traffic

import (
	"bytes"
	"encoding/json"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

const (
	defaultRate            = 100
	defaultWindow          = 5 * time.Minute
	defaultMaxFingerprints = 1000

	// sampleBytes is the amount of a sampled payload kept as its example, per
	// fingerprint — enough to recognize the request shape in the admin JSON,
	// not the whole body.
	sampleBytes = 200

	// hashBytes caps how much of a non-JSON-RPC body is fed to the
	// fingerprint hash. A REST or gRPC body has no "params" member to reduce
	// it to, so the whole body would otherwise be compacted and hashed — work
	// proportional to a payload an untrusted client chooses the size of, on a
	// path that is supposed to cost almost nothing.
	//
	// The documented consequence: two non-JSON-RPC requests identical in
	// their first 4 KiB share one fingerprint however they differ after it.
	// That understates diversity for a client that varies only a deep tail,
	// which is the safe direction — the gauge exists to spot traffic that is
	// suspiciously repetitive, and the failure mode is looking repetitive,
	// not looking diverse.
	hashBytes = 4 << 10
)

// Option configures a Sampler built by New.
type Option func(*Sampler)

// WithRate sets the 1-in-N sampling rate. n <= 0 is ignored, leaving the
// default (or a prior WithRate) in place.
func WithRate(n int) Option {
	return func(s *Sampler) {
		if n > 0 {
			s.rate = n
		}
	}
}

// WithWindow sets the fixed window length. d <= 0 is ignored.
func WithWindow(d time.Duration) Option {
	return func(s *Sampler) {
		if d > 0 {
			s.window = d
		}
	}
}

// WithMaxFingerprints sets the maximum number of distinct fingerprints kept
// per service per window. n <= 0 is ignored.
func WithMaxFingerprints(n int) Option {
	return func(s *Sampler) {
		if n > 0 {
			s.maxFingerprints = n
		}
	}
}

// Sampler tracks request-shape diversity per service. See the package doc for
// the fingerprint, window and sampling rules. The zero value is not usable;
// construct with New.
type Sampler struct {
	rate            int
	window          time.Duration
	maxFingerprints int

	mu       sync.RWMutex
	services map[domain.ServiceID]*serviceState
}

// New creates a Sampler with defaults (rate 100, a 5-minute window,
// 1000 fingerprints per service per window), overridden by opts.
func New(opts ...Option) *Sampler {
	s := &Sampler{
		rate:            defaultRate,
		window:          defaultWindow,
		maxFingerprints: defaultMaxFingerprints,
		services:        make(map[domain.ServiceID]*serviceState),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// serviceState is the per-service sampling counter plus its two windows. The
// windows are guarded by a mutex so that observing one service never blocks
// another; the counter is deliberately outside it.
//
// The counter is atomic because it is the only thing 99 relays out of 100
// touch. Taking a per-service mutex to decide "not this one" put every relay
// for a busy service through one lock for no other purpose — the sampled
// hundredth is the only relay with anything to record.
type serviceState struct {
	counter atomic.Uint64

	mu       sync.Mutex
	current  *fpWindow
	previous *fpWindow
}

// otherMethod is the shared bucket a raw method string folds into once a
// window's per-method table is already at its cap. It is itself one of the
// entries counted against that cap.
const otherMethod = "_other"

// fpWindow is one fixed-length window's tally.
type fpWindow struct {
	start        time.Time
	end          time.Time
	sampled      int
	overflow     int
	fingerprints map[uint64]*fpRecord
	methods      map[string]*methodRecord

	// methodOverflow counts the distinct raw method strings folded into
	// otherMethod because methods was already at its cap. overflowMethods
	// deduplicates that count and is itself capped at maxFingerprints
	// entries — bounded, like everything else here, rather than growing with
	// however many method names a client invents. Past that second cap a
	// repeat of an already-folded-but-unremembered method recounts as
	// "distinct"; that tail is an accepted approximation, since the point is
	// bounding memory, not an exact cardinality.
	methodOverflow  int
	overflowMethods map[string]struct{}
}

// fpRecord is what a window remembers about one fingerprint.
type fpRecord struct {
	method string
	count  int
	sample string
}

// methodRecord is what a window remembers about one raw method string.
type methodRecord struct {
	sampled  int
	distinct int
}

func newWindow(start time.Time, d time.Duration) *fpWindow {
	return &fpWindow{
		start:           start,
		end:             start.Add(d),
		fingerprints:    make(map[uint64]*fpRecord),
		methods:         make(map[string]*methodRecord),
		overflowMethods: make(map[string]struct{}),
	}
}

// record folds one fingerprinted payload into the window: bumps the window
// and per-method sampled counts always, and — for a fingerprint not already
// tracked — either starts tracking it (bumping distinct) or, if the window is
// already at its cap, counts it as overflow instead.
func (w *fpWindow) record(method string, fp uint64, sample string, maxFingerprints int) {
	w.sampled++

	mr := w.methodRecordFor(method, maxFingerprints)
	mr.sampled++

	rec, exists := w.fingerprints[fp]
	if !exists {
		if len(w.fingerprints) >= maxFingerprints {
			w.overflow++
			return
		}
		rec = &fpRecord{method: method, sample: sample}
		w.fingerprints[fp] = rec
		mr.distinct++
	}
	rec.count++
}

// methodRecordFor returns the methodRecord method should be tallied against:
// its own entry while the table has room, otherwise the shared otherMethod
// bucket, capped at maxFingerprints real entries (the same knob that bounds
// the fingerprint table, so the per-service memory this package uses stays a
// small multiple of one number rather than unbounded in either dimension).
func (w *fpWindow) methodRecordFor(method string, maxFingerprints int) *methodRecord {
	if mr, ok := w.methods[method]; ok {
		return mr
	}
	if len(w.methods) < maxFingerprints {
		mr := &methodRecord{}
		w.methods[method] = mr
		return mr
	}

	if _, seen := w.overflowMethods[method]; !seen {
		if len(w.overflowMethods) < maxFingerprints {
			w.overflowMethods[method] = struct{}{}
		}
		w.methodOverflow++
	}
	mr, ok := w.methods[otherMethod]
	if !ok {
		mr = &methodRecord{}
		w.methods[otherMethod] = mr
	}
	return mr
}

// stateFor returns the serviceState for id, creating it (with a fresh current
// window starting now) on first use.
func (s *Sampler) stateFor(id domain.ServiceID) *serviceState {
	s.mu.RLock()
	st, ok := s.services[id]
	s.mu.RUnlock()
	if ok {
		return st
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok = s.services[id]; ok {
		return st
	}
	st = &serviceState{current: newWindow(time.Now(), s.window)}
	s.services[id] = st
	return st
}

// Observe records one relay's payloads against serviceID. It runs on every
// relay, so the unsampled path is the one that matters: a map lookup for the
// service and one atomic increment, no lock at all. Only the relay whose
// count lands on a multiple of the configured rate is fingerprinted.
//
// The hashing happens before the lock is taken. It is pure work on the
// payload — no window state is involved — so holding the service's mutex
// across it would serialise every sampled relay for that service behind
// whichever one is currently compacting a body.
//
// Windows therefore roll on the sampled path rather than on every relay. A
// service that has gone quiet keeps its last window as "current" until traffic
// returns, which is why the gauge lister (PreviousWindow) refuses to report a
// previous window that has gone stale rather than trusting the roll to have
// happened.
func (s *Sampler) Observe(serviceID domain.ServiceID, payloads []domain.Payload) {
	if len(payloads) == 0 {
		return
	}

	st := s.stateFor(serviceID)
	if st.counter.Add(1)%uint64(s.rate) != 0 {
		return
	}

	type fingerprinted struct {
		method string
		fp     uint64
		sample string
	}
	prints := make([]fingerprinted, len(payloads))
	for i, p := range payloads {
		method, fp, sample := fingerprintPayload(p)
		prints[i] = fingerprinted{method: method, fp: fp, sample: sample}
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	now := time.Now()
	if !now.Before(st.current.end) {
		st.previous = st.current
		st.current = newWindow(now, s.window)
	}

	for _, pr := range prints {
		st.current.record(pr.method, pr.fp, pr.sample, s.maxFingerprints)
	}
}

// isJSONRPCShaped reports whether t is an RPCType whose body is a JSON-RPC
// envelope — a top-level "method" and "params" — as opposed to one where a
// non-empty Method() can coexist with a body that isn't JSON-RPC at all.
// RPCTypeGRPC is the case that matters: its method comes from the URL path
// and its body is protobuf, so gjson would silently find no "params" and
// every distinct call to one gRPC method would collapse to one fingerprint.
func isJSONRPCShaped(t domain.RPCType) bool {
	switch t {
	case domain.RPCTypeJSONRPC, domain.RPCTypeCometBFT:
		return true
	default:
		return false
	}
}

// fingerprintPayload reduces one payload to its raw method (empty for
// non-JSON-RPC-shaped payloads), its fingerprint hash, and the first
// sampleBytes of its body kept as an example. The non-JSON-RPC hash sees at
// most hashBytes of the body — see that constant for what it costs.
func fingerprintPayload(p domain.Payload) (method string, fp uint64, sample string) {
	data := p.Bytes()
	sample = truncate(data, sampleBytes)

	if isJSONRPCShaped(p.RPCType()) {
		if method = p.Method(); method != "" {
			params := compactJSON(gjson.GetBytes(data, "params").Raw)
			return method, hashFNV64(method + "\x00" + params), sample
		}
	}

	body := compactJSON(truncate(data, hashBytes))
	return "", hashFNV64(p.HTTPMethod() + p.Path() + body), sample
}

// compactJSON removes insignificant whitespace from raw JSON text. Text that
// fails to compact (empty, or not valid JSON — a REST or CometBFT body need
// not be JSON at all) is returned unchanged, since the fingerprint only needs
// stability, not a canonical encoding.
func compactJSON(raw string) string {
	if raw == "" {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(raw)); err != nil {
		return raw
	}
	return buf.String()
}

func truncate(data []byte, n int) string {
	if len(data) > n {
		data = data[:n]
	}
	return string(data)
}

func hashFNV64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// MethodStats summarizes one raw method's traffic within a window.
type MethodStats struct {
	Sampled       int     `json:"sampled"`
	Distinct      int     `json:"distinct"`
	DistinctRatio float64 `json:"distinct_ratio"`
}

// Fingerprint describes one distinct request shape observed within a window.
// Method is the raw method string — bounded to this admin-facing struct, see
// the package doc; it must never be attached to a metric label.
type Fingerprint struct {
	Method string  `json:"method"`
	Count  int     `json:"count"`
	Share  float64 `json:"share"`
	Sample string  `json:"sample"`
}

// Summary reports one service's traffic shape for one window.
type Summary struct {
	ServiceID     string                 `json:"service_id"`
	WindowStart   time.Time              `json:"window_start"`
	WindowEnd     time.Time              `json:"window_end"`
	Sampled       int                    `json:"sampled"`
	Distinct      int                    `json:"distinct"`
	Overflow      int                    `json:"overflow"`
	DistinctRatio float64                `json:"distinct_ratio"`
	Top1Share     float64                `json:"top1_share"`
	PerMethod     map[string]MethodStats `json:"per_method"`

	// MethodOverflow counts the distinct raw method strings folded into
	// PerMethod[otherMethod] because the per-method table was already at its
	// cap — see fpWindow.methodOverflow.
	MethodOverflow int `json:"method_overflow"`
}

// Summary returns serviceID's summary for the current window, or the
// previous one when previous is true. The second return is false when the
// service is unknown, or when previous is requested but no window has rolled
// yet.
func (s *Sampler) Summary(serviceID domain.ServiceID, previous bool) (Summary, bool) {
	w, ok := s.windowFor(serviceID, previous)
	if !ok {
		return Summary{}, false
	}
	return summarize(serviceID, w), true
}

// staleWindowFactor bounds how old the previous window may be before
// PreviousWindow disowns it, as a multiple of the window length. Two is the
// smallest value that cannot fire during normal operation: while a current
// window is filling, the previous one ended at most one window ago, and
// rolling happens on the sampled path, so a low-rate service can legitimately
// run a little past its own window end before the next roll.
const staleWindowFactor = 2

// PreviousWindow reports the previous (complete) window's distinct ratio and
// top-1 share for serviceID, for the metrics collector.
//
// ok is false when there is no completed window — and also when the completed
// window has gone stale. Windows roll when traffic arrives, so a service that
// stops receiving requests freezes with whatever its last two windows held,
// and a gauge read from that window would keep exporting a number describing
// traffic that stopped hours ago. An absent series says "no recent traffic";
// a stale one says "this is what the traffic looks like", which is a lie the
// longer it stands.
func (s *Sampler) PreviousWindow(serviceID domain.ServiceID) (distinctRatio, top1Share float64, ok bool) {
	w, ok := s.windowFor(serviceID, true)
	if !ok {
		return 0, 0, false
	}
	if time.Since(w.end) > staleWindowFactor*s.window {
		return 0, 0, false
	}
	summary := summarize(serviceID, w)
	return summary.DistinctRatio, summary.Top1Share, true
}

// Top returns serviceID's fingerprints for the given window (current, or
// previous when previous is true), ordered by descending count, limited to
// the first n. n <= 0 means no limit. A nil slice means the service, or that
// window, is unknown.
func (s *Sampler) Top(serviceID domain.ServiceID, previous bool, n int) []Fingerprint {
	w, ok := s.windowFor(serviceID, previous)
	if !ok {
		return nil
	}

	out := make([]Fingerprint, 0, len(w.fingerprints))
	for _, rec := range w.fingerprints {
		var share float64
		if w.sampled > 0 {
			share = float64(rec.count) / float64(w.sampled)
		}
		out = append(out, Fingerprint{
			Method: rec.method,
			Count:  rec.count,
			Share:  share,
			Sample: rec.sample,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Sample < out[j].Sample
	})

	if n > 0 && n < len(out) {
		out = out[:n]
	}
	return out
}

// Services returns every service ID the sampler has observed at least one
// relay for, sorted for stable output.
func (s *Sampler) Services() []domain.ServiceID {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]domain.ServiceID, 0, len(s.services))
	for id := range s.services {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// windowFor snapshots the requested window under the service's lock. It
// returns a fresh copy of the window's maps so callers can read it after
// releasing the lock without racing a concurrent Observe.
func (s *Sampler) windowFor(serviceID domain.ServiceID, previous bool) (*fpWindow, bool) {
	s.mu.RLock()
	st, ok := s.services[serviceID]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	w := st.current
	if previous {
		w = st.previous
	}
	if w == nil {
		return nil, false
	}
	return cloneWindow(w), true
}

// cloneWindow copies a window's records so a reader can walk them without
// holding the service's lock.
func cloneWindow(w *fpWindow) *fpWindow {
	out := &fpWindow{
		start:          w.start,
		end:            w.end,
		sampled:        w.sampled,
		overflow:       w.overflow,
		methodOverflow: w.methodOverflow,
		fingerprints:   make(map[uint64]*fpRecord, len(w.fingerprints)),
		methods:        make(map[string]*methodRecord, len(w.methods)),
	}
	for k, v := range w.fingerprints {
		rec := *v
		out.fingerprints[k] = &rec
	}
	for k, v := range w.methods {
		rec := *v
		out.methods[k] = &rec
	}
	return out
}

func summarize(serviceID domain.ServiceID, w *fpWindow) Summary {
	distinct := len(w.fingerprints)

	var distinctRatio float64
	if w.sampled > 0 {
		distinctRatio = float64(distinct) / float64(w.sampled)
	}

	var top1Share float64
	if w.sampled > 0 {
		maxCount := 0
		for _, rec := range w.fingerprints {
			if rec.count > maxCount {
				maxCount = rec.count
			}
		}
		top1Share = float64(maxCount) / float64(w.sampled)
	}

	perMethod := make(map[string]MethodStats, len(w.methods))
	for method, mr := range w.methods {
		var ratio float64
		if mr.sampled > 0 {
			ratio = float64(mr.distinct) / float64(mr.sampled)
		}
		perMethod[method] = MethodStats{
			Sampled:       mr.sampled,
			Distinct:      mr.distinct,
			DistinctRatio: ratio,
		}
	}

	return Summary{
		ServiceID:      string(serviceID),
		WindowStart:    w.start,
		WindowEnd:      w.end,
		Sampled:        w.sampled,
		Distinct:       distinct,
		Overflow:       w.overflow,
		DistinctRatio:  distinctRatio,
		Top1Share:      top1Share,
		PerMethod:      perMethod,
		MethodOverflow: w.methodOverflow,
	}
}
