package healthcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/override"
	"github.com/pokt-network/sage/qos"
)

// ExternalSourceManager owns one ExternalBlockFetcher per service and lets the
// admin API replace a service's sources on a running gateway.
//
// The sources come from `external_block_sources` in the config file, and on
// the deployment this was built for that file is a sealed secret the operator
// could not reach when thirteen of its sixty-eight sources turned out to be
// retired, misnamed or rate-limited (2026-09-13). An admin change is kept in
// the override store (package override): every replica applies it within the
// watch interval and a restarted process starts with it; without Redis the
// store is this process only. The file's sources stay known underneath, so
// DELETE returns to them.
type ExternalSourceManager struct {
	logger   *slog.Logger
	failures ExternalSourceFailureRecorder
	// resolve returns the floor setter for a service, or false when the
	// service has no plugin or its plugin tracks no block height.
	resolve func(domain.ServiceID) (qos.ExternalFloorSetter, bool)

	mu        sync.Mutex
	ctx       context.Context // set by Start; nil before
	overrides override.Store  // may be nil: per process, nothing persisted
	services  map[domain.ServiceID]*managedSources
}

type managedSources struct {
	configured  []config.ExternalBlockSource // the file's; nil when it has none
	override    []config.ExternalBlockSource // the admin's; nil when none, empty when "stop polling"
	hasOverride bool
	overrideRaw string // as stored, to skip re-applying the same value
	setter      qos.ExternalFloorSetter
	fetcher     *ExternalBlockFetcher
	cancel      context.CancelFunc
}

// effective is what is polled: the override when there is one, else the file's.
func (ms *managedSources) effective() []config.ExternalBlockSource {
	if ms.hasOverride {
		return ms.override
	}
	return ms.configured
}

// Where a service's sources come from, in the admin view.
const (
	// SourceOriginConfig: the file's sources are polled.
	SourceOriginConfig = "config"
	// SourceOriginAdmin: an admin-set list is polled.
	SourceOriginAdmin = "admin"
	// SourceOriginAdminDisabled: an admin stopped polling; the file's sources
	// are known but idle.
	SourceOriginAdminDisabled = "admin-disabled"
	// SourceOriginNone: nothing to poll.
	SourceOriginNone = "none"
)

// overridePrefix is the manager's key space in the override store; one key
// per service, holding {"sources":[...]} ("sources":[] means stop polling).
const overridePrefix = "external_sources/"

func overrideKey(serviceID domain.ServiceID) string { return overridePrefix + string(serviceID) }

// persistTimeout bounds one write-through; the admin caller is waiting.
const persistTimeout = 3 * time.Second

// Errors the admin API maps to statuses.
var (
	// ErrUnknownService is a service SAGE has no QoS plugin for.
	ErrUnknownService = errors.New("service is not registered")
	// ErrNoFloor is a service whose plugin tracks no block height, so a source
	// would have nothing to lift.
	ErrNoFloor = errors.New("the service's QoS plugin tracks no block height, so external sources have nothing to lift")
	// ErrInvalidSources wraps a validation failure of the submitted sources.
	ErrInvalidSources = errors.New("invalid external block sources")
)

// NewExternalSourceManager builds a manager. failures may be nil.
func NewExternalSourceManager(
	logger *slog.Logger,
	failures ExternalSourceFailureRecorder,
	resolve func(domain.ServiceID) (qos.ExternalFloorSetter, bool),
) *ExternalSourceManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &ExternalSourceManager{
		logger:   logger,
		failures: failures,
		resolve:  resolve,
		services: make(map[domain.ServiceID]*managedSources),
	}
}

// SetOverrides attaches the store admin changes persist through. Call before
// Start; Start then applies what the store holds and watches it.
func (m *ExternalSourceManager) SetOverrides(store override.Store) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.overrides = store
}

// Persistent reports whether admin changes reach other replicas and survive a
// restart.
func (m *ExternalSourceManager) Persistent() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.overrides != nil && m.overrides.Shared()
}

// Configure registers a service's sources from the config file. Call before
// Start. It resolves the floor setter now so a service whose plugin cannot
// take a floor is reported at startup, not discovered at poll time.
func (m *ExternalSourceManager) Configure(serviceID domain.ServiceID, sources []config.ExternalBlockSource) error {
	setter, err := m.setterFor(serviceID)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ms := m.services[serviceID]
	if ms == nil {
		ms = &managedSources{}
		m.services[serviceID] = ms
	}
	ms.configured = sources
	ms.setter = setter
	return nil
}

// Start begins polling every service's effective sources and, when an
// override store is attached, applies what it holds and follows its changes.
// Fetchers stop when ctx is cancelled.
func (m *ExternalSourceManager) Start(ctx context.Context) {
	m.mu.Lock()
	m.ctx = ctx
	for id, ms := range m.services {
		m.restartLocked(id, ms)
	}
	store := m.overrides
	m.mu.Unlock()
	if store != nil {
		override.Watch(ctx, m.logger, store, overridePrefix, 0, m.applyPersisted)
	}
}

// Set replaces a service's sources: validated, persisted, then applied here
// (other replicas follow through the store). An empty list means "stop
// polling this service". ErrInvalidSources, ErrUnknownService and ErrNoFloor
// are the failures the caller can act on.
func (m *ExternalSourceManager) Set(serviceID domain.ServiceID, sources []config.ExternalBlockSource) (ExternalSourceView, error) {
	if len(sources) > 0 {
		if err := ValidateExternalSources(sources); err != nil {
			return ExternalSourceView{}, fmt.Errorf("%w: %w", ErrInvalidSources, err)
		}
	}
	setter, err := m.setterFor(serviceID)
	if err != nil {
		return ExternalSourceView{}, err
	}
	raw, err := encodeSources(sources)
	if err != nil {
		return ExternalSourceView{}, err
	}
	m.mu.Lock()
	store := m.overrides
	m.mu.Unlock()
	if store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
		defer cancel()
		if err := store.Set(ctx, overrideKey(serviceID), raw); err != nil {
			return ExternalSourceView{}, fmt.Errorf("external sources not persisted, not applied: %w", err)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ms := m.services[serviceID]
	if ms == nil {
		ms = &managedSources{}
		m.services[serviceID] = ms
	}
	ms.setter = setter
	m.applyOverrideLocked(serviceID, ms, sources, raw)
	m.logger.Warn("external block sources replaced through the admin API",
		"service_id", serviceID, "sources", len(sources), "persisted", store != nil && store.Shared())
	return m.viewLocked(serviceID, ms), nil
}

// Remove clears a service's admin override: polling returns to the file's
// sources, or stops if the file has none. It reports whether there was an
// override to clear.
func (m *ExternalSourceManager) Remove(serviceID domain.ServiceID) bool {
	m.mu.Lock()
	store := m.overrides
	m.mu.Unlock()
	if store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
		defer cancel()
		if err := store.Delete(ctx, overrideKey(serviceID)); err != nil {
			m.logger.Warn("external block sources: override removed here but not from the store; it will return on the next reload", "service_id", serviceID, "error", err)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ms, ok := m.services[serviceID]
	if !ok || !ms.hasOverride {
		return false
	}
	m.clearOverrideLocked(serviceID, ms)
	m.logger.Warn("external block sources override cleared through the admin API; the file's sources are polled again", "service_id", serviceID)
	return true
}

// Get returns one service's view.
func (m *ExternalSourceManager) Get(serviceID domain.ServiceID) (ExternalSourceView, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ms, ok := m.services[serviceID]
	if !ok {
		return ExternalSourceView{}, false
	}
	return m.viewLocked(serviceID, ms), true
}

// View returns every service's sources and poll status, sorted by service.
func (m *ExternalSourceManager) View() []ExternalSourceView {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ExternalSourceView, 0, len(m.services))
	for id, ms := range m.services {
		out = append(out, m.viewLocked(id, ms))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ServiceID < out[j].ServiceID })
	return out
}

// applyPersisted brings the manager in line with the override store: every
// key is an override to hold, every managed override without a key is one
// that was cleared elsewhere.
func (m *ExternalSourceManager) applyPersisted(entries map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[domain.ServiceID]bool, len(entries))
	for key, raw := range entries {
		serviceID := domain.ServiceID(strings.TrimPrefix(key, overridePrefix))
		seen[serviceID] = true
		ms := m.services[serviceID]
		if ms != nil && ms.hasOverride && ms.overrideRaw == raw {
			continue
		}
		sources, err := decodeSources(raw)
		if err != nil {
			m.logger.Warn("external block sources: ignoring a persisted override that does not parse", "service_id", serviceID, "error", err)
			continue
		}
		if ms == nil {
			setter, err := m.setterFor(serviceID)
			if err != nil {
				m.logger.Warn("external block sources: persisted override for a service that cannot take one", "service_id", serviceID, "error", err)
				continue
			}
			ms = &managedSources{setter: setter}
			m.services[serviceID] = ms
		}
		m.applyOverrideLocked(serviceID, ms, sources, raw)
		m.logger.Info("external block sources applied from the override store", "service_id", serviceID, "sources", len(sources))
	}
	for serviceID, ms := range m.services {
		if ms.hasOverride && !seen[serviceID] {
			m.clearOverrideLocked(serviceID, ms)
			m.logger.Info("external block sources override cleared from the override store; the file's sources are polled again", "service_id", serviceID)
		}
	}
}

func (m *ExternalSourceManager) applyOverrideLocked(serviceID domain.ServiceID, ms *managedSources, sources []config.ExternalBlockSource, raw string) {
	ms.override = sources
	ms.hasOverride = true
	ms.overrideRaw = raw
	m.restartLocked(serviceID, ms)
}

func (m *ExternalSourceManager) clearOverrideLocked(serviceID domain.ServiceID, ms *managedSources) {
	ms.override = nil
	ms.hasOverride = false
	ms.overrideRaw = ""
	if ms.configured == nil {
		m.stopLocked(ms)
		delete(m.services, serviceID)
		return
	}
	m.restartLocked(serviceID, ms)
}

// restartLocked stops any running fetcher and starts one for the effective
// sources, if there are any and Start has run. Caller holds m.mu.
func (m *ExternalSourceManager) restartLocked(serviceID domain.ServiceID, ms *managedSources) {
	m.stopLocked(ms)
	sources := ms.effective()
	if m.ctx == nil || len(sources) == 0 || ms.setter == nil {
		return
	}
	ctx, cancel := context.WithCancel(m.ctx)
	fetcher := NewExternalBlockFetcher(serviceID, sources, m.logger)
	fetcher.SetFailureRecorder(m.failures)
	heights := fetcher.Start(ctx)
	setter := ms.setter
	safego.Go(m.logger, "external.blockheight.floor", func() {
		for h := range heights {
			setter.SetExternalFloor(h.Height)
		}
	})
	ms.fetcher = fetcher
	ms.cancel = cancel
}

func (m *ExternalSourceManager) stopLocked(ms *managedSources) {
	if ms.cancel != nil {
		ms.cancel()
	}
	ms.cancel = nil
	ms.fetcher = nil
}

func (m *ExternalSourceManager) setterFor(serviceID domain.ServiceID) (qos.ExternalFloorSetter, error) {
	if m.resolve == nil {
		return nil, ErrUnknownService
	}
	setter, ok := m.resolve(serviceID)
	if !ok || setter == nil {
		return nil, ErrNoFloor
	}
	return setter, nil
}

func (m *ExternalSourceManager) viewLocked(serviceID domain.ServiceID, ms *managedSources) ExternalSourceView {
	v := ExternalSourceView{ServiceID: serviceID, Sources: []ExternalSourceSpec{}, Configured: []ExternalSourceSpec{}}
	switch {
	case ms.hasOverride && len(ms.override) == 0:
		v.Origin = SourceOriginAdminDisabled
	case ms.hasOverride:
		v.Origin = SourceOriginAdmin
	case len(ms.configured) > 0:
		v.Origin = SourceOriginConfig
	default:
		v.Origin = SourceOriginNone
	}
	for _, s := range ms.effective() {
		v.Sources = append(v.Sources, SpecFromSource(s))
	}
	for _, s := range ms.configured {
		v.Configured = append(v.Configured, SpecFromSource(s))
	}
	if ms.fetcher != nil {
		v.Status = ms.fetcher.Status()
		v.Status.Running = true
	}
	return v
}

// ExternalSourceView is one service's sources and their poll status.
type ExternalSourceView struct {
	ServiceID domain.ServiceID `json:"service_id"`
	// Origin says whose sources are polled: config, admin, admin-disabled or
	// none.
	Origin string `json:"origin"`
	// Sources are the ones polled now.
	Sources []ExternalSourceSpec `json:"sources"`
	// Configured are the file's, whatever is polled; what DELETE returns to.
	Configured []ExternalSourceSpec `json:"configured"`
	Status     ExternalSourceStatus `json:"status"`
}

// ExternalSourceSpec is one source as the admin API reads and writes it:
// the config fields with durations as strings ("15s").
type ExternalSourceSpec struct {
	URL      string `json:"url"`
	Type     string `json:"type,omitempty"`
	Method   string `json:"method,omitempty"`
	Path     string `json:"path,omitempty"`
	Interval string `json:"interval,omitempty"`
	Timeout  string `json:"timeout,omitempty"`
}

// SpecFromSource renders a config source for the admin view.
func SpecFromSource(s config.ExternalBlockSource) ExternalSourceSpec {
	spec := ExternalSourceSpec{URL: s.URL, Type: s.Type, Method: s.Method, Path: s.Path}
	if s.Interval > 0 {
		spec.Interval = s.Interval.String()
	}
	if s.Timeout > 0 {
		spec.Timeout = s.Timeout.String()
	}
	return spec
}

// SourceFromSpec parses an admin-submitted source. Durations are optional.
func SourceFromSpec(spec ExternalSourceSpec) (config.ExternalBlockSource, error) {
	src := config.ExternalBlockSource{URL: strings.TrimSpace(spec.URL), Type: spec.Type, Method: spec.Method, Path: spec.Path}
	var err error
	if spec.Interval != "" {
		if src.Interval, err = time.ParseDuration(spec.Interval); err != nil {
			return src, fmt.Errorf("interval %q: %w", spec.Interval, err)
		}
	}
	if spec.Timeout != "" {
		if src.Timeout, err = time.ParseDuration(spec.Timeout); err != nil {
			return src, fmt.Errorf("timeout %q: %w", spec.Timeout, err)
		}
	}
	return src, nil
}

// The stored shape: the specs, so what is read back is what was written.
type storedSources struct {
	Sources []ExternalSourceSpec `json:"sources"`
}

func encodeSources(sources []config.ExternalBlockSource) (string, error) {
	st := storedSources{Sources: make([]ExternalSourceSpec, 0, len(sources))}
	for _, s := range sources {
		st.Sources = append(st.Sources, SpecFromSource(s))
	}
	b, err := json.Marshal(st)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func decodeSources(raw string) ([]config.ExternalBlockSource, error) {
	var st storedSources
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return nil, err
	}
	out := make([]config.ExternalBlockSource, 0, len(st.Sources))
	for _, spec := range st.Sources {
		src, err := SourceFromSpec(spec)
		if err != nil {
			return nil, err
		}
		out = append(out, src)
	}
	if len(out) > 0 {
		if err := ValidateExternalSources(out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// maxExternalSourcesPerService bounds an admin submission. Several sources
// are polled in parallel on one ticker, so each one is a request per tick.
const maxExternalSourcesPerService = 8

// ValidateExternalSources checks what an operator submitted before it is
// polled: at least one source, an http(s) URL with a host, a known type, and
// sane durations. The config loader does not validate these (a PATH file
// must load unmodified), so the admin path is the one place a typo is
// caught before it becomes a poll every fifteen seconds.
func ValidateExternalSources(sources []config.ExternalBlockSource) error {
	if len(sources) == 0 {
		return errors.New("at least one source is required")
	}
	if len(sources) > maxExternalSourcesPerService {
		return fmt.Errorf("%d sources; at most %d per service", len(sources), maxExternalSourcesPerService)
	}
	for i, s := range sources {
		u, err := url.Parse(s.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("sources[%d].url %q: must be an http(s) URL with a host", i, s.URL)
		}
		switch strings.ToLower(s.Type) {
		case "", "json_rpc", "jsonrpc", "rest", "comet_bft", "cometbft":
		default:
			return fmt.Errorf("sources[%d].type %q: one of json_rpc, rest, comet_bft", i, s.Type)
		}
		if s.Interval < 0 || (s.Interval > 0 && s.Interval < time.Second) || s.Interval > time.Hour {
			return fmt.Errorf("sources[%d].interval %s: between 1s and 1h, or unset for 30s", i, s.Interval)
		}
		if s.Timeout < 0 || s.Timeout > time.Minute {
			return fmt.Errorf("sources[%d].timeout %s: at most 1m, or unset for 10s", i, s.Timeout)
		}
	}
	return nil
}
