package healthcheck

import (
	"context"
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
	"github.com/pokt-network/sage/qos"
)

// ExternalSourceManager owns one ExternalBlockFetcher per service and lets the
// admin API replace a service's sources on a running process.
//
// The sources come from `external_block_sources` in the config file, and on
// the deployment this was built for that file is a sealed secret the operator
// could not reach when thirteen of its sixty-eight sources turned out to be
// retired, misnamed or rate-limited (2026-09-13). A change here is
// per process and does not survive a restart: the process comes back on the
// file's sources. It is the same contract as PUT /admin/log-level, for the
// same reason.
type ExternalSourceManager struct {
	logger   *slog.Logger
	failures ExternalSourceFailureRecorder
	// resolve returns the floor setter for a service, or false when the
	// service has no plugin or its plugin tracks no block height. Nil means
	// "every service resolves to nothing", which makes Set refuse everything;
	// tests pass their own.
	resolve func(domain.ServiceID) (qos.ExternalFloorSetter, bool)

	mu       sync.Mutex
	ctx      context.Context // set by Start; nil before
	services map[domain.ServiceID]*managedSources
}

type managedSources struct {
	sources []config.ExternalBlockSource
	origin  string
	setter  qos.ExternalFloorSetter
	fetcher *ExternalBlockFetcher
	cancel  context.CancelFunc
}

// Where a service's sources came from, in the admin view.
const (
	SourceOriginConfig = "config"
	SourceOriginAdmin  = "admin"
)

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
	m.services[serviceID] = &managedSources{sources: sources, origin: SourceOriginConfig, setter: setter}
	return nil
}

// Start begins polling every configured service. Sources set afterwards
// start polling as they are set; the fetchers stop when ctx is cancelled.
func (m *ExternalSourceManager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx = ctx
	for id, ms := range m.services {
		m.startLocked(id, ms)
	}
}

// Set replaces a service's sources and restarts its polling. It returns the
// resulting view. ErrInvalidSources, ErrUnknownService and ErrNoFloor are
// the failures the caller can act on.
func (m *ExternalSourceManager) Set(serviceID domain.ServiceID, sources []config.ExternalBlockSource) (ExternalSourceView, error) {
	if err := ValidateExternalSources(sources); err != nil {
		return ExternalSourceView{}, fmt.Errorf("%w: %w", ErrInvalidSources, err)
	}
	setter, err := m.setterFor(serviceID)
	if err != nil {
		return ExternalSourceView{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if prev, ok := m.services[serviceID]; ok {
		m.stopLocked(prev)
	}
	ms := &managedSources{sources: sources, origin: SourceOriginAdmin, setter: setter}
	m.services[serviceID] = ms
	if m.ctx != nil {
		m.startLocked(serviceID, ms)
	}
	m.logger.Warn("external block sources replaced through the admin API; not persisted, the file's sources return on restart",
		"service_id", serviceID,
		"sources", len(sources),
	)
	return m.viewLocked(serviceID, ms), nil
}

// Remove stops polling a service and forgets its sources. It reports whether
// the service had any.
func (m *ExternalSourceManager) Remove(serviceID domain.ServiceID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ms, ok := m.services[serviceID]
	if !ok {
		return false
	}
	m.stopLocked(ms)
	delete(m.services, serviceID)
	m.logger.Warn("external block sources removed through the admin API; not persisted, the file's sources return on restart",
		"service_id", serviceID,
	)
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

// startLocked starts a fetcher for ms and the goroutine that applies its
// heights. Caller holds m.mu and m.ctx is set.
func (m *ExternalSourceManager) startLocked(serviceID domain.ServiceID, ms *managedSources) {
	ctx, cancel := context.WithCancel(m.ctx)
	fetcher := NewExternalBlockFetcher(serviceID, ms.sources, m.logger)
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

func (m *ExternalSourceManager) viewLocked(serviceID domain.ServiceID, ms *managedSources) ExternalSourceView {
	v := ExternalSourceView{ServiceID: serviceID, Origin: ms.origin, Sources: make([]ExternalSourceSpec, 0, len(ms.sources))}
	for _, s := range ms.sources {
		v.Sources = append(v.Sources, SpecFromSource(s))
	}
	if ms.fetcher != nil {
		v.Status = ms.fetcher.Status()
		v.Status.Running = true
	}
	return v
}

// ExternalSourceView is one service's sources and their poll status.
type ExternalSourceView struct {
	ServiceID domain.ServiceID     `json:"service_id"`
	Origin    string               `json:"origin"`
	Sources   []ExternalSourceSpec `json:"sources"`
	Status    ExternalSourceStatus `json:"status"`
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
		return errors.New("at least one source is required; use DELETE to stop polling")
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
