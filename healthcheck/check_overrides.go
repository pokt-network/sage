package healthcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/override"
)

// CheckOverrides lets the admin API replace one service's configured health
// checks (active_health_checks.local) on a running gateway.
//
// Adding a check used to mean editing the config file, which on the mainnet
// deployment is a sealed 1Password item whose edit restarts every pod — for a
// one-line probe like sei's EVM face. An admin change is kept in the override
// store, the same way external block sources are: every replica applies it
// within the watch interval and a restarted process starts with it; without
// Redis it is this process only. The file's block stays known underneath, so
// DELETE returns to it. An admin block replaces the file's block for that
// service; the QoS plugin's own checks run regardless, as they always do.
type CheckOverrides struct {
	sink   checksSink
	known  func(domain.ServiceID) bool
	logger *slog.Logger

	mu        sync.Mutex
	base      config.HealthCheckConfig
	overrides override.Store // may be nil: per process, nothing persisted
	admin     map[domain.ServiceID]adminBlock
	current   *ConfiguredChecks // what the sink was last given
}

// Current returns the checks last handed to the executor.
func (c *CheckOverrides) Current() *ConfiguredChecks {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// checksSink is where the merged checks go. *Executor implements it.
type checksSink interface {
	SetConfiguredChecks(c *ConfiguredChecks)
}

type adminBlock struct {
	block config.ServiceHealthChecks
	raw   string // as stored, to skip re-applying the same value
}

// checkOverridePrefix is the key space in the override store; one key per
// service, holding a ServiceChecksSpec.
const checkOverridePrefix = "health_checks/"

// ErrInvalidChecks wraps a validation failure of submitted checks.
var ErrInvalidChecks = errors.New("invalid health checks")

// CheckSpec is one check as the admin API reads and writes it: the config
// file's fields, durations as strings.
type CheckSpec struct {
	Name               string `json:"name"`
	Type               string `json:"type,omitempty"`
	Method             string `json:"method,omitempty"`
	Path               string `json:"path,omitempty"`
	Body               string `json:"body,omitempty"`
	ExpectedStatusCode int    `json:"expected_status_code,omitempty"`
	ReputationSignal   string `json:"reputation_signal,omitempty"`
	Timeout            string `json:"timeout,omitempty"`
}

// ServiceChecksSpec is one service's block. An admin block is always
// enabled: to stop a service's configured checks, submit an empty list.
type ServiceChecksSpec struct {
	CheckInterval string      `json:"check_interval,omitempty"`
	Checks        []CheckSpec `json:"checks"`
}

// CheckView is one service's checks for the admin API.
type CheckView struct {
	ServiceID domain.ServiceID `json:"service_id"`
	// Origin is config (the file's block runs), admin (an admin block runs)
	// or none.
	Origin    string            `json:"origin"`
	Effective ServiceChecksSpec `json:"effective"`
	// Configured is the file's block, what DELETE returns to; absent when
	// the file has none.
	Configured *ServiceChecksSpec `json:"configured,omitempty"`
}

// NewCheckOverrides builds the manager. known reports whether a service is
// one this gateway serves; logger may be nil.
func NewCheckOverrides(sink checksSink, known func(domain.ServiceID) bool, logger *slog.Logger) *CheckOverrides {
	if logger == nil {
		logger = slog.Default()
	}
	return &CheckOverrides{sink: sink, known: known, logger: logger, admin: make(map[domain.ServiceID]adminBlock)}
}

// SetOverrides attaches the store admin changes persist through. Call before
// Start.
func (c *CheckOverrides) SetOverrides(store override.Store) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.overrides = store
}

// Persistent reports whether admin changes reach other replicas and survive a
// restart.
func (c *CheckOverrides) Persistent() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.overrides != nil && c.overrides.Shared()
}

// SetBase installs the config file's checks — at startup and on every reload —
// and applies them with the admin blocks on top. It returns the problems
// worth telling an operator about, as BuildConfiguredChecks does.
func (c *CheckOverrides) SetBase(base config.HealthCheckConfig) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.base = base
	return c.applyLocked()
}

// Start applies what the override store holds and follows its changes.
func (c *CheckOverrides) Start(ctx context.Context) {
	c.mu.Lock()
	store := c.overrides
	c.mu.Unlock()
	if store != nil {
		override.Watch(ctx, c.logger, store, checkOverridePrefix, 0, c.applyPersisted)
	}
}

// Set replaces a service's configured checks: validated, persisted, then
// applied here (other replicas follow through the store).
func (c *CheckOverrides) Set(serviceID domain.ServiceID, spec ServiceChecksSpec) (CheckView, error) {
	if c.known != nil && !c.known(serviceID) {
		return CheckView{}, ErrUnknownService
	}
	block, err := spec.block(serviceID)
	if err != nil {
		return CheckView{}, fmt.Errorf("%w: %w", ErrInvalidChecks, err)
	}
	if _, warnings := BuildConfiguredChecks(config.HealthCheckConfig{Local: []config.ServiceHealthChecks{block}}); len(warnings) > 0 {
		return CheckView{}, fmt.Errorf("%w: %s", ErrInvalidChecks, strings.Join(warnings, "; "))
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return CheckView{}, err
	}
	c.mu.Lock()
	store := c.overrides
	c.mu.Unlock()
	if store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
		defer cancel()
		if err := store.Set(ctx, checkOverridePrefix+string(serviceID), string(raw)); err != nil {
			return CheckView{}, fmt.Errorf("health checks not persisted, not applied: %w", err)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.admin[serviceID] = adminBlock{block: block, raw: string(raw)}
	c.applyLocked()
	c.logger.Warn("health checks replaced through the admin API",
		"service_id", serviceID, "checks", len(block.Checks), "persisted", store != nil && store.Shared())
	return c.viewLocked(serviceID), nil
}

// Remove clears a service's admin block; the file's block runs again. It
// reports whether there was one to clear.
func (c *CheckOverrides) Remove(serviceID domain.ServiceID) bool {
	c.mu.Lock()
	store := c.overrides
	c.mu.Unlock()
	if store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
		defer cancel()
		if err := store.Delete(ctx, checkOverridePrefix+string(serviceID)); err != nil {
			c.logger.Warn("health checks: override removed here but not from the store; it will return on the next watch", "service_id", serviceID, "error", err)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.admin[serviceID]; !ok {
		return false
	}
	delete(c.admin, serviceID)
	c.applyLocked()
	c.logger.Warn("health checks override cleared through the admin API; the file's checks run again", "service_id", serviceID)
	return true
}

// Get returns one service's view, false when neither the file nor an admin
// block names it.
func (c *CheckOverrides) Get(serviceID domain.ServiceID) (CheckView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.viewLocked(serviceID)
	return v, v.Origin != SourceOriginNone
}

// View returns every service with configured or admin checks, sorted.
func (c *CheckOverrides) View() []CheckView {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := map[domain.ServiceID]bool{}
	for _, b := range c.base.Local {
		ids[domain.ServiceID(b.ServiceID)] = true
	}
	for id := range c.admin {
		ids[id] = true
	}
	out := make([]CheckView, 0, len(ids))
	for id := range ids {
		out = append(out, c.viewLocked(id))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ServiceID < out[j].ServiceID })
	return out
}

// applyPersisted brings the admin blocks in line with the override store.
func (c *CheckOverrides) applyPersisted(entries map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := make(map[domain.ServiceID]adminBlock, len(entries))
	for key, raw := range entries {
		id := domain.ServiceID(strings.TrimPrefix(key, checkOverridePrefix))
		if cur, ok := c.admin[id]; ok && cur.raw == raw {
			next[id] = cur
			continue
		}
		var spec ServiceChecksSpec
		block, err := func() (config.ServiceHealthChecks, error) {
			if err := json.Unmarshal([]byte(raw), &spec); err != nil {
				return config.ServiceHealthChecks{}, err
			}
			return spec.block(id)
		}()
		if err != nil {
			c.logger.Warn("health checks: ignoring a persisted override that does not parse", "service_id", id, "error", err)
			continue
		}
		next[id] = adminBlock{block: block, raw: raw}
		c.logger.Info("health checks applied from the override store", "service_id", id, "checks", len(block.Checks))
	}
	changed := len(next) != len(c.admin)
	for id, b := range next {
		if cur, ok := c.admin[id]; !ok || cur.raw != b.raw {
			changed = true
		}
	}
	if changed {
		c.admin = next
		c.applyLocked()
	}
}

// applyLocked builds the file's blocks with each admin block in place of the
// file's for its service, and hands the result to the executor.
func (c *CheckOverrides) applyLocked() []string {
	merged := c.base
	merged.Local = nil
	for _, b := range c.base.Local {
		if _, replaced := c.admin[domain.ServiceID(b.ServiceID)]; !replaced {
			merged.Local = append(merged.Local, b)
		}
	}
	ids := make([]domain.ServiceID, 0, len(c.admin))
	for id := range c.admin {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		merged.Local = append(merged.Local, c.admin[id].block)
	}
	checks, warnings := BuildConfiguredChecks(merged)
	c.current = checks
	c.sink.SetConfiguredChecks(checks)
	return warnings
}

func (c *CheckOverrides) viewLocked(id domain.ServiceID) CheckView {
	v := CheckView{ServiceID: id, Origin: SourceOriginNone, Effective: ServiceChecksSpec{Checks: []CheckSpec{}}}
	for _, b := range c.base.Local {
		if domain.ServiceID(b.ServiceID) == id {
			spec := specFromBlock(b)
			v.Configured = &spec
			if b.Enabled {
				v.Origin, v.Effective = SourceOriginConfig, spec
			}
		}
	}
	if a, ok := c.admin[id]; ok {
		v.Origin, v.Effective = SourceOriginAdmin, specFromBlock(a.block)
	}
	return v
}

// block turns a spec into a config block for serviceID.
func (s ServiceChecksSpec) block(serviceID domain.ServiceID) (config.ServiceHealthChecks, error) {
	b := config.ServiceHealthChecks{ServiceID: string(serviceID), Enabled: true}
	if s.CheckInterval != "" {
		d, err := time.ParseDuration(s.CheckInterval)
		if err != nil || d < 0 {
			return b, fmt.Errorf("check_interval %q is not a duration", s.CheckInterval)
		}
		b.CheckInterval = d
	}
	for _, cs := range s.Checks {
		hc := config.HealthCheck{
			Name: cs.Name, Type: cs.Type, Method: cs.Method, Path: cs.Path, Body: cs.Body,
			ExpectedStatusCode: cs.ExpectedStatusCode, ReputationSignal: cs.ReputationSignal,
		}
		if cs.Timeout != "" {
			d, err := time.ParseDuration(cs.Timeout)
			if err != nil || d < 0 {
				return b, fmt.Errorf("check %q: timeout %q is not a duration", cs.Name, cs.Timeout)
			}
			hc.Timeout = d
		}
		b.Checks = append(b.Checks, hc)
	}
	return b, nil
}

func specFromBlock(b config.ServiceHealthChecks) ServiceChecksSpec {
	s := ServiceChecksSpec{Checks: []CheckSpec{}}
	if b.CheckInterval > 0 {
		s.CheckInterval = b.CheckInterval.String()
	}
	for _, hc := range b.Checks {
		cs := CheckSpec{
			Name: hc.Name, Type: hc.Type, Method: hc.Method, Path: hc.Path, Body: hc.Body,
			ExpectedStatusCode: hc.ExpectedStatusCode, ReputationSignal: hc.ReputationSignal,
		}
		if hc.Timeout > 0 {
			cs.Timeout = hc.Timeout.String()
		}
		s.Checks = append(s.Checks, cs)
	}
	return s
}
