package tuning

import (
	"errors"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

// errNoStore is returned when a change is submitted to a gateway built without
// a tuning store. Reporting it beats accepting the write into nothing.
var errNoStore = errors.New("tuning is not enabled on this gateway")

// Override is one stored value, with enough context to answer "who changed
// this, and when" from the admin API.
type Override struct {
	Value Value     `json:"value"`
	SetAt time.Time `json:"set_at"`
}

// KnobState is a knob plus whatever has been set on it, for the admin API.
type KnobState struct {
	Knob             Knob                          `json:"knob"`
	Global           *Override                     `json:"global,omitempty"`
	ServiceOverrides map[domain.ServiceID]Override `json:"service_overrides,omitempty"`
}

// Store holds runtime overrides.
//
// In memory only, and deliberately so for now: an override is an operator
// reacting to something in front of them, and a reaction that silently outlives
// the incident — and the operator's shift — is how a gateway ends up running
// settings nobody remembers choosing. Restarting clears them and the config
// file is authoritative again. The admin API says so on every read; when that
// stops being the right trade, the persistence to add is Redis, alongside the
// feature-flag store, not a file SAGE writes to.
type Store struct {
	mu      sync.RWMutex
	global  map[string]Override
	service map[string]map[domain.ServiceID]Override
	// base holds what the config file says, as the operator would read it back.
	// The store does not otherwise know: every reader passes its own base to
	// Int/Duration/Float, because the base for a per-service knob comes from
	// that service's config block and only the reader can resolve it. That is
	// fine for resolving a value and useless for ANSWERING one — an operator
	// asking what is in force gets overrides and no idea what they are
	// overriding. SetBase is how a reader tells the store the answer it
	// already has. Empty for a knob nobody registered, which reads as unknown
	// rather than as zero.
	base map[string]string
}

// NewStore returns an empty store. Nothing is seeded from config: an entry here
// means "somebody overrode this", and seeding it with config values would make
// every knob look overridden and hide the ones that are.
func NewStore() *Store {
	return &Store{
		global:  make(map[string]Override),
		service: make(map[string]map[domain.ServiceID]Override),
		base:    make(map[string]string),
	}
}

// Set records an override. An empty serviceID sets the global value.
//
// A nil Store accepts nothing and reports it, so a gateway built without
// tuning behaves like one where nobody has set anything.
func (s *Store) Set(name string, serviceID domain.ServiceID, raw string) error {
	value, err := Parse(name, raw)
	if err != nil {
		return err
	}
	if s == nil {
		return errNoStore
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	override := Override{Value: value, SetAt: time.Now()}
	if serviceID == "" {
		s.global[name] = override
		return nil
	}
	if s.service[name] == nil {
		s.service[name] = make(map[domain.ServiceID]Override)
	}
	s.service[name][serviceID] = override
	return nil
}

// Delete removes an override, returning whether there was one. An empty
// serviceID clears the global value and leaves per-service overrides alone —
// they are the narrower statement and clearing them by accident would revert a
// service the operator did not mean to touch.
func (s *Store) Delete(name string, serviceID domain.ServiceID) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if serviceID == "" {
		_, existed := s.global[name]
		delete(s.global, name)
		return existed
	}
	overrides, ok := s.service[name]
	if !ok {
		return false
	}
	_, existed := overrides[serviceID]
	delete(overrides, serviceID)
	if len(overrides) == 0 {
		delete(s.service, name)
	}
	return existed
}

// lookup resolves a knob for a service: per-service override first, then
// global, then nothing.
func (s *Store) lookup(name string, serviceID domain.ServiceID) (Value, bool) {
	if s == nil {
		return Value{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if overrides, ok := s.service[name]; ok {
		if o, ok := overrides[serviceID]; ok {
			return o.Value, true
		}
	}
	if o, ok := s.global[name]; ok {
		return o.Value, true
	}
	return Value{}, false
}

// Int returns the override for a knob, or base when nothing is set.
func (s *Store) Int(name string, serviceID domain.ServiceID, base int) int {
	if v, ok := s.lookup(name, serviceID); ok {
		return v.Int
	}
	return base
}

// Duration returns the override for a knob, or base when nothing is set.
func (s *Store) Duration(name string, serviceID domain.ServiceID, base time.Duration) time.Duration {
	if v, ok := s.lookup(name, serviceID); ok {
		return v.Dur
	}
	return base
}

// Float returns the override for a knob, or base when nothing is set.
func (s *Store) Float(name string, serviceID domain.ServiceID, base float64) float64 {
	if v, ok := s.lookup(name, serviceID); ok {
		return v.Float
	}
	return base
}

// All returns every registered knob with whatever has been set on it — every
// knob, not only the touched ones, so the admin API and the UI list what can be
// changed rather than what somebody happens to have changed already.
func (s *Store) All() map[string]KnobState {
	out := make(map[string]KnobState, len(Knobs))
	for _, knob := range Knobs {
		out[knob.Name] = KnobState{Knob: knob}
	}
	if s == nil {
		return out
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for name, override := range s.global {
		state, ok := out[name]
		if !ok {
			// A knob removed from the registry while an override survived. Not
			// reachable today (Set validates against the registry), but listing
			// it is how it would be noticed rather than silently dropped.
			state = KnobState{Knob: Knob{Name: name}}
		}
		o := override
		state.Global = &o
		out[name] = state
	}
	for name, overrides := range s.service {
		state, ok := out[name]
		if !ok {
			state = KnobState{Knob: Knob{Name: name}}
		}
		state.ServiceOverrides = make(map[domain.ServiceID]Override, len(overrides))
		for serviceID, o := range overrides {
			state.ServiceOverrides[serviceID] = o
		}
		out[name] = state
	}
	return out
}

// ServiceOverrides returns the per-service overrides set on one knob, or nil
// when none are.
//
// It exists for a reader that has to act on every override rather than resolve
// one — the health-check scheduler, whose tick has to be short enough for the
// fastest cadence anyone has asked for, and which therefore cannot wait to be
// asked about a service to find out. A knob with per-service overrides that
// nothing enumerates is a knob that silently does nothing for the service it
// was set on, which is worse than not offering it.
func (s *Store) ServiceOverrides(name string) map[domain.ServiceID]Override {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byService := s.service[name]
	if len(byService) == 0 {
		return nil
	}
	out := make(map[domain.ServiceID]Override, len(byService))
	for id, o := range byService {
		out[id] = o
	}
	return out
}

// SetBase records what the config file says a knob is, so a reader of the
// admin API can see what an override is overriding. Wire time, from the same
// value the resolving closure was built with.
//
// It is display only. Nothing reads it to decide behaviour — the base still
// travels with each Int/Duration/Float call, because that is where a
// per-service config value can actually be resolved — so a stale or missing
// entry costs an operator context and costs a relay nothing.
func (s *Store) SetBase(name, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.base[name] = value
}

// Effective describes what is in force for one knob, for an operator asking
// rather than for a reader resolving.
type Effective struct {
	Knob Knob `json:"knob"`
	// Base is the config file's value, empty when nothing registered one.
	Base string `json:"base,omitempty"`
	// Value is what applies now: the global override if set, else Base.
	Value string `json:"value"`
	// Overridden says whether Value came from an override rather than config.
	Overridden bool `json:"overridden"`
	// Global and ServiceOverrides are the raw overrides behind the answer.
	Global           *Override                     `json:"global,omitempty"`
	ServiceOverrides map[domain.ServiceID]Override `json:"service_overrides,omitempty"`
}

// EffectiveFor reports what is in force for one knob globally, and whether the
// knob exists at all.
//
// Deliberately global-only. A per-service answer would have to invent the
// service's config base, which the store does not have and cannot derive — so
// it would be a confident guess, and the per-service overrides are listed here
// instead for the caller to read against their own config.
func (s *Store) EffectiveFor(name string) (Effective, bool) {
	knob, ok := Lookup(name)
	if !ok {
		return Effective{}, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	eff := Effective{Knob: knob, Base: s.base[name], Value: s.base[name]}
	if o, set := s.global[name]; set {
		override := o
		eff.Global = &override
		eff.Value = o.Value.Raw
		eff.Overridden = true
	}
	if byService := s.service[name]; len(byService) > 0 {
		eff.ServiceOverrides = make(map[domain.ServiceID]Override, len(byService))
		for id, o := range byService {
			eff.ServiceOverrides[id] = o
		}
	}
	return eff, true
}
