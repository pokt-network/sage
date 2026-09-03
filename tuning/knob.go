// Package tuning holds runtime overrides for the numeric settings a config
// file would otherwise fix until the next restart.
//
// It is the same shape as featureflag: config is the base, an override map is
// consulted on each read, and a per-service override beats a global one. The
// difference is only what a value is — a flag is a bool, a knob is a duration,
// a count or a ratio, so it needs bounds and a type.
//
// What this deliberately is NOT is a second configuration system. A knob exists
// here only if it is read on the request path through a function the middleware
// already calls; anything decided once at wire time (a listen address, a
// storage backend, a plugin's chain rules) cannot be changed by writing to a
// map and must not pretend otherwise. Adding a knob whose value is captured at
// startup would produce an admin API that accepts a change and does nothing —
// the exact failure the config Inert list exists to end.
package tuning

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Kind is a knob's value type. It decides how a submitted string is parsed and
// how the admin UI renders the control.
type Kind string

const (
	// KindInt is a plain count.
	KindInt Kind = "int"
	// KindDuration is a Go duration string ("250ms", "3s").
	KindDuration Kind = "duration"
	// KindFloat is a ratio or multiplier.
	KindFloat Kind = "float"
)

// Canonical knob names. Use the constant, never the string: a typo in a string
// literal produces a knob nobody can set and no error anywhere.
const (
	// KnobRetryMaxRetries overrides retry_config.max_retries.
	KnobRetryMaxRetries = "retry.max_retries"
	// KnobRetryMaxLatency overrides retry_config.max_latency.
	KnobRetryMaxLatency = "retry.max_latency"
	// KnobHedgeDelay overrides retry_config.hedge_delay.
	KnobHedgeDelay = "retry.hedge_delay"
	// KnobRelayTimeout overrides timeout_config.relay_timeout.
	KnobRelayTimeout = "timeout.relay_timeout"
	// KnobHealthCheckInterval overrides active_health_checks.interval.
	KnobHealthCheckInterval = "health_checks.interval"
)

// Knob describes one overridable setting.
type Knob struct {
	// Name is the canonical name, and the path segment the admin API uses.
	Name string `json:"name"`
	// Kind is the value type.
	Kind Kind `json:"kind"`
	// Description is written for whoever is about to change it in a hurry.
	Description string `json:"description"`
	// Min and Max bound the accepted value, inclusive. A submission outside
	// them is refused rather than clamped: an operator who typed 900s for a
	// relay timeout has made a mistake, and silently storing 60s instead would
	// leave them believing the 900.
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	// Unit labels Min/Max for humans. Durations are bounded in milliseconds.
	Unit string `json:"unit"`
}

// Knobs is the registry: every setting that can be overridden at runtime.
//
// Adding one means adding it here AND reading it at the point the value is
// used — see the closures in cmd/sagegw.Build. A knob registered but never read
// is worse than no knob at all.
var Knobs = []Knob{
	{
		Name:        KnobRetryMaxRetries,
		Kind:        KindInt,
		Description: "Additional attempts after the first. 0 disables retrying.",
		Min:         0,
		Max:         10,
		Unit:        "attempts",
	},
	{
		Name:        KnobRetryMaxLatency,
		Kind:        KindDuration,
		Description: "Total time budget across attempts; a retry is not started once it is spent. 0 means no budget.",
		Min:         0,
		Max:         120_000,
		Unit:        "ms",
	},
	{
		Name:        KnobHedgeDelay,
		Kind:        KindDuration,
		Description: "How long to wait for the primary before racing a second endpoint. 0 disables hedging.",
		Min:         0,
		Max:         60_000,
		Unit:        "ms",
	},
	{
		Name:        KnobRelayTimeout,
		Kind:        KindDuration,
		Description: "Ceiling on one relay, applied by the timeout middleware around the whole chain.",
		Min:         100,
		Max:         300_000,
		Unit:        "ms",
	},
	{
		Name: KnobHealthCheckInterval,
		Kind: KindDuration,
		// Written for whoever is about to change it in a hurry, which here
		// means: this is a spend dial, and it is the only one whose cost is
		// paid in relays rather than latency.
		Description: "How often every backend of every service is probed. Each probe is a paid relay, so halving this doubles health-check spend — on the mainnet canary probes were 13.7% of all relay volume at 60s. Raising it trades staleness for spend: an endpoint's health signal is at best this old, and at worst a whole health-check cycle old (see sage_health_check_cycle_seconds, which is the cadence actually being achieved).",
		Min:         1_000,
		Max:         3_600_000,
		Unit:        "ms",
	},
}

// knobsByName indexes the registry once. Lookups happen on the request path.
var knobsByName = func() map[string]Knob {
	m := make(map[string]Knob, len(Knobs))
	for _, k := range Knobs {
		m[k.Name] = k
	}
	return m
}()

// Lookup returns a knob by name.
func Lookup(name string) (Knob, bool) {
	k, ok := knobsByName[name]
	return k, ok
}

// KnobNames returns every registered name, sorted — for error messages that
// tell the caller what they could have said instead.
func KnobNames() []string {
	out := make([]string, 0, len(knobsByName))
	for name := range knobsByName {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Value is a parsed knob value. Only the field matching the knob's Kind is
// meaningful; the others are zero.
type Value struct {
	Int   int           `json:"int,omitempty"`
	Dur   time.Duration `json:"-"`
	Float float64       `json:"float,omitempty"`
	// Raw is what the operator typed, kept for display so "250ms" comes back
	// as "250ms" rather than as 250000000.
	Raw string `json:"raw"`
}

// Parse turns a submitted string into a Value, refusing anything the knob's
// kind or bounds do not allow.
func Parse(name, raw string) (Value, error) {
	knob, ok := Lookup(name)
	if !ok {
		return Value{}, fmt.Errorf("unknown knob %q; registered knobs are: %s",
			name, strings.Join(KnobNames(), ", "))
	}

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Value{}, fmt.Errorf("%s: a value is required", name)
	}

	switch knob.Kind {
	case KindInt:
		n, err := strconv.Atoi(raw)
		if err != nil {
			return Value{}, fmt.Errorf("%s: %q is not a whole number", name, raw)
		}
		if err := checkBounds(knob, float64(n), raw); err != nil {
			return Value{}, err
		}
		return Value{Int: n, Raw: raw}, nil

	case KindDuration:
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Value{}, fmt.Errorf("%s: %q is not a duration (try \"250ms\" or \"3s\")", name, raw)
		}
		if err := checkBounds(knob, float64(d.Milliseconds()), raw); err != nil {
			return Value{}, err
		}
		return Value{Dur: d, Raw: raw}, nil

	case KindFloat:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return Value{}, fmt.Errorf("%s: %q is not a number", name, raw)
		}
		if err := checkBounds(knob, f, raw); err != nil {
			return Value{}, err
		}
		return Value{Float: f, Raw: raw}, nil

	default:
		return Value{}, fmt.Errorf("%s: knob has unknown kind %q", name, knob.Kind)
	}
}

// checkBounds refuses an out-of-range value, naming the range.
func checkBounds(knob Knob, got float64, raw string) error {
	if got < knob.Min || got > knob.Max {
		return fmt.Errorf("%s: %q is outside the accepted range %g–%g %s",
			knob.Name, raw, knob.Min, knob.Max, knob.Unit)
	}
	return nil
}
