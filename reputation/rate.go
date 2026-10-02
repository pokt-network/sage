package reputation

import (
	"math"
	"time"
)

// Defaults for the chronic-failure rate term. The numbers were chosen against
// PATH's mainnet defect rates (three operators at 0.216%, 0.065% and
// 0.00003%) and a simulation of bursts on clean keys; docs/scoring.md §7.3
// has the table.
const (
	// DefaultHalfLifeAttempts is the EWMA half-life of the failure rate, in
	// attempts. Long on purpose: a 6-critical burst on a clean key must stay
	// under the onset, and only chronic behaviour should reach the penalty.
	DefaultHalfLifeAttempts = 20000
	// DefaultOnsetRate is the failure rate below which the term is zero.
	DefaultOnsetRate = 0.0002
	// DefaultFullRate is the failure rate at which the penalty is -40 points —
	// out of tier 1 and most of the way through tier 2.
	DefaultFullRate = 0.01

	penaltyAtFull = -40.0
	penaltyCap    = -70.0
)

// RateConfig parameterises the chronic-failure rate term of a reputation
// score. Zero values mean the defaults above; a negative HalfLifeAttempts
// turns the term off.
type RateConfig struct {
	// HalfLifeAttempts is the EWMA half-life in attempts. 0 = default,
	// negative = term off.
	HalfLifeAttempts int
	// OnsetRate is the failure rate at which the penalty starts. 0 = default.
	OnsetRate float64
	// FullRate is the failure rate at which the penalty reaches -40. 0 = default.
	FullRate float64
}

// Defaulted returns the config with its zero fields filled from the defaults
// and nothing else changed. Config validation reads the rates through this
// rather than through Normalized, so an inconsistent pair an operator actually
// wrote is refused at load time instead of being silently corrected into a
// curve they did not choose.
func (c RateConfig) Defaulted() RateConfig {
	if c.HalfLifeAttempts == 0 {
		c.HalfLifeAttempts = DefaultHalfLifeAttempts
	}
	if c.OnsetRate == 0 {
		c.OnsetRate = DefaultOnsetRate
	}
	if c.FullRate == 0 {
		c.FullRate = DefaultFullRate
	}
	return c
}

// Normalized returns the config with defaults filled in and an inconsistent
// onset/full pair reset. Every method below assumes it has been called;
// NewService calls it once.
func (c RateConfig) Normalized() RateConfig {
	c = c.Defaulted()
	// An inconsistent pair is treated as unset; config callers are refused
	// earlier by validateReputation, this guards programmatic callers. Penalty
	// divides by log10(FullRate/OnsetRate) and takes the log of a ratio of the
	// two, so a non-positive rate or an inverted pair yields NaN or a sign
	// flip: a score that silently stops being a number, or an endpoint
	// *rewarded* for failing. Both rates move together because the pair is what
	// is inconsistent — keeping one of them would build a curve nobody chose.
	if c.OnsetRate < 0 || c.FullRate < 0 || c.FullRate >= 1 || c.FullRate <= c.OnsetRate {
		c.OnsetRate = DefaultOnsetRate
		c.FullRate = DefaultFullRate
	}
	return c
}

// Enabled reports whether the term is on. Negative half-life is the off switch.
func (c RateConfig) Enabled() bool { return c.HalfLifeAttempts > 0 }

// Lambda is the EWMA step: rate += Lambda * (weight - rate).
func (c RateConfig) Lambda() float64 {
	if !c.Enabled() {
		return 0
	}
	return math.Ln2 / float64(c.HalfLifeAttempts)
}

// Penalty maps a failure rate to the points subtracted from the additive
// score: 0 up to OnsetRate, logarithmic to -40 at FullRate, continuing at the
// same slope to -70 (one decade past FullRate) and capped there.
func (c RateConfig) Penalty(rate float64) float64 {
	if !c.Enabled() || rate <= c.OnsetRate {
		return 0
	}
	span := math.Log10(c.FullRate / c.OnsetRate)
	if rate <= c.FullRate {
		return penaltyAtFull * math.Log10(rate/c.OnsetRate) / span
	}
	p := penaltyAtFull + (penaltyCap-penaltyAtFull)*math.Log10(rate/c.FullRate)
	if p < penaltyCap {
		return penaltyCap
	}
	return p
}

// FailureWeight is what one signal contributes to the failure rate: a
// critical or fatal error counts fully, a major error (a timeout, a 5xx)
// half, everything else zero. Client-attributed outcomes never reach the
// service and so are not attempts at all.
func FailureWeight(t SignalType) float64 {
	switch t {
	case SignalCriticalError, SignalFatalError:
		return 1
	case SignalMajorError:
		return 0.5
	default:
		return 0
	}
}

// wsRateHalfLife is how fast a WebSocket key's failure rate fades with time,
// on top of the per-attempt decay every key has.
//
// A WebSocket key earns few attempts: a connection records one success per
// wsSuccessSignalInterval (30s), so it cannot buy immunity by pushing frames,
// and a quiet chain carries one or two connections. At that volume the
// per-attempt half-life of 20,000 is weeks: on mainnet (2026-10-02) a poly key
// with one connection over four keys earned about 30 attempts an hour, a
// month per halving, and a host fixed today would have stayed demoted into
// November. Fading with time, a key that stops failing loses half its rate
// every six hours, about a day and a half from the -40 point to the onset,
// whatever its traffic; one that keeps failing holds a rate set by how often
// it fails per hour, which is what a connection-graded surface can measure.
const wsRateHalfLife = 6 * time.Hour

// decayWSRate fades rate by the time since the key's last signal (a Unix
// second, 0 when there was none). A signal stamped before it fades nothing.
func decayWSRate(rate float64, last int64, now time.Time) float64 {
	if rate == 0 || last == 0 {
		return rate
	}
	dt := now.Sub(time.Unix(last, 0))
	if dt <= 0 {
		return rate
	}
	return rate * math.Exp2(-float64(dt)/float64(wsRateHalfLife))
}
