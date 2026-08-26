package circuitbreaker

import (
	"testing"
	"time"
)

// driveAtRate feeds the gate total outcomes with failPct of them failing,
// interleaved so the running rate is representative rather than front-loaded.
// It stops at the first break: production cannot keep feeding a broken domain
// (it is filtered out of selection), and MarkBroken short-circuits without
// recording while broken, so continuing would let successes pile up unopposed
// and poison the NEXT episode's rate.
func driveAtRate(b *Breaker, serviceID, domain string, total, failPct int) bool {
	acc := 0
	for i := 0; i < total; i++ {
		acc += failPct
		if acc >= 100 {
			acc -= 100
			if b.MarkBroken(serviceID, domain, "simulated") {
				return true
			}
			continue
		}
		b.RecordSuccess(serviceID, domain)
	}
	return b.IsBroken(serviceID, domain)
}

// oneWindowBreaker keeps every outcome in a single window so the tests measure
// hysteresis and nothing else — a rollover mid-drive would reset the counts.
func oneWindowBreaker(opts ...Option) *Breaker {
	base := []Option{WithFailureRateGate(time.Hour, defaultMinFailures, defaultFailureRateThreshold)}
	return New(append(base, opts...)...)
}

// A domain whose failure rate sits just above the threshold must be removed
// ONCE, and must NOT be removed again every time its TTL lets it back in.
//
// PATH's measured production case (2026-08-21): relay-miner hosts at 78–80%
// success against an 80% line flapped for 69–92% of a six-hour window, held
// out by escalating TTLs, while one of them answered 40 consecutive probes
// with zero errors.
func TestBreaker_MarginalDomainDoesNotFlap(t *testing.T) {
	b := oneWindowBreaker()
	const domain = "marginal.example.com"

	// 21% failure — just past the 20% threshold.
	if !driveAtRate(b, "solana", domain, 200, 21) {
		t.Fatal("a domain over the threshold must break the first time")
	}
	expireBreak(t, b, "solana", domain)
	if b.IsBroken("solana", domain) {
		t.Fatal("break did not expire; test cannot measure re-break")
	}

	// Same behaviour again: still marginal, not newly worse.
	if driveAtRate(b, "solana", domain, 200, 21) {
		t.Fatal("marginal domain re-broke after readmission: this is the flap — " +
			"it never gets the traffic to prove itself and is held out for escalating TTLs")
	}
}

// The margin must not disarm the breaker. A domain genuinely far past the line
// has to break again after readmission, and has to escalate — otherwise this
// trades one bug for a worse one. Production shape: hosts at ~50% success
// against the same line the marginal hosts sat just under.
func TestBreaker_BadlyBrokenDomainStillRebreaksAndEscalates(t *testing.T) {
	b := oneWindowBreaker()
	const domain = "dead.example.com"

	if !driveAtRate(b, "solana", domain, 200, 50) {
		t.Fatal("first break")
	}
	expireBreak(t, b, "solana", domain)

	if !driveAtRate(b, "solana", domain, 200, 50) {
		t.Fatal("a domain at 50% failure must re-break after readmission")
	}
	b.mu.RLock()
	hit := b.broken["solana"][domain].HitCount
	b.mu.RUnlock()
	if hit != 2 {
		t.Errorf("HitCount = %d after re-break, want 2 (escalated)", hit)
	}
}

// A host with no successes at all must still break on readmission: the margin
// raises the line to 35%, and 100% is past it. PATH tried an attempt-count
// floor alongside the margin and dropped it for sparing exactly this host.
func TestBreaker_ZeroSuccessDomainStillRebreaks(t *testing.T) {
	b := oneWindowBreaker()
	const domain = "silent.example.com"

	if !driveAtRate(b, "solana", domain, 20, 100) {
		t.Fatal("first break")
	}
	expireBreak(t, b, "solana", domain)

	broke := false
	for i := 0; i < defaultMinFailures && !broke; i++ {
		broke = b.MarkBroken("solana", domain, "still dead")
	}
	if !broke {
		t.Fatal("a domain with zero successes must re-break within minFailures")
	}
}

// The margin shares escalation's memory: once the last episode has aged out,
// the domain is judged as a first offender on the plain threshold again —
// and, consistently, its next break is not escalated either.
func TestBreaker_RebreakMarginLapsesWithEscalationMemory(t *testing.T) {
	b := oneWindowBreaker(WithEscalationMemory(20 * time.Millisecond))
	const domain = "marginal.example.com"

	if !driveAtRate(b, "solana", domain, 200, 21) {
		t.Fatal("first break")
	}
	expireBreak(t, b, "solana", domain)
	time.Sleep(30 * time.Millisecond)

	if !driveAtRate(b, "solana", domain, 200, 21) {
		t.Fatal("with the episode forgotten, the plain threshold applies again")
	}
	b.mu.RLock()
	hit := b.broken["solana"][domain].HitCount
	b.mu.RUnlock()
	if hit != 1 {
		t.Errorf("HitCount = %d, want 1: an episode the margin forgot must not escalate either", hit)
	}
}

// The outcome hook must see exactly what the gate counts — both sides of the
// fraction, and nothing while the domain is broken, when MarkBroken returns
// before the gate and the failure is not part of any rate.
func TestBreaker_OutcomeHookSeesWhatTheGateSees(t *testing.T) {
	b := firstErrorBreaker()
	var got []string
	b.SetOutcomeHook(func(serviceID, domain, outcome string) {
		got = append(got, serviceID+"/"+domain+"/"+outcome)
	})

	b.RecordSuccess("eth", "a.example.com")
	b.MarkBroken("eth", "a.example.com", "first") // breaks: first-error breaker
	b.MarkBroken("eth", "a.example.com", "again") // already broken: not counted
	b.RecordSuccess("eth", "a.example.com")       // still counted: it is the denominator

	want := []string{
		"eth/a.example.com/" + OutcomeSuccess,
		"eth/a.example.com/" + OutcomeFailure,
		"eth/a.example.com/" + OutcomeSuccess,
	}
	if len(got) != len(want) {
		t.Fatalf("hook calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call %d = %q, want %q", i, got[i], want[i])
		}
	}
}
