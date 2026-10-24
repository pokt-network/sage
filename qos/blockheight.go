package qos

import "math"

const (
	// maxBlockHeightDelta is how far past the perceived height a stored height
	// may sit and still be read as one (HeightGetter). Further is a parsing bug
	// or a false answer.
	maxBlockHeightDelta = 1_000_000

	// MaxPlausibleBlockHeight is an absolute ceiling on any height a real chain
	// could report. It is deliberately far above every live chain — Solana, the
	// fastest, adds roughly 63M slots a year and would need ~15,000 years to
	// reach it — because its job is not to be a tight bound. Consensus already
	// bounds heights relative to its peers; this bounds them in absolute terms,
	// so no arithmetic downstream of an observation can be pushed near
	// math.MaxUint64 and made to wrap.
	MaxPlausibleBlockHeight = 1_000_000_000_000 // 1e12
)

// IsPlausibleBlockHeight reports whether a height could have come from a real
// chain: non-zero, and below the absolute ceiling.
//
// Zero is excluded because it is the "unknown" value everywhere in this
// package, not a height.
func IsPlausibleBlockHeight(height uint64) bool {
	return height > 0 && height <= MaxPlausibleBlockHeight
}

// saturatingAdd returns a+b, clamped to math.MaxUint64 instead of wrapping.
//
// Block-height arithmetic mixes a chain-reported number with an operator-set
// allowance, so neither operand is fully trusted. Wrapping turns a ceiling into
// a floor — the resulting tiny number excludes every honest height — which
// fails open rather than closed. Saturating keeps the comparison meaningful.
func saturatingAdd(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}

// saturatingMul returns a*b, clamped to math.MaxUint64 instead of wrapping.
func saturatingMul(a, b uint64) uint64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a > math.MaxUint64/b {
		return math.MaxUint64
	}
	return a * b
}
