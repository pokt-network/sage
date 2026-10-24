package qos

import (
	"math"
	"testing"
)

func TestIsPlausibleBlockHeight(t *testing.T) {
	cases := []struct {
		height uint64
		want   bool
	}{
		{0, false},                           // "unknown", not a height
		{1, true},                            // genesis-ish
		{20_000_000, true},                   // ethereum
		{250_000_000, true},                  // solana slots
		{MaxPlausibleBlockHeight, true},      // the ceiling is inclusive
		{MaxPlausibleBlockHeight + 1, false}, // one over
		{math.MaxUint64, false},              // the value that used to wrap the cap
	}
	for _, tc := range cases {
		if got := IsPlausibleBlockHeight(tc.height); got != tc.want {
			t.Errorf("IsPlausibleBlockHeight(%d) = %v, want %v", tc.height, got, tc.want)
		}
	}
}

func TestSaturatingAdd(t *testing.T) {
	cases := []struct{ a, b, want uint64 }{
		{1, 2, 3},
		{0, 0, 0},
		{math.MaxUint64, 1, math.MaxUint64},
		{math.MaxUint64 - 5, 10, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64},
	}
	for _, tc := range cases {
		if got := saturatingAdd(tc.a, tc.b); got != tc.want {
			t.Errorf("saturatingAdd(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestSaturatingMul(t *testing.T) {
	cases := []struct{ a, b, want uint64 }{
		{3, 4, 12},
		{0, math.MaxUint64, 0},
		{math.MaxUint64, 0, 0},
		{math.MaxUint64, 2, math.MaxUint64},
		{math.MaxUint64/3 + 1, 3, math.MaxUint64},
	}
	for _, tc := range cases {
		if got := saturatingMul(tc.a, tc.b); got != tc.want {
			t.Errorf("saturatingMul(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}
