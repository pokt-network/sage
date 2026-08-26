package config

import (
	"testing"
	"time"
)

func TestMethodBlocksConfig_Effective(t *testing.T) {
	cases := []struct {
		cfg     MethodBlocksConfig
		wantTTL time.Duration
		wantEsc int
	}{
		{MethodBlocksConfig{}, 5 * time.Minute, 3},
		{MethodBlocksConfig{TTL: time.Minute, EscalationThreshold: 5}, time.Minute, 5},
		{MethodBlocksConfig{TTL: -1, EscalationThreshold: -1}, 0, 0},
	}
	for _, tc := range cases {
		if got := tc.cfg.EffectiveTTL(); got != tc.wantTTL {
			t.Errorf("%+v: EffectiveTTL = %v, want %v", tc.cfg, got, tc.wantTTL)
		}
		if got := tc.cfg.EffectiveEscalation(); got != tc.wantEsc {
			t.Errorf("%+v: EffectiveEscalation = %d, want %d", tc.cfg, got, tc.wantEsc)
		}
	}
}
