package tuning

import "testing"

// An operator asking what a knob is now was previously told only what had been
// overridden, and had to combine that with the config file themselves. On the
// mainnet canary on 2026-09-03 that meant nobody could tell whether a
// configured 500 workers had been clamped, rejected or honoured.
func TestEffectiveFor(t *testing.T) {
	s := NewStore()
	s.SetBase(KnobHealthCheckInterval, "60s")

	t.Run("the config value when nothing is overridden", func(t *testing.T) {
		eff, ok := s.EffectiveFor(KnobHealthCheckInterval)
		if !ok {
			t.Fatal("knob not found")
		}
		if eff.Value != "60s" || eff.Base != "60s" {
			t.Errorf("value=%q base=%q, want both 60s", eff.Value, eff.Base)
		}
		if eff.Overridden {
			t.Error("reported as overridden with no override set")
		}
	})

	t.Run("the override once one is set, with the base still visible", func(t *testing.T) {
		if err := s.Set(KnobHealthCheckInterval, "", "120s"); err != nil {
			t.Fatal(err)
		}
		eff, _ := s.EffectiveFor(KnobHealthCheckInterval)
		if eff.Value != "120s" {
			t.Errorf("value = %q, want the override 120s", eff.Value)
		}
		if eff.Base != "60s" {
			t.Errorf("base = %q, want the config value still shown — it is what the override is overriding", eff.Base)
		}
		if !eff.Overridden {
			t.Error("not reported as overridden")
		}
	})

	t.Run("per-service overrides are listed, not resolved", func(t *testing.T) {
		if err := s.Set(KnobHealthCheckInterval, "eth", "30s"); err != nil {
			t.Fatal(err)
		}
		eff, _ := s.EffectiveFor(KnobHealthCheckInterval)
		if got := eff.ServiceOverrides["eth"].Value.Raw; got != "30s" {
			t.Errorf("eth override = %q, want 30s", got)
		}
		// The global answer must not have moved: a per-service base cannot be
		// resolved here, so the endpoint says what it knows and no more.
		if eff.Value != "120s" {
			t.Errorf("global value = %q, want it unchanged by a service override", eff.Value)
		}
	})

	t.Run("a knob nobody registered a base for reads as unknown, not zero", func(t *testing.T) {
		eff, ok := s.EffectiveFor(KnobHedgeDelay)
		if !ok {
			t.Fatal("knob not found")
		}
		if eff.Base != "" || eff.Value != "" {
			t.Errorf("base=%q value=%q, want empty rather than a made-up zero", eff.Base, eff.Value)
		}
	})

	t.Run("an unknown knob is not found", func(t *testing.T) {
		if _, ok := s.EffectiveFor("nope.not.a.knob"); ok {
			t.Error("reported an unregistered knob as existing")
		}
	})
}
