package tuning

import (
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// domainServiceID keeps the helper calls readable; the store is keyed by the
// domain type, the tests are written with plain strings.
func domainServiceID(s string) domain.ServiceID { return domain.ServiceID(s) }

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		knob    string
		raw     string
		wantErr string
		check   func(*testing.T, Value)
	}{
		{
			name: "int",
			knob: KnobRetryMaxRetries,
			raw:  "3",
			check: func(t *testing.T, v Value) {
				if v.Int != 3 {
					t.Fatalf("Int = %d, want 3", v.Int)
				}
			},
		},
		{
			name: "duration keeps what the operator typed",
			knob: KnobHedgeDelay,
			raw:  "250ms",
			check: func(t *testing.T, v Value) {
				if v.Dur != 250*time.Millisecond {
					t.Fatalf("Dur = %s, want 250ms", v.Dur)
				}
				if v.Raw != "250ms" {
					t.Fatalf("Raw = %q, want the original notation", v.Raw)
				}
			},
		},
		{
			name:    "unknown knob names the ones that exist",
			knob:    "retry.max_retrys",
			raw:     "3",
			wantErr: KnobRetryMaxRetries,
		},
		{
			name:    "wrong type",
			knob:    KnobRetryMaxRetries,
			raw:     "250ms",
			wantErr: "not a whole number",
		},
		{
			name:    "duration that is not a duration",
			knob:    KnobHedgeDelay,
			raw:     "250",
			wantErr: "not a duration",
		},
		{
			// Refused, not clamped: an operator who typed this has made a
			// mistake, and storing 300s instead leaves them believing the 900.
			name:    "above the range",
			knob:    KnobRelayTimeout,
			raw:     "900s",
			wantErr: "outside the accepted range",
		},
		{
			name:    "below the range",
			knob:    KnobRelayTimeout,
			raw:     "1ms",
			wantErr: "outside the accepted range",
		},
		{
			name:    "empty",
			knob:    KnobRetryMaxRetries,
			raw:     "   ",
			wantErr: "a value is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.knob, tt.raw)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected an error containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.check(t, got)
		})
	}
}

func TestStore_Precedence(t *testing.T) {
	s := NewStore()

	// Nothing set: the config value is what a caller gets back.
	if got := s.Int(KnobRetryMaxRetries, "eth", 2); got != 2 {
		t.Fatalf("with no override, got %d, want the base 2", got)
	}

	if err := s.Set(KnobRetryMaxRetries, "", "5"); err != nil {
		t.Fatalf("set global: %v", err)
	}
	if got := s.Int(KnobRetryMaxRetries, "eth", 2); got != 5 {
		t.Fatalf("global override: got %d, want 5", got)
	}

	if err := s.Set(KnobRetryMaxRetries, "eth", "1"); err != nil {
		t.Fatalf("set per-service: %v", err)
	}
	if got := s.Int(KnobRetryMaxRetries, "eth", 2); got != 1 {
		t.Fatalf("per-service override: got %d, want 1", got)
	}
	if got := s.Int(KnobRetryMaxRetries, "poly", 2); got != 5 {
		t.Fatalf("another service should still see the global: got %d, want 5", got)
	}
}

// TestStore_DeleteGlobalLeavesServiceOverrides pins the asymmetry: the
// per-service value is the narrower statement, and clearing the global must not
// revert a service the operator did not name.
func TestStore_DeleteGlobalLeavesServiceOverrides(t *testing.T) {
	s := NewStore()
	mustSet(t, s, KnobHedgeDelay, "", "500ms")
	mustSet(t, s, KnobHedgeDelay, "eth", "100ms")

	if !s.Delete(KnobHedgeDelay, "") {
		t.Fatal("expected the global override to have existed")
	}

	if got := s.Duration(KnobHedgeDelay, "eth", time.Second); got != 100*time.Millisecond {
		t.Fatalf("eth = %s, want its own 100ms to survive", got)
	}
	if got := s.Duration(KnobHedgeDelay, "poly", time.Second); got != time.Second {
		t.Fatalf("poly = %s, want the config base back", got)
	}
	if s.Delete(KnobHedgeDelay, "") {
		t.Fatal("deleting twice should report nothing was there")
	}
}

func TestStore_AllListsEveryKnob(t *testing.T) {
	s := NewStore()
	mustSet(t, s, KnobRelayTimeout, "eth", "5s")

	all := s.All()
	if len(all) != len(Knobs) {
		t.Fatalf("All returned %d knobs, want all %d registered", len(all), len(Knobs))
	}

	untouched, ok := all[KnobRetryMaxRetries]
	if !ok {
		t.Fatal("a knob nobody has set must still be listed")
	}
	if untouched.Global != nil || len(untouched.ServiceOverrides) != 0 {
		t.Fatal("an untouched knob must not carry overrides")
	}
	if untouched.Knob.Description == "" {
		t.Fatal("the listing must carry the description a UI renders")
	}

	touched := all[KnobRelayTimeout]
	if touched.ServiceOverrides["eth"].Value.Raw != "5s" {
		t.Fatalf("expected eth's override to be listed, got %+v", touched)
	}
	if touched.ServiceOverrides["eth"].SetAt.IsZero() {
		t.Fatal("an override must record when it was set")
	}
}

// TestStore_NilIsInert covers a gateway built without tuning: reads fall
// through to the config value and writes are refused rather than accepted into
// nothing.
func TestStore_NilIsInert(t *testing.T) {
	var s *Store

	if got := s.Int(KnobRetryMaxRetries, "eth", 2); got != 2 {
		t.Fatalf("nil store read: got %d, want the base 2", got)
	}
	if got := s.Duration(KnobHedgeDelay, "eth", time.Second); got != time.Second {
		t.Fatalf("nil store read: got %s, want the base", got)
	}
	if err := s.Set(KnobRetryMaxRetries, "", "3"); err == nil {
		t.Fatal("writing to a nil store must report that it went nowhere")
	}
	if s.Delete(KnobRetryMaxRetries, "") {
		t.Fatal("nil store delete should report nothing")
	}
	if len(s.All()) != len(Knobs) {
		t.Fatal("a nil store still lists what exists")
	}
}

func mustSet(t *testing.T, s *Store, knob string, serviceID, raw string) {
	t.Helper()
	if err := s.Set(knob, domainServiceID(serviceID), raw); err != nil {
		t.Fatalf("set %s=%s: %v", knob, raw, err)
	}
}
