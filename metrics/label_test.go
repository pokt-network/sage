package metrics

import (
	"fmt"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/pokt-network/sage/domain"
)

func TestBoundedLabel_AdmitsUpToCapThenCollapses(t *testing.T) {
	b := cappedLabel(3)

	for _, v := range []string{"relayer_proxy", "session", "supplier"} {
		if got := b.value(v); got != v {
			t.Errorf("value(%q) = %q, want it admitted", v, got)
		}
	}

	if got := b.value("one_too_many"); got != otherLabel {
		t.Errorf("value beyond the cap = %q, want %q", got, otherLabel)
	}
	// Admitted values keep their series after the cap is reached.
	if got := b.value("session"); got != "session" {
		t.Errorf("already-admitted value = %q, want %q", got, "session")
	}
}

// A label value arriving over the network can be invalid UTF-8, which
// client_golang panics on. Bounding must not lose that repair.
func TestBoundedLabel_SanitizesBeforeAdmitting(t *testing.T) {
	b := cappedLabel(4)

	if got := b.value("bad\xff\xfeutf8"); got == "bad\xff\xfeutf8" {
		t.Error("invalid UTF-8 should have been repaired")
	}

	long := make([]byte, maxLabelLen*2)
	for i := range long {
		long[i] = 'x'
	}
	if got := b.value(string(long)); len(got) > maxLabelLen {
		t.Errorf("label length = %d, want ≤ %d", len(got), maxLabelLen)
	}
}

func TestBoundedLabel_ConcurrentUseStaysWithinCap(t *testing.T) {
	const cap = 8
	b := cappedLabel(cap)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				b.value(fmt.Sprintf("codespace-%d", (i*50+j)%40))
			}
		}(i)
	}
	wg.Wait()

	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.seen) > cap {
		t.Errorf("admitted %d distinct values, want ≤ %d", len(b.seen), cap)
	}
}

// The three policies exist because label values arrive from three places that
// fail differently. What they share is the floor: none of them can hand
// client_golang a value that panics it.
func TestLabelPolicies(t *testing.T) {
	invalid := "eth\xff\xc0\xae"

	t.Run("open admits everything but still sanitizes", func(t *testing.T) {
		p := openLabel()
		if got := p.value("anything-at-all"); got != "anything-at-all" {
			t.Errorf("value = %q, want it admitted unchanged", got)
		}
		if got := p.value(invalid); !utf8.ValidString(got) {
			t.Errorf("value = %q, which is not valid UTF-8 — client_golang panics on that", got)
		}
	})

	t.Run("allowed collapses anything unconfigured", func(t *testing.T) {
		p := allowedLabel([]domain.ServiceID{"eth", "poly"})
		if got := p.serviceValue("eth"); got != "eth" {
			t.Errorf("configured service = %q, want %q", got, "eth")
		}
		if got := p.serviceValue("made-up"); got != unknownLabel {
			t.Errorf("unconfigured service = %q, want %q", got, unknownLabel)
		}
		if got := p.value(invalid); got != unknownLabel {
			t.Errorf("hostile value = %q, want %q", got, unknownLabel)
		}
	})

	t.Run("capped admits the first n and collapses the rest", func(t *testing.T) {
		p := cappedLabel(3)
		for _, v := range []string{"a", "b", "c"} {
			if got := p.value(v); got != v {
				t.Errorf("value(%q) = %q, want it admitted", v, got)
			}
		}
		if got := p.value("d"); got != otherLabel {
			t.Errorf("value past the cap = %q, want %q", got, otherLabel)
		}
		// An already-admitted value keeps its series once the cap is spent.
		if got := p.value("a"); got != "a" {
			t.Errorf("admitted value = %q after the cap filled, want %q", got, "a")
		}
	})

	t.Run("zero value is usable", func(t *testing.T) {
		var p labelPolicy
		if got := p.value(invalid); !utf8.ValidString(got) {
			t.Errorf("zero-value policy returned invalid UTF-8: %q", got)
		}
	})
}
