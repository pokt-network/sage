package metrics

import (
	"fmt"
	"sync"
	"testing"
)

func TestBoundedLabel_AdmitsUpToCapThenCollapses(t *testing.T) {
	b := newBoundedLabel(3)

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
	b := newBoundedLabel(4)

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
	b := newBoundedLabel(cap)

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
