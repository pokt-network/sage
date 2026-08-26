package methodblock

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStore_MarkBlocksOnlyThatMethod(t *testing.T) {
	s := New()
	s.Mark("solana", "slow.example.com", "getProgramAccounts", true)

	if !s.Blocked("solana", "slow.example.com", "getProgramAccounts") {
		t.Fatal("marked method must be blocked")
	}
	if s.Blocked("solana", "slow.example.com", "getSlot") {
		t.Fatal("a different method on the same host must not be blocked")
	}
	if s.Blocked("solana", "other.example.com", "getProgramAccounts") {
		t.Fatal("a different host must not be blocked")
	}
	if s.Blocked("eth", "slow.example.com", "getProgramAccounts") {
		t.Fatal("a different service must not be blocked")
	}
}

func TestStore_MarkExpires(t *testing.T) {
	s := New(WithTTL(20 * time.Millisecond))
	s.Mark("eth", "h.example.com", "eth_getLogs", true)
	time.Sleep(30 * time.Millisecond)
	if s.Blocked("eth", "h.example.com", "eth_getLogs") {
		t.Fatal("mark must expire after the TTL")
	}
}

// A re-mark refreshes to one TTL from now; it never extends past that. A
// method mark is cheap to be wrong about, so it must not accumulate.
func TestStore_RemarkRefreshesDoesNotExtend(t *testing.T) {
	s := New(WithTTL(time.Hour))
	s.Mark("eth", "h.example.com", "eth_getLogs", true)
	first := s.Active("eth")[0].Expiry
	time.Sleep(5 * time.Millisecond)
	s.Mark("eth", "h.example.com", "eth_getLogs", true)
	second := s.Active("eth")[0].Expiry
	if !second.After(first) {
		t.Fatal("re-mark must refresh the expiry")
	}
	if time.Until(second) > time.Hour+time.Second {
		t.Fatal("re-mark must not extend past one TTL from now")
	}
	if len(s.Active("eth")) != 1 {
		t.Fatalf("re-mark created a second block: %+v", s.Active("eth"))
	}
}

func TestStore_ThirdDistinctMethodEscalatesToHost(t *testing.T) {
	s := New()
	if s.Mark("eth", "dead.example.com", "eth_getLogs", true) {
		t.Fatal("first mark must not escalate")
	}
	if s.Mark("eth", "dead.example.com", "eth_call", true) {
		t.Fatal("second mark must not escalate")
	}
	if !s.Mark("eth", "dead.example.com", "eth_getBalance", true) {
		t.Fatal("third distinct method must escalate")
	}
	if !s.Blocked("eth", "dead.example.com", "eth_blockNumber") {
		t.Fatal("host block must cover a method that was never marked")
	}
	active := s.Active("eth")
	if len(active) != 1 || active[0].Method != "" {
		t.Fatalf("escalation must collapse the method marks into one host block, got %+v", active)
	}
}

// The finding this test exists for: -32601 is MethodBlocking but
// client-attributed, and a healthy node without debug_*/trace_* answers it to
// every catalogued method a client cares to ask for. Three such marks must
// keep three methods away from the host and leave the host itself serving
// everything else.
func TestStore_NonEscalatingMarksNeverHostBlock(t *testing.T) {
	s := New()
	for _, m := range []string{"debug_traceCall", "trace_block", "debug_storageRangeAt"} {
		if s.Mark("eth", "healthy.example.com", m, false) {
			t.Fatalf("client-attributed mark of %q escalated", m)
		}
	}
	if s.Blocked("eth", "healthy.example.com", "eth_call") {
		t.Fatal("client-attributed marks host-blocked a healthy node")
	}
	if !s.Blocked("eth", "healthy.example.com", "debug_traceCall") {
		t.Fatal("a non-escalating mark must still block its own method")
	}
	if len(s.Active("eth")) != 3 {
		t.Fatalf("want three method blocks, got %+v", s.Active("eth"))
	}
}

// A non-escalating mark must not be counted toward a later escalating mark's
// live count: two -32601s plus one real timeout is one timeout of evidence.
func TestStore_NonEscalatingMarksDoNotCountTowardEscalation(t *testing.T) {
	s := New() // escalation 3
	s.Mark("eth", "h.example.com", "debug_traceCall", false)
	s.Mark("eth", "h.example.com", "trace_block", false)
	if s.Mark("eth", "h.example.com", "eth_getLogs", true) {
		t.Fatal("two client marks plus one supplier mark must not escalate")
	}
	if s.Blocked("eth", "h.example.com", "eth_call") {
		t.Fatal("host must not be blocked")
	}
	// Two more supplier marks reach the threshold on supplier evidence alone.
	s.Mark("eth", "h.example.com", "eth_call", true)
	if !s.Mark("eth", "h.example.com", "eth_getBalance", true) {
		t.Fatal("three supplier marks must escalate")
	}
}

// A supplier-attributed mark inside its live window is not un-counted by a
// later client-attributed mark on the same method.
func TestStore_SupplierMarkStickyAgainstLaterClientMark(t *testing.T) {
	s := New()
	s.Mark("eth", "h.example.com", "eth_getLogs", true)
	s.Mark("eth", "h.example.com", "eth_getLogs", false) // must not downgrade
	s.Mark("eth", "h.example.com", "eth_call", true)
	if !s.Mark("eth", "h.example.com", "eth_getBalance", true) {
		t.Fatal("a client re-mark erased supplier evidence")
	}
}

// Three re-marks of ONE method are not three methods.
func TestStore_RemarksOfOneMethodDoNotEscalate(t *testing.T) {
	s := New()
	for i := 0; i < 5; i++ {
		if s.Mark("eth", "h.example.com", "eth_getLogs", true) {
			t.Fatalf("re-mark %d escalated", i)
		}
	}
	if s.Blocked("eth", "h.example.com", "eth_call") {
		t.Fatal("host must not be blocked")
	}
}

// Marks spaced past the TTL are never simultaneously live, so they must
// never accumulate into an escalation. This fails if Mark stops deleting
// expired methods when counting live ones.
func TestStore_MarksSpacedPastTTLDoNotEscalate(t *testing.T) {
	s := New(WithTTL(10 * time.Millisecond))
	for _, m := range []string{"a", "b", "c"} {
		if s.Mark("eth", "h.example.com", m, true) {
			t.Fatalf("mark of %q escalated", m)
		}
		time.Sleep(15 * time.Millisecond)
	}
	if s.Blocked("eth", "h.example.com", "d") {
		t.Fatal("host must not be blocked")
	}
}

func TestStore_EscalationThresholdZeroNeverEscalates(t *testing.T) {
	s := New(WithEscalation(0))
	for _, m := range []string{"a", "b", "c", "d"} {
		if s.Mark("eth", "h.example.com", m, true) {
			t.Fatal("escalation disabled must never escalate")
		}
	}
	if s.Blocked("eth", "h.example.com", "e") {
		t.Fatal("host must not be blocked")
	}
}

func TestStore_TTLZeroDisablesMarking(t *testing.T) {
	s := New(WithTTL(0))
	s.Mark("eth", "h.example.com", "eth_getLogs", true)
	if s.Blocked("eth", "h.example.com", "eth_getLogs") {
		t.Fatal("TTL <= 0 must disable marking entirely")
	}
}

func TestStore_ClearDropsMarksAndEscalationState(t *testing.T) {
	s := New()
	s.Mark("eth", "h.example.com", "a", true)
	s.Mark("eth", "h.example.com", "b", true)
	if n := s.Clear("eth"); n != 2 {
		t.Fatalf("Clear returned %d, want 2", n)
	}
	if s.Blocked("eth", "h.example.com", "a") {
		t.Fatal("cleared mark still blocks")
	}
	if s.Mark("eth", "h.example.com", "c", true) {
		t.Fatal("a mark after Clear must count as the first, not the third")
	}
}

func TestStore_EmptyHostOrMethodIsNoop(t *testing.T) {
	s := New()
	s.Mark("eth", "", "a", true)
	s.Mark("eth", "h.example.com", "", true)
	if len(s.Active("eth")) != 0 {
		t.Fatalf("empty key produced a block: %+v", s.Active("eth"))
	}
}

func TestStore_SweepDropsExpiredHosts(t *testing.T) {
	s := New(WithTTL(10 * time.Millisecond))
	s.Mark("eth", "h.example.com", "a", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.sweepInterval = 5 * time.Millisecond
	s.StartSweep(ctx)
	time.Sleep(40 * time.Millisecond)
	s.mu.RLock()
	_, present := s.byService["eth"]["h.example.com"]
	s.mu.RUnlock()
	if present {
		t.Fatal("sweep must remove a host whose every mark expired")
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				s.Mark("eth", "h.example.com", string(rune('a'+i)), true)
				s.Blocked("eth", "h.example.com", "z")
				s.Active("eth")
			}
		}(i)
	}
	wg.Wait()
}
