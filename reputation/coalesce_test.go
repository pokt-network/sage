package reputation

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func stateOf(t *testing.T, s *serviceImpl, ep domain.EndpointAddr) State {
	t.Helper()
	key := s.keyOf(ep, domain.RPCTypeJSONRPC)
	sh := s.shard(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.cache["solana"][key]
}

func timeoutAt(ts time.Time) Signal {
	sig := NewMajorErrorSignal(reasonTransportTimeout, 0)
	sig.Timestamp = ts
	return sig
}

// Timeouts landing together cost the additive score once per window, while
// the failure rate counts every one; other major errors are charged each.
func TestRecordSignal_CoalescesTimeoutBursts(t *testing.T) {
	ctx := context.Background()
	ep := domain.EndpointAddr("s1-https://solana.example.network")
	probe, _ := newTestServiceStore()
	major := probe.scoring.Load().impacts.Impact(SignalMajorError)

	t.Run("a burst costs one hit, the rate counts all", func(t *testing.T) {
		s, _ := newTestServiceStore()
		t0 := time.Now()
		for i := range 6 {
			_ = s.RecordSignal(ctx, "solana", ep, domain.RPCTypeJSONRPC, timeoutAt(t0.Add(time.Duration(i)*100*time.Millisecond)))
		}
		st := stateOf(t, s, ep)
		if st.Score != 100+major {
			t.Fatalf("score %v, want %v: six timeouts inside a second are one hit", st.Score, 100+major)
		}
		one, _ := newTestServiceStore()
		_ = one.RecordSignal(ctx, "solana", ep, domain.RPCTypeJSONRPC, timeoutAt(t0))
		if st.Rate <= stateOf(t, one, ep).Rate || st.Attempts != 6 {
			t.Fatalf("rate %v attempts %d: every timeout must feed the rate", st.Rate, st.Attempts)
		}
	})
	t.Run("sustained timeouts still floor the key", func(t *testing.T) {
		s, _ := newTestServiceStore()
		t0 := time.Now()
		for i := range 200 {
			_ = s.RecordSignal(ctx, "solana", ep, domain.RPCTypeJSONRPC, timeoutAt(t0.Add(time.Duration(i)*100*time.Millisecond)))
		}
		if st := stateOf(t, s, ep); st.Score != 0 {
			t.Fatalf("score %v after 20s of timeouts, want 0", st.Score)
		}
	})
	t.Run("other major errors are charged each", func(t *testing.T) {
		s, _ := newTestServiceStore()
		t0 := time.Now()
		for i := range 3 {
			sig := NewMajorErrorSignal("upstream_5xx", 0)
			sig.Timestamp = t0.Add(time.Duration(i) * 100 * time.Millisecond)
			_ = s.RecordSignal(ctx, "solana", ep, domain.RPCTypeJSONRPC, sig)
		}
		if st := stateOf(t, s, ep); st.Score != 100+3*major {
			t.Fatalf("score %v, want %v", st.Score, 100+3*major)
		}
	})
}
