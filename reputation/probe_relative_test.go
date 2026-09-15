package reputation

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
)

// A probe success cannot lift a key that traffic is failing: the key keeps
// the score traffic gave it until probeDefer has passed without traffic.
func TestService_ProbeSuccessDoesNotOutvoteRecentTraffic(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1a-https://a.example")
	t0 := time.Now()

	fail := NewMajorErrorSignal("http_408", 0)
	fail.Timestamp = t0
	require.NoError(t, svc.RecordSignal(ctx, "sei", ep, domain.RPCTypeJSONRPC, fail))
	afterFail, _ := svc.GetScore(ctx, "sei", ep, domain.RPCTypeJSONRPC)
	require.Less(t, afterFail, 100.0)

	probe := NewSuccessSignal("health_check", 0)
	probe.Probe = true
	probe.Timestamp = t0.Add(time.Minute)
	require.NoError(t, svc.RecordSignal(ctx, "sei", ep, domain.RPCTypeJSONRPC, probe))
	got, _ := svc.GetScore(ctx, "sei", ep, domain.RPCTypeJSONRPC)
	assert.Equal(t, afterFail, got, "a probe a minute after failing traffic moved the score")

	probe.Timestamp = t0.Add(probeDefer + time.Minute)
	require.NoError(t, svc.RecordSignal(ctx, "sei", ep, domain.RPCTypeJSONRPC, probe))
	got, _ = svc.GetScore(ctx, "sei", ep, domain.RPCTypeJSONRPC)
	assert.Greater(t, got, afterFail, "with no traffic for probeDefer, a probe is how the key recovers")
}

// A benched key (additive below MinThreshold) still climbs back on probes even
// with recent traffic: the collapse fallback's occasional pick must not trap
// it at 0. Probes lift it into probation; from there traffic decides.
func TestService_ProbesStillLiftABenchedKey(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1a-https://a.example")
	t0 := time.Now()
	for i := 0; i < 4; i++ { // 4 criticals floor the additive term
		fail := NewCriticalErrorSignal("transport_connect_failed", 0)
		fail.Timestamp = t0
		require.NoError(t, svc.RecordSignal(ctx, "solana", ep, domain.RPCTypeJSONRPC, fail))
	}
	floored, _ := svc.GetScore(ctx, "solana", ep, domain.RPCTypeJSONRPC)
	require.Equal(t, 0.0, floored)

	probe := NewSuccessSignal("health_check", 0)
	probe.Probe = true
	probe.Timestamp = t0.Add(time.Minute) // traffic is recent
	for i := 0; i < 5; i++ {
		require.NoError(t, svc.RecordSignal(ctx, "solana", ep, domain.RPCTypeJSONRPC, probe))
	}
	got, _ := svc.GetScore(ctx, "solana", ep, domain.RPCTypeJSONRPC)
	assert.GreaterOrEqual(t, got, svc.selector.cfg.MinThreshold, "probes lift a benched key back into probation")
	assert.Less(t, got, svc.selector.cfg.ProbationThreshold, "and no further while traffic is recent")
}

// The chronic penalty is measured from the pool's best rate when the relative
// term is on: the best key pays nothing, a worse one pays the difference.
func TestService_ChronicPenaltyIsRelativeToThePool(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	best := domain.EndpointAddr("pokt1a-https://a.example")
	worse := domain.EndpointAddr("pokt1b-https://b.example")
	for ep, rate := range map[domain.EndpointAddr]float64{best: 0.013, worse: 0.015} {
		key := svc.keyOf(ep, domain.RPCTypeJSONRPC)
		sh := svc.shard(key)
		sh.mu.Lock()
		if sh.cache["sei"] == nil {
			sh.cache["sei"] = map[string]State{}
		}
		sh.cache["sei"][key] = State{Score: 100, Rate: rate, Attempts: 5000, TrafficAttempts: 5000}
		sh.mu.Unlock()
	}
	rc := svc.rate

	absolute, _ := svc.GetScore(ctx, "sei", worse, domain.RPCTypeJSONRPC)
	assert.InDelta(t, 100+rc.Penalty(0.015), absolute, 0.01, "no gate: the absolute penalty")

	svc.SetRelativeChronic(func(domain.ServiceID) bool { return true })
	svc.refreshBaselines()
	relative, _ := svc.GetScore(ctx, "sei", worse, domain.RPCTypeJSONRPC)
	assert.InDelta(t, 100+rc.Penalty(0.015)-rc.Penalty(0.013), relative, 0.01)
	top, _ := svc.GetScore(ctx, "sei", best, domain.RPCTypeJSONRPC)
	assert.Equal(t, 100.0, top, "the pool's best key pays nothing")

	svc.SetRelativeChronic(func(domain.ServiceID) bool { return false })
	svc.refreshBaselines()
	back, _ := svc.GetScore(ctx, "sei", worse, domain.RPCTypeJSONRPC)
	assert.InDelta(t, absolute, back, 0.01, "flag off: back to the absolute penalty")
}
