package healthcheck

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// fakeResolver is a live cadence source the test can move.
type fakeResolver struct {
	global   time.Duration
	perSvc   map[domain.ServiceID]time.Duration
	shortest time.Duration
}

func (f fakeResolver) For(serviceID domain.ServiceID) time.Duration {
	if d, ok := f.perSvc[serviceID]; ok {
		return d
	}
	return f.global
}

func (f fakeResolver) Shortest() time.Duration {
	if f.shortest > 0 {
		return f.shortest
	}
	return f.global
}

// The point of the whole change: the cadence is resolved per cycle, so an
// operator changing it does not need a redeploy.
func TestServiceInterval_ResolvedLive(t *testing.T) {
	e := &Executor{interval: time.Minute}
	empty := &ConfiguredChecks{}

	if got := e.serviceInterval("eth", empty); got != time.Minute {
		t.Fatalf("with no resolver: %v, want the constructed interval", got)
	}

	r := fakeResolver{global: 2 * time.Minute}
	e.SetIntervalResolver(r)
	if got := e.serviceInterval("eth", empty); got != 2*time.Minute {
		t.Errorf("global override = %v, want 2m", got)
	}

	// Moving it again takes effect immediately, without rebuilding anything.
	e.SetIntervalResolver(fakeResolver{global: 30 * time.Second})
	if got := e.serviceInterval("eth", empty); got != 30*time.Second {
		t.Errorf("after a second change: %v, want 30s", got)
	}
}

// A per-service override beats a global one, and beats the config file.
func TestServiceInterval_PerServiceWins(t *testing.T) {
	e := &Executor{interval: time.Minute}
	e.SetIntervalResolver(fakeResolver{
		global: 2 * time.Minute,
		perSvc: map[domain.ServiceID]time.Duration{"eth": 15 * time.Second},
	})

	if got := e.serviceInterval("eth", &ConfiguredChecks{}); got != 15*time.Second {
		t.Errorf("eth = %v, want its own 15s override", got)
	}
	if got := e.serviceInterval("poly", &ConfiguredChecks{}); got != 2*time.Minute {
		t.Errorf("poly = %v, want the global 2m", got)
	}
}

// The tick has to be at least as fast as the fastest cadence anyone asked for,
// including a service the scheduler has not reached this cycle. Without
// Shortest, a per-service override faster than the tick would simply not be
// honoured — the knob would look accepted and do nothing.
func TestTick_FollowsTheFastestOverride(t *testing.T) {
	e := &Executor{interval: time.Minute}
	e.configured.Store(&ConfiguredChecks{})

	e.SetIntervalResolver(fakeResolver{global: 5 * time.Minute, shortest: 5 * time.Minute})
	if got := e.tick(); got != 5*time.Minute {
		t.Errorf("tick = %v, want the 5m override rather than the constructed 1m", got)
	}

	// One service set faster: the whole cycle has to keep up with it.
	e.SetIntervalResolver(fakeResolver{
		global:   5 * time.Minute,
		perSvc:   map[domain.ServiceID]time.Duration{"eth": 10 * time.Second},
		shortest: 10 * time.Second,
	})
	if got := e.tick(); got != 10*time.Second {
		t.Errorf("tick = %v, want 10s: a service asked for that cadence", got)
	}
}

// A configured local check_interval faster than the override still shortens
// the tick — the config is not overridden into being ignored, only outranked
// for the cadence of the service it names.
func TestTick_StillHonoursConfiguredChecks(t *testing.T) {
	e := &Executor{interval: time.Minute}
	checks, _ := BuildConfiguredChecks(config.HealthCheckConfig{
		Enabled: true,
		Local: []config.ServiceHealthChecks{
			{ServiceID: "poly", Enabled: true, CheckInterval: 5 * time.Second},
		},
	})
	e.configured.Store(checks)
	e.SetIntervalResolver(fakeResolver{global: 5 * time.Minute, shortest: 5 * time.Minute})

	if got := e.tick(); got != 5*time.Second {
		t.Errorf("tick = %v, want 5s from the configured check", got)
	}
}

// The other half of the cadence trade. max_workers was a config key with no Go
// field until 2026-09-03: an operator set 500 on the mainnet canary and got 4,
// while four-way concurrency was the thing making a cycle take 74s.
func TestWorkerCount_ResolvedLive(t *testing.T) {
	e := &Executor{workers: 4}

	if got := e.workerCount(); got != 4 {
		t.Fatalf("with no resolver: %d, want the constructed 4", got)
	}

	e.SetWorkerResolver(func() int { return 16 })
	if got := e.workerCount(); got != 16 {
		t.Errorf("override = %d, want 16", got)
	}

	// A non-positive override is not a pool of zero, which would dispatch
	// nothing at all.
	for _, n := range []int{0, -1} {
		e.SetWorkerResolver(func() int { return n })
		if got := e.workerCount(); got != 4 {
			t.Errorf("override %d gave %d, want the fallback 4", n, got)
		}
	}
}

// The pool is sized once per cycle. Resolving it per dispatch would put two
// differently-sized semaphores in play for one pass, so the cycle would not
// have a concurrency limit — it would have two.
func TestRunOnce_SizesThePoolOncePerCycle(t *testing.T) {
	var calls atomic.Int32
	e := &Executor{
		workers:         4,
		logger:          slog.New(slog.DiscardHandler),
		qosRegistry:     qos.NewRegistry(),
		sessions:        &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}, "poly": {}, "kava": {}}},
		now:             time.Now,
		lastRun:         map[probeKey]time.Time{},
		coveredServices: map[domain.ServiceID]struct{}{},
	}
	e.configured.Store(&ConfiguredChecks{})
	e.SetWorkerResolver(func() int { calls.Add(1); return 8 })

	e.runOnce(context.Background())

	if got := calls.Load(); got != 1 {
		t.Errorf("resolver called %d times in one cycle, want 1: the pool must be sized once", got)
	}
}
