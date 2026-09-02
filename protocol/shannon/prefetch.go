package shannon

import (
	"context"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
)

// PrefetchConfig paces the startup session prefetch against the full node.
//
// The full node is a shared, rate-limited dependency: one gateway asking for
// every service's session at once, across a rolling fleet, is a burst it has
// no reason to absorb. Both knobs are about being a good citizen, not about
// speed — the prefetch has a whole readiness window to finish in.
type PrefetchConfig struct {
	// Concurrency is how many session fetches may be in flight at once. Zero
	// means DefaultPrefetchConcurrency.
	Concurrency int
	// MinInterval is the minimum spacing between two fetches leaving this
	// process, applied across all workers. Zero means
	// DefaultPrefetchMinInterval; negative disables pacing.
	MinInterval time.Duration
}

const (
	// DefaultPrefetchConcurrency matches the health-check executor's worker
	// count, which has been the fleet's steady-state concurrency against the
	// same full node for months. Prefetch is a startup burst, so it has less
	// licence than that, not more.
	DefaultPrefetchConcurrency = 4
	// DefaultPrefetchMinInterval spaces fetches at 20 per second. Seventy-odd
	// services then take under four seconds — well inside a readiness window,
	// and a rate no full node notices.
	DefaultPrefetchMinInterval = 50 * time.Millisecond
)

// PrefetchResult reports what the prefetch achieved.
type PrefetchResult struct {
	// Ready are the services that now hold a cached session, so the first
	// relay for them does not pay a synchronous fetch.
	Ready []domain.ServiceID
	// Failed counts services whose session could not be fetched — no owned
	// app, no suppliers staked, or the full node said no.
	Failed int
	// Elapsed is how long the whole prefetch took.
	Elapsed time.Duration
}

// PrefetchSessions warms the session cache for every configured service.
//
// Without it, a pod that has just gone ready holds no sessions at all, and the
// first relay for each service pays a synchronous full-node fetch on the
// request path (getSession falls through to refreshSession). Under a rolling
// deploy that lands as one timed-out request per service — the mainnet canary
// on 2026-09-02, where a freshly ready pod answered bsc, fuse, sei, linea,
// opbnb and zksync-era with "relay timeout exceeded" while it fetched what it
// could have fetched before taking traffic.
//
// This became load-bearing when reputation hydration decoupled readiness from
// probing. Readiness used to imply sessions existed, because the warm gate
// waited on health-check results and a probe cannot run without a session. A
// hydrated pod is ready in seconds, before the first probe cycle has run, so
// the sessions have to be fetched deliberately or not at all.
//
// Failures are counted, not returned: a service with no staked suppliers has
// no session to fetch and must not hold up a pod that can serve the other
// seventy. The caller decides what to do with a short Ready list.
func (p *Protocol) PrefetchSessions(ctx context.Context, cfg PrefetchConfig) PrefetchResult {
	start := time.Now()
	services := p.sessions.ConfiguredServices()

	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultPrefetchConcurrency
	}
	if concurrency > len(services) {
		concurrency = len(services)
	}
	if concurrency == 0 {
		return PrefetchResult{Elapsed: time.Since(start)}
	}

	pace := newPacer(cfg.MinInterval)
	defer pace.stop()

	var (
		mu     sync.Mutex
		result PrefetchResult
		wg     sync.WaitGroup
	)

	work := make(chan domain.ServiceID)
	for range concurrency {
		wg.Add(1)
		safego.Go(p.logger, "shannon.prefetch", func() {
			defer wg.Done()
			for serviceID := range work {
				if !pace.wait(ctx) {
					return
				}
				err := p.prefetchOne(ctx, serviceID)
				mu.Lock()
				if err != nil {
					result.Failed++
				} else {
					result.Ready = append(result.Ready, serviceID)
				}
				mu.Unlock()
			}
		})
	}

	for serviceID := range services {
		select {
		case work <- serviceID:
		case <-ctx.Done():
			// Out of time: close and let the workers drain. What was fetched
			// stays fetched; the rest is paid on the request path as before.
			close(work)
			wg.Wait()
			result.Elapsed = time.Since(start)
			return result
		}
	}
	close(work)
	wg.Wait()

	result.Elapsed = time.Since(start)
	return result
}

// prefetchOne populates the session and endpoint caches for one service. It
// deliberately goes through the same path a relay takes, so what it warms is
// exactly what a relay would otherwise have had to fetch.
func (p *Protocol) prefetchOne(ctx context.Context, serviceID domain.ServiceID) error {
	appAddr, err := p.pickApp(serviceID)
	if err != nil {
		return err
	}
	_, err = p.sessions.getEndpoints(ctx, string(serviceID), appAddr)
	return err
}

// pacer spaces outbound fetches so a startup burst does not arrive at the full
// node as a burst. A ticker rather than a token bucket: there is no credit to
// accumulate here — the point is a floor on the gap between requests, not an
// average.
type pacer struct {
	ticker *time.Ticker
}

func newPacer(interval time.Duration) *pacer {
	if interval == 0 {
		interval = DefaultPrefetchMinInterval
	}
	if interval < 0 {
		return &pacer{}
	}
	return &pacer{ticker: time.NewTicker(interval)}
}

// wait blocks until the next fetch may leave, reporting false if the context
// ended first.
func (p *pacer) wait(ctx context.Context) bool {
	if p.ticker == nil {
		return ctx.Err() == nil
	}
	select {
	case <-p.ticker.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *pacer) stop() {
	if p.ticker != nil {
		p.ticker.Stop()
	}
}
