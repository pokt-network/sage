// Package healthcheck orchestrates periodic active health checks across all
// configured services, using QoS plugins to generate check payloads and
// interpret responses.
package healthcheck

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/observe"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
)

const (
	defaultInterval = 30 * time.Second
	defaultWorkers  = 4
)

// Executor runs periodic health checks across all configured services.
type Executor struct {
	protocol    protocol.Relayer
	endpoints   protocol.EndpointProvider
	sessions    protocol.SessionManager
	qosRegistry *qos.Registry
	repService  reputation.Service
	obsQueue    *observe.Queue
	logger      *slog.Logger

	interval time.Duration
	workers  int

	// configured holds health checks declared in YAML. They run in addition to
	// the plugin's own checks, never instead of them.
	//
	// Atomic because a config reload replaces the whole set from the reload's
	// own goroutine while a check cycle is reading it. A plain field write
	// there is a data race, and the swap is wholesale — the loop wants one
	// consistent set per cycle, not a half-updated map.
	configured atomic.Pointer[ConfiguredChecks]

	// dedupByBackendURL fires one relay per unique backend URL rather than one
	// per supplier, fanning the result to the other suppliers on that URL.
	// Atomic for the same reason as configured.
	dedupByBackendURL atomic.Bool
	// cycle counts completed health-check rounds. It rotates which supplier
	// represents each backend so no single registration carries every probe.
	cycle uint64

	// leader decides whether this replica probes at all; sink is where its
	// results go; source is where the others' results come from. All
	// optional: nil leader means always probe, nil sink means publish
	// nothing, nil source means apply only what this replica probed.
	leader   Leader
	sink     ProbeSink
	source   ProbeSource
	recorder ResultRecorder

	// warm tracks readiness: the pod can steer selection once it has applied
	// health-check results (leader probes or follower stream) for enough of
	// the configured services. Before that a fresh pod selects blind and
	// returns failures until it warms, so readiness must gate on this rather
	// than on a session existing. warmMu guards coveredServices; warm is the
	// latched result read on the hot readiness path without a lock.
	warmMu           sync.Mutex
	coveredServices  map[domain.ServiceID]struct{}
	warm             atomic.Bool
	warmThresholdSet bool
	warmThreshold    int

	// flags gates traffic-informed probing; skipper holds its per-cycle state.
	// Both nil unless SetTrafficSkip wired them, and a nil skipper means every
	// due check runs, which is the behaviour that predates this.
	flags   featureflag.FlagStore
	skipper *trafficSkipper

	// workersFn resolves the probe concurrency at read time, so a runtime
	// override takes effect on the next cycle. Nil means the value NewExecutor
	// was given. Read once per cycle, not per dispatch: a pool that changed
	// size mid-cycle would have two different semaphores in play for one pass.
	workersFn func() int

	// intervalFn resolves the probe cadence at read time so a runtime override
	// takes effect on the next cycle rather than the next restart. Nil means
	// the value NewExecutor was given, which is what tests and a gateway with
	// no tuning store use. See SetIntervalResolver.
	intervalFn IntervalResolver

	// probeTimeoutNanos bounds one health check, in nanoseconds. Atomic for
	// the same reason configured is: a reload writes it while a cycle reads it.
	// Zero means DefaultProbeTimeout.
	probeTimeoutNanos atomic.Int64

	// now is the clock the schedule reads; tests move it.
	now func() time.Time
	// lastRun is when each (service, backend, check) was last scheduled. Only
	// runOnce touches it, and runOnce is called from one goroutine, so no
	// lock. Rebuilt every cycle from the backends seen, so a backend that
	// leaves the session takes its entries with it.
	lastRun map[probeKey]time.Time

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// probeKey identifies one check against one backend of one service.
type probeKey struct {
	service domain.ServiceID
	backend string
	check   string
}

// MaxProbeWorkers is the ceiling on health-check concurrency, applied wherever
// the value comes from.
//
// It exists because there were briefly two ceilings. The tuning knob refused
// anything above 64 while the config path accepted any number at all, so the
// mainnet canary ran 500-wide probe bursts on 2026-09-03 through a build that
// would have rejected 65 from an operator's own hand. Declaring a value
// unreasonable on one path and waving it through on the other is worse than
// either choice alone.
//
// 512 rather than 64, because the canary answered the question the low ceiling
// was guessing at. Half an hour of 500-wide bursts moved nothing the wrong
// way: probe 502s fell from 0.70 to 0.58 per second, 408s fell, and
// per-supplier transport failures got FLATTER, not sharper. The likely reason
// is that a burst and a trickle cost a supplier the same concurrency-seconds
// — the same ~1,100 probes either way — but 500 workers hold a connection for
// a second while 4 hold one continuously, and connection limits care about the
// shape rather than the integral. That is one fleet at one traffic share and
// not a general law, which is why there is still a ceiling.
const MaxProbeWorkers = 512

// clampWorkers bounds a worker count from any source. Out-of-range is clamped
// and reported rather than refused: this was an unimplemented key until
// 2026-09-03, and turning it into one that stops the gateway would make the
// upgrade path punish an operator for a value that had been inert.
func clampWorkers(n int, logger *slog.Logger) int {
	if n <= MaxProbeWorkers {
		return n
	}
	if logger != nil {
		logger.Warn("active_health_checks.max_workers is above the ceiling and has been reduced to it",
			slog.Int("requested", n),
			slog.Int("using", MaxProbeWorkers),
		)
	}
	return MaxProbeWorkers
}

// NewExecutor constructs an Executor. Interval and worker count fall back to
// defaults (30s, 4) if zero, and a count above MaxProbeWorkers is clamped.
func NewExecutor(
	protocol protocol.Relayer,
	endpoints protocol.EndpointProvider,
	sessions protocol.SessionManager,
	qosReg *qos.Registry,
	repSvc reputation.Service,
	obsQueue *observe.Queue,
	interval time.Duration,
	workers int,
	logger *slog.Logger,
) *Executor {
	if interval <= 0 {
		interval = defaultInterval
	}
	if workers <= 0 {
		workers = defaultWorkers
	}
	workers = clampWorkers(workers, logger)
	e := &Executor{
		protocol:        protocol,
		endpoints:       endpoints,
		sessions:        sessions,
		qosRegistry:     qosReg,
		repService:      repSvc,
		obsQueue:        obsQueue,
		logger:          logger,
		interval:        interval,
		workers:         workers,
		now:             time.Now,
		lastRun:         make(map[probeKey]time.Time),
		coveredServices: make(map[domain.ServiceID]struct{}),
	}
	e.dedupByBackendURL.Store(true)
	return e
}

// tick is how often the loop wakes: the shortest cadence in play, so the
// fastest service is served on time while everything else waits for its own
// interval. Re-read every cycle because a reload can change the per-service
// intervals.
func (e *Executor) tick() time.Duration {
	tick := e.interval
	if e.intervalFn != nil {
		if s := e.intervalFn.Shortest(); s > 0 {
			tick = s
		}
	}
	if s := e.configured.Load().shortestInterval(); s > 0 && s < tick {
		tick = s
	}
	return tick
}

// serviceInterval is how often this service's checks come round: its runtime
// override, else its configured cadence, else the global one.
//
// The override wins over the configured per-service value on purpose. An
// operator reaching for the admin API is reacting to something in front of
// them, and a config file quietly outranking that would make the override look
// accepted and do nothing.
func (e *Executor) serviceInterval(serviceID domain.ServiceID, configured *ConfiguredChecks) time.Duration {
	if e.intervalFn != nil {
		if d := e.intervalFn.For(serviceID); d > 0 {
			return d
		}
	}
	if d := configured.IntervalFor(serviceID); d > 0 {
		return d
	}
	return e.interval
}

// due reports whether a probe should run this cycle and stamps it if so.
//
// The tolerance of half a tick is deliberate: the ticker fires at 30.000s
// while the previous run was stamped a few microseconds after the tick before
// it, so an exact comparison would find 29.9999s elapsed and slip the probe
// by a whole tick. Half a tick still rounds to the nearest tick.
func (e *Executor) due(key probeKey, interval, tick time.Duration, now time.Time, next map[probeKey]time.Time) bool {
	last, seen := e.lastRun[key]
	if seen && now.Sub(last) < interval-tick/2 {
		next[key] = last
		return false
	}
	next[key] = now
	return true
}

// SetBackendURLDedup turns per-backend deduplication on or off. On by default;
// see HealthCheckConfig.DisableBackendURLDedup for why.
func (e *Executor) SetBackendURLDedup(enabled bool) {
	e.dedupByBackendURL.Store(enabled)
}

// SetConfiguredChecks attaches operator-declared checks. Passing nil, or not
// calling this at all, leaves only the plugin's checks running.
//
// Safe to call while the executor is running: a config reload calls it from
// its own goroutine, and the swap takes effect on the next cycle.
func (e *Executor) SetConfiguredChecks(c *ConfiguredChecks) {
	e.configured.Store(c)
}

// SetLeader installs the election the probe loop consults. Wire time only.
func (e *Executor) SetLeader(l Leader) { e.leader = l }

// SetProbeSink installs where this replica's probe results are published.
func (e *Executor) SetProbeSink(s ProbeSink) { e.sink = s }

// SetProbeSource installs the feed of other replicas' probe results; Start
// runs it.
func (e *Executor) SetProbeSource(s ProbeSource) { e.source = s }

// IntervalResolver reports the probe cadence for a service, and the shortest
// cadence any service has been given.
//
// Two methods because the scheduler needs both and they are different
// questions. For is asked once per service per cycle and decides when that
// service's checks come round. Shortest decides how often the cycle itself
// runs, and has to account for a service whose cadence was set faster than the
// global one — including a service the resolver has never been asked about,
// which is why the scheduler cannot simply take the minimum of what it saw.
type IntervalResolver interface {
	For(serviceID domain.ServiceID) time.Duration
	Shortest() time.Duration
}

// SetIntervalResolver installs a live source for the probe cadence, so an
// operator can change it without a redeploy.
//
// The cadence is the one setting here whose cost is paid in relays rather than
// in latency — every probe is bought from the app's stake — and it was the one
// captured at wire time. A per-service `local[].check_interval` was already
// picked up by a config reload while the global interval was not, which is the
// kind of asymmetry nobody discovers until they need the other one.
func (e *Executor) SetIntervalResolver(r IntervalResolver) { e.intervalFn = r }

// SetWorkerResolver installs a live source for the probe concurrency.
//
// Separate from SetIntervalResolver because the two dials trade in opposite
// directions and an operator needs both: more workers shortens the cycle,
// while a longer interval cuts the number of probes outright. Only the second
// reduces what is spent — the first spreads the same probes over less time, at
// the cost of concurrency against suppliers that are also serving client
// relays.
func (e *Executor) SetWorkerResolver(fn func() int) { e.workersFn = fn }

// workerCount is the pool size for one cycle.
func (e *Executor) workerCount() int {
	if e.workersFn != nil {
		if n := e.workersFn(); n > 0 {
			return clampWorkers(n, nil)
		}
	}
	return e.workers
}

// SetTrafficSkip enables traffic-informed probing: a due check against a
// backend that client traffic has already graded this cycle is not sent.
//
// Both arguments are required for it to do anything. counter is where the
// traffic readings come from — a reputation.Service that does not implement
// reputation.TrafficCounter leaves this off — and flags is what gates it per
// service at runtime. Call at wire time.
func (e *Executor) SetTrafficSkip(counter reputation.TrafficCounter, flags featureflag.FlagStore, cfg TrafficSkipConfig) {
	if counter == nil || flags == nil {
		return
	}
	e.flags = flags
	e.skipper = newTrafficSkipper(counter, cfg)
}

// SetResultRecorder installs the metric hook for applied results.
func (e *Executor) SetResultRecorder(r ResultRecorder) { e.recorder = r }

// Start begins the background health check loop. It is safe to call Start
// only once; subsequent calls do nothing.
func (e *Executor) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		tick := e.tick()
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Per tick, not per loop: a recovery that wrapped the whole
				// loop would contain the panic and still leave the ticker
				// dead, which is a stopped health checker that logged once.
				start := e.now()
				safego.Run(e.logger, "healthcheck.cycle", func() { e.runOnce(ctx) })
				e.recordCycle(e.now().Sub(start), tick)
				e.logWarmProgress()
				if t := e.tick(); t != tick {
					tick = t
					ticker.Reset(tick)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// The feed of other replicas' probe results. Restarted after every
	// return with a short pause: a Redis blip must not leave a follower
	// blind until the process restarts.
	if e.source != nil {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			for ctx.Err() == nil {
				safego.Run(e.logger, "healthcheck.probe.source", func() {
					if err := e.source.Run(ctx, func(r ProbeResult) {
						r.Source = ResultSourceStream
						e.applyResult(ctx, r)
					}); err != nil && ctx.Err() == nil {
						e.logger.Warn("healthcheck: probe source stopped, restarting", "error", err)
					}
				})
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
		}()
	}
}

// Stop cancels the background loop and waits for all goroutines to exit.
func (e *Executor) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
}

// runOnce performs a single health check cycle across all configured services.
func (e *Executor) runOnce(ctx context.Context) {
	// Probing is the leader's job: a probe is a paid relay against the app's
	// stake, and N replicas probing buy N copies of one fact. Followers
	// learn the same facts from the leader's published results.
	if e.leader != nil && !e.leader.IsLeader() {
		return
	}
	services := e.sessions.ConfiguredServices()
	if len(services) == 0 {
		return
	}

	// Read the configured checks once per cycle rather than per service: a
	// reload landing mid-cycle should change the next round, not half of this
	// one.
	configured := e.configured.Load()
	now := e.now()
	tick := e.tick()
	// next replaces lastRun at the end of the cycle, holding only the probes
	// seen this cycle.
	next := make(map[probeKey]time.Time, len(e.lastRun))
	if e.skipper != nil {
		e.skipper.beginCycle()
	}
	// Probes issued this cycle, per service. Counted in the scheduling loop,
	// which is single-goroutine, so no lock: the workers it dispatches to run
	// concurrently but do not touch this.
	issued := make(map[domain.ServiceID]int, len(services))

	// Semaphore limits concurrent workers. Sized once per cycle: resolving it
	// per dispatch would put two differently-sized pools in play for one pass.
	sem := make(chan struct{}, e.workerCount())

	for serviceID := range services {
		serviceID := serviceID // capture for goroutine
		plugin := e.qosRegistry.Get(serviceID)
		if plugin == nil {
			continue
		}
		checker, ok := plugin.(qos.HealthChecker)
		if !ok {
			continue
		}

		serviceInterval := e.serviceInterval(serviceID, configured)

		eps, err := e.endpoints.AvailableEndpoints(ctx, serviceID, domain.RPCTypeJSONRPC)
		if err != nil {
			// Debug: the protocol reports the cause once when it changes; a
			// service with no suppliers would otherwise say so every cycle.
			e.logger.Debug("healthcheck: failed to get endpoints",
				"service_id", serviceID,
				"error", err,
			)
			continue
		}

		for _, group := range e.groupByBackend(eps) {
			group := group // capture for goroutine
			probe := group.probe(e.cycle)
			// Plugin checks first: they feed block height and chain ID
			// tracking, so they must run even when a config adds its own.
			//
			// slices.Concat, not append: the slice a plugin returns is the
			// plugin's, and appending to it writes into its backing array
			// whenever it has spare capacity — one service's configured checks
			// landing in the array a plugin hands to every service. Concat
			// always allocates.
			all := slices.Concat(checker.HealthChecks(probe), configured.For(serviceID))

			// Keep the checks that are due on this backend. A check's own
			// interval only ever slows it down: it runs at the longer of its
			// spacing and the service's cadence.
			checks := make([]qos.HealthCheck, 0, len(all))
			for _, check := range all {
				interval := max(check.Interval, serviceInterval)
				if !e.due(probeKey{serviceID, group.key, check.Name}, interval, tick, now, next) {
					continue
				}
				// After due, not before: the skip decision needs this cycle's
				// traffic reading recorded for every backend that was going to
				// be probed, and a check whose own interval has not elapsed
				// was not going to be probed either way. Stamping it due and
				// then skipping it is also what keeps the schedule honest — a
				// skipped check is covered, not overdue, so it must not come
				// back the moment traffic pauses.
				if e.skipCoveredByTraffic(ctx, serviceID, probe, group.endpoints, group.key, check, interval, now) {
					continue
				}
				checks = append(checks, check)
			}
			if len(checks) == 0 {
				continue
			}

			issued[serviceID] += len(checks)

			sem <- struct{}{}
			e.wg.Add(1)
			go func() {
				defer safego.Recover(e.logger, "healthcheck.endpoint")
				defer e.wg.Done()
				defer func() { <-sem }()
				e.checkEndpoint(ctx, serviceID, probe, group.endpoints, plugin, checks, configured)
			}()
		}
	}

	e.lastRun = next
	if e.recorder != nil {
		e.recorder.RecordHealthCheckCycleProbes(issued)
	}
	if e.skipper != nil {
		e.logSkipProgress()
		e.skipper.endCycle(now)
	}
	e.cycle++
}

// skipCoveredByTraffic reports whether client traffic has already bought what
// this check would learn.
//
// Three conditions, all of them load-bearing. The flag is off by default and
// resolves per service, so an operator can keep probing a chain whose block
// height matters most. The pod must be warm: readiness counts coverage from
// applied probe results, so a pod that skipped its way to fewer probes could
// hold itself out of rotation — and warm latches, so this costs one atomic
// read forever after. And the traffic must be recent and substantial enough to
// stand in for the probe's own observation (see DefaultMinTrafficSignals).
func (e *Executor) skipCoveredByTraffic(
	ctx context.Context,
	serviceID domain.ServiceID,
	ep domain.EndpointAddr,
	siblings domain.EndpointAddrList,
	backend string,
	check qos.HealthCheck,
	interval time.Duration,
	now time.Time,
) bool {
	if e.skipper == nil || !e.warm.Load() {
		return false
	}
	// An essential check is one whose ANSWER client traffic may not supply —
	// the plugin's block-height probe. The traffic threshold guarantees that
	// enough observations arrive, not that any of them contains a height: only
	// one method per chain yields one, and a client sends whatever it likes,
	// so a service carrying heavy eth_call traffic can clear the gate by
	// orders of magnitude and teach the block consensus nothing.
	//
	// That is an argument about uncertainty, so the answer is to remove the
	// uncertainty rather than to refuse outright. If a height for this backend
	// arrived within the probe's own interval — from anywhere, a client relay
	// as readily as a probe — then the probe is buying a second copy of a fact
	// the plugin already has, and the reason to keep it does not apply. The
	// canary showed sixteen busy services sitting at 2-5 seconds of chain-view
	// staleness against an 86-second cycle, which is client traffic supplying
	// heights continuously; refusing to skip there was protecting nothing.
	if check.Essential && !e.heightIsFresh(serviceID, siblings, interval, now) {
		return false
	}
	if !e.flags.IsEnabled(ctx, featureflag.FlagTrafficInformedProbing, serviceID) {
		return false
	}
	if !e.skipper.skip(serviceID, ep, backend, check.Payload.RPCType(), interval, now) {
		return false
	}
	if e.recorder != nil {
		e.recorder.RecordHealthCheckSkipped(serviceID)
	}
	return true
}

// logSkipProgress says why traffic-informed probing is skipping nothing.
//
// It is WARN and once per cycle, for the reason the warm-up log is: a feature
// that is switched on and does nothing is indistinguishable from a feature
// that is not wired, and on the mainnet canary on 2026-09-03 telling those two
// apart cost an experiment and a round trip. It goes silent the moment
// anything is skipped, so a working deployment never sees it.
//
// Nothing considered means the flag is off everywhere, which is the default
// and not worth a line.
func (e *Executor) logSkipProgress() {
	d := e.skipper.lastDecision
	if d.considered == 0 || d.skipped > 0 {
		return
	}
	e.logger.Warn("traffic-informed probing skipped nothing this cycle",
		slog.Int("considered", d.considered),
		slog.Int("awaiting_window", d.waiting),
		slog.Uint64("max_traffic_delta", d.maxDelta),
		slog.Uint64("min_traffic_signals", e.skipper.minSignals),
	)
}

// heightIsFresh reports whether this backend has supplied a block height
// within one interval, from any source.
//
// The sibling set, not the one endpoint the rotation picked: a height is a
// fact about the backend and not about the staked registration used to reach
// it, which is the same reason probe results already fan out to siblings.
// Client traffic reaches whichever registration selection chose, which is
// rarely the one this cycle's rotation would have probed.
//
// A plugin that cannot answer — one that tracks no height at all — reports
// nothing, and the caller then keeps probing. Unknown is not fresh.
func (e *Executor) heightIsFresh(
	serviceID domain.ServiceID,
	siblings domain.EndpointAddrList,
	interval time.Duration,
	now time.Time,
) bool {
	observer, ok := e.qosRegistry.Get(serviceID).(qos.HeightObserver)
	if !ok {
		return false
	}
	last, seen := observer.LastHeightObservation(siblings)
	if !seen {
		return false
	}
	return now.Sub(last) < interval
}

// backendGroup is the set of endpoints sharing one backend URL.
type backendGroup struct {
	// key names the backend for scheduling: its URL, or the endpoint address
	// when the group is a single unparseable or undeduplicated endpoint.
	key       string
	endpoints domain.EndpointAddrList
}

// probe picks which supplier carries this cycle's relay for the backend.
//
// Rotating rather than always using the first member matters for two reasons:
// a relay consumes the probing supplier's per-session allowance, and a supplier
// that is never probed is never observed to be individually broken (a bad
// registration in front of a healthy backend still fails to relay). Rotation
// spreads the cost and eventually exercises every registration.
func (g backendGroup) probe(cycle uint64) domain.EndpointAddr {
	return g.endpoints[cycle%uint64(len(g.endpoints))]
}

// groupByBackend collapses endpoints sharing a backend URL into one group.
// With deduplication off, every endpoint becomes its own group and the caller's
// behavior is unchanged.
func (e *Executor) groupByBackend(eps domain.EndpointAddrList) []backendGroup {
	if !e.dedupByBackendURL.Load() {
		groups := make([]backendGroup, 0, len(eps))
		for _, ep := range eps {
			groups = append(groups, backendGroup{key: string(ep), endpoints: domain.EndpointAddrList{ep}})
		}
		return groups
	}

	byURL := make(map[string]int, len(eps))
	groups := make([]backendGroup, 0, len(eps))
	for _, ep := range eps {
		url, err := ep.URL()
		if err != nil || url == "" {
			// An address we cannot parse gets its own group rather than being
			// lumped in with every other unparseable one.
			groups = append(groups, backendGroup{key: string(ep), endpoints: domain.EndpointAddrList{ep}})
			continue
		}
		if idx, ok := byURL[url]; ok {
			groups[idx].endpoints = append(groups[idx].endpoints, ep)
			continue
		}
		byURL[url] = len(groups)
		groups = append(groups, backendGroup{key: url, endpoints: domain.EndpointAddrList{ep}})
	}
	return groups
}

// checkEndpoint runs all health checks against probe and applies the results to
// every endpoint sharing its backend (probe included).
//
// configured is the cycle's own snapshot of the operator-declared rules,
// threaded down rather than re-read: the checks being run came from it, so
// grading their failures against a set that has since been swapped would
// penalise an endpoint by a rule that is no longer in the file.
func (e *Executor) checkEndpoint(
	ctx context.Context,
	serviceID domain.ServiceID,
	probe domain.EndpointAddr,
	siblings domain.EndpointAddrList,
	plugin qos.Plugin,
	checks []qos.HealthCheck,
	configured *ConfiguredChecks,
) {
	for _, check := range checks {
		e.sendCheck(ctx, serviceID, probe, siblings, plugin, check, configured)
	}
}

// checkSignal grades a health check that came back with an HTTP response.
//
// HTTP status alone is not enough to say a check passed. An endpoint can answer
// 200 with a body we cannot parse, or — worse — a well-formed body reporting a
// chain it was never asked about. Grading on status only meant both scored a
// full success and stayed in rotation.
func checkSignal(checkName string, statusCode int, extractErr error, latency time.Duration) reputation.Signal {
	reason := "health_check: " + checkName

	switch {
	case statusCode < 200 || statusCode >= 300:
		return reputation.NewMinorErrorSignal(reason, latency)

	// A wrong chain is not a bad moment: the endpoint is healthy and confidently
	// serving someone else's chain under this service's name. Nothing it reports
	// is usable, and its block heights would otherwise sail through the height
	// checks — so eject rather than penalize.
	case errors.Is(extractErr, qos.ErrWrongChain):
		return reputation.NewCriticalErrorSignal(reason+": wrong chain", latency)

	// A 200 we cannot parse is a real failure but a milder one — most causes are
	// transient or an endpoint quirk rather than an imposter.
	case extractErr != nil:
		return reputation.NewMinorErrorSignal(reason+": unparseable response", latency)

	default:
		return reputation.NewSuccessSignal(reason, latency)
	}
}

// sendCheck sends a single health check and applies its result here, then
// publishes the result so every other replica can apply it too.
// siblings are every endpoint on the probed backend, including ep. Results the
// backend produced — its block height, whether it answered correctly — apply to
// all of them: they are the same machine reached through different
// registrations. A transport failure is the exception, since that can be the
// probing registration rather than the backend behind it.
func (e *Executor) sendCheck(
	ctx context.Context,
	serviceID domain.ServiceID,
	ep domain.EndpointAddr,
	siblings domain.EndpointAddrList,
	plugin qos.Plugin,
	check qos.HealthCheck,
	configured *ConfiguredChecks,
) {
	_ = plugin
	_ = configured

	// Bound the probe on its own, not on the relay timeout it would otherwise
	// inherit. See probeTimeout: a hung backend costs one worker for the whole
	// relay timeout, and with a small pool that is a large fraction of the
	// fleet's probe capacity spent learning something a few seconds would have
	// told us.
	probeCtx, cancel := context.WithTimeout(ctx, e.probeTimeout())
	defer cancel()

	result := e.probe(probeCtx, serviceID, ep, siblings, check)
	e.applyResult(ctx, result)
	if e.sink != nil {
		if err := e.sink.Publish(ctx, result); err != nil {
			e.logger.Warn("healthcheck: publish probe result", "service_id", serviceID, "check", check.Name, "error", err)
		}
	}
}

// DefaultProbeTimeout bounds one health check.
//
// A probe inherits nothing useful from the relay path: without this it runs
// under the client relay timeout (defaults.timeout.relay_timeout, 30s
// unconfigured), because the executor passes its own long-lived context
// straight down. One hung backend then holds a worker for thirty seconds, and
// with the default pool of four that is a quarter of the fleet's entire probe
// capacity spent waiting for an answer whose content no longer matters — a
// backend that has not replied in five seconds is unhealthy either way, and
// the check has already learned that.
//
// Five seconds is chosen against the two failure directions, not as a round
// number. Too long is the state SAGE was in: on the mainnet canary on
// 2026-09-03 the fleet averaged 1.39s per probe at four-way concurrency
// (2.87 probes/s), which is almost entirely tail — a healthy eth_blockNumber
// answers in tens of milliseconds — and the resulting sweep took five to
// nineteen minutes against a sixty-second configured interval. Too short is
// worse than slow: a probe cut off early is graded a minor error against a
// supplier that was merely loaded, so the timeout manufactures the failure it
// reports. Five seconds is an order of magnitude above a healthy response and
// six times below the relay timeout, which leaves the grading honest.
//
// This lowers probe LOAD rather than raising it, which is the reason to prefer
// it to a bigger worker pool: the same suppliers serve client traffic, and
// more concurrency there competes with relays.
const DefaultProbeTimeout = 5 * time.Second

// SetProbeTimeout overrides how long one health check may take. Wire time,
// from active_health_checks.probe_timeout. A non-positive value is stored as
// given and read back as the default; see probeTimeout, which is the single
// place that decision is made.
func (e *Executor) SetProbeTimeout(d time.Duration) {
	e.probeTimeoutNanos.Store(int64(d))
}

// probeTimeout is the live value, read once per probe. Atomic because a config
// reload writes it from its own goroutine while a cycle is reading it, and the
// one place a non-positive setting becomes the default — normalising on the
// way in as well would be a second copy of the rule that could disagree.
func (e *Executor) probeTimeout() time.Duration {
	if d := e.probeTimeoutNanos.Load(); d > 0 {
		return time.Duration(d)
	}
	return DefaultProbeTimeout
}

// recordProbeRelay reports one probe send to the relay-attempt metrics, so a
// health check shows up in sage_relay_total and sage_relay_latency_seconds
// under request_type="probe" the way a client attempt shows up under
// "client". Probes bypass the middleware chain, so this is the only place
// they can be counted.
//
// The status mirrors the metrics middleware: the response's status when there
// is one, 502 as the sentinel for a relay that failed before producing a
// response, and 0 for neither.
func (e *Executor) recordProbeRelay(
	serviceID domain.ServiceID,
	ep domain.EndpointAddr,
	resp *domain.Response,
	latency time.Duration,
	err error,
) {
	if e.recorder == nil {
		return
	}
	statusCode := 0
	switch {
	case resp != nil:
		statusCode = resp.HTTPStatusCode
	case err != nil:
		statusCode = http.StatusBadGateway
	}
	e.recorder.RecordProbeRelay(serviceID, ep, statusCode, latency, err)
}

// probe sends one health check and packages what came back as a
// ProbeResult. A transport failure is graded here, on the replica that saw
// the error, because the verdict travels and the error does not.
func (e *Executor) probe(ctx context.Context, serviceID domain.ServiceID, ep domain.EndpointAddr, siblings domain.EndpointAddrList, check qos.HealthCheck) ProbeResult {
	start := time.Now()
	resp, err := e.protocol.SendRelay(ctx, serviceID, ep, check.Payload)
	latency := time.Since(start)
	e.recordProbeRelay(serviceID, ep, resp, latency, err)
	result := ProbeResult{
		ServiceID: serviceID,
		Endpoint:  ep,
		Siblings:  siblings,
		Check:     check.Name,
		RPCType:   check.Payload.RPCType(),
		Request:   check.Payload.Bytes(),
		LatencyMS: latency.Milliseconds(),
		ProbedAt:  start,
		Source:    ResultSourceProbe,
	}
	if err != nil {
		e.logger.Debug("healthcheck: relay error",
			"service_id", serviceID, "endpoint", ep, "check", check.Name, "error", err)
		result.TransportError = err.Error()
		verdict := heuristic.AnalyzeTransportError(err, ctx.Err())
		result.TransportReason = verdict.Reason
		if verdict.ShouldPenalize {
			result.TransportSeverity = verdict.PenaltySeverity
		}
		return result
	}
	result.StatusCode = resp.HTTPStatusCode
	result.Body = resp.Body
	if len(result.Body) > maxProbeBodyBytes {
		result.Body = result.Body[:maxProbeBodyBytes]
	}
	return result
}

// Warm reports whether the executor has applied results for enough of the
// configured services that reputation can steer endpoint selection. It is the
// readiness signal: a pod is not put into rotation until it is warm, so a
// fresh or rolled pod does not take traffic while it would still select blind.
//
// The threshold is 75% of the configured services, so the handful with no
// suppliers on the network (which never produce a result) cannot hold
// readiness down forever. With no configured services there is nothing to wait
// for and it reads warm immediately.
func (e *Executor) Warm() bool {
	if e.warm.Load() {
		return true
	}
	e.warmMu.Lock()
	defer e.warmMu.Unlock()
	e.ensureWarmThresholdLocked()
	if e.warmThreshold == 0 || len(e.coveredServices) >= e.warmThreshold {
		e.warm.Store(true)
		return true
	}
	return false
}

// ensureWarmThresholdLocked computes the warm threshold once, from the
// configured service count. Called under warmMu.
func (e *Executor) ensureWarmThresholdLocked() {
	if e.warmThresholdSet {
		return
	}
	n := len(e.sessions.ConfiguredServices())
	// ceil(0.75 * n); 0 stays 0 (warm immediately).
	e.warmThreshold = (n*3 + 3) / 4
	e.warmThresholdSet = true
}

// SeedCoverage credits services whose reputation was loaded from shared
// storage at startup, so readiness does not wait for this pod to re-probe what
// it already knows.
//
// The warm gate exists to keep a pod that would select blind out of rotation.
// A service whose scores were just hydrated is not blind — the pod holds real
// state for it, at most one idle TTL old — so it counts exactly as a probe
// result does. Without this a pod can load every score it needs and still sit
// 503 for minutes waiting to observe them again.
//
// Only configured services count. Storage is shared across a fleet and outlives
// any one config, so a service this pod does not serve must not inflate the
// coverage its readiness is measured against.
func (e *Executor) SeedCoverage(services []domain.ServiceID) {
	configured := e.sessions.ConfiguredServices()
	for _, svc := range services {
		if _, ok := configured[svc]; !ok {
			continue
		}
		e.markCovered(svc)
	}
}

// maxUnwarmedServicesLogged bounds the service list in the warm-up log. A
// fleet runs dozens of services and the point of the line is which ones are
// missing, not all of them; a handful names the pattern without turning one
// log line into a page.
const maxUnwarmedServicesLogged = 10

// recordCycle reports how long a cycle took and says so when it overran the
// tick it was scheduled on.
//
// The cycle runs on the ticker's own goroutine and dispatch blocks on a fixed
// worker pool, so a cycle that outlasts its tick does not overlap the next one
// — it delays it, and time.Ticker drops the tick it missed. The consequence is
// that active_health_checks.interval is a FLOOR, not the cadence: with enough
// backends for the worker pool, the real cadence is the cycle duration, every
// service is probed in one burst as the loop reaches it, and per-service probe
// rates measured over anything shorter than a cycle are sampling artifacts.
//
// None of that was observable, which is how it went unexplained until the
// mainnet canary on 2026-09-03 showed a service flat for fourteen minutes on a
// sixty-second interval and then jumping thirty-four probes at once.
func (e *Executor) recordCycle(elapsed, tick time.Duration) {
	if e.recorder != nil {
		e.recorder.RecordHealthCheckCycle(elapsed, tick)
	}
	if tick <= 0 || elapsed <= tick {
		return
	}
	e.logger.Warn("health check cycle overran its interval; the configured interval is not the cadence being achieved",
		slog.Duration("elapsed", elapsed),
		slog.Duration("interval", tick),
		slog.Int("workers", e.workerCount()),
	)
}

// logWarmProgress says why readiness is still 503, once per cycle until it is
// not.
//
// It is WARN rather than INFO deliberately. A pod that is not warm is a pod
// not serving, and this line is the only explanation of that anywhere: /ready
// returns a bare 503, and the "no session yet, skipping service" path that
// usually causes it logs at DEBUG, which production log levels suppress. The
// mainnet canary spent a rollout on 2026-09-02 being restarted by its startup
// probe with no logged reason at all, diagnosed only from a goroutine dump and
// a metrics scrape.
//
// It stops as soon as the pod is warm, and warm is latched, so a healthy pod
// logs this a bounded number of times at startup and never again.
func (e *Executor) logWarmProgress() {
	if e.warm.Load() {
		return
	}

	e.warmMu.Lock()
	e.ensureWarmThresholdLocked()
	covered, threshold := len(e.coveredServices), e.warmThreshold
	missing := make([]string, 0, maxUnwarmedServicesLogged)
	truncated := 0
	for svc := range e.sessions.ConfiguredServices() {
		if _, ok := e.coveredServices[svc]; ok {
			continue
		}
		if len(missing) < maxUnwarmedServicesLogged {
			missing = append(missing, string(svc))
			continue
		}
		truncated++
	}
	e.warmMu.Unlock()

	// Sorted so consecutive lines are comparable; map order would make every
	// line look different while nothing changed.
	slices.Sort(missing)

	e.logger.Warn("health checks: not warm, readiness is 503",
		"covered", covered,
		"needed", threshold,
		"awaiting", missing,
		"awaiting_not_listed", truncated,
	)
}

// markCovered records that a result has been applied for a service and latches
// warm once the threshold is met.
func (e *Executor) markCovered(svc domain.ServiceID) {
	if e.warm.Load() {
		return
	}
	e.warmMu.Lock()
	e.coveredServices[svc] = struct{}{}
	e.ensureWarmThresholdLocked()
	warm := e.warmThreshold == 0 || len(e.coveredServices) >= e.warmThreshold
	e.warmMu.Unlock()
	if warm {
		e.warm.Store(true)
	}
}

// applyResult lands one probe's knowledge on this replica: the reputation
// signal, the block height on every sibling, the observation. Identical for
// a result this replica probed and one it received, which is the point.
func (e *Executor) applyResult(ctx context.Context, r ProbeResult) {
	e.markCovered(r.ServiceID)
	if e.recorder != nil {
		e.recorder.RecordHealthCheckResult(r.ServiceID, string(r.Source))
	}
	latency := time.Duration(r.LatencyMS) * time.Millisecond
	rpcType := r.RPCType
	if rpcType == "" {
		rpcType = domain.RPCTypeJSONRPC
	}

	if r.TransportError != "" {
		// Penalize only the endpoint that actually failed. A relay error can be
		// the supplier's session or signing rather than the backend, and there
		// is no response to tell the two apart — blaming the backend's other
		// registrations for it would eject healthy ones.
		if e.repService != nil && r.TransportSeverity != "" {
			signal := severitySignal(r.TransportSeverity, "health_check: "+r.Check+": "+r.TransportReason, latency)
			signal.Probe = true
			_ = e.repService.RecordSignal(ctx, r.ServiceID, r.Endpoint, rpcType, signal)
		}
		e.submitObservation(r.ServiceID, r.Endpoint, r.Request, nil, 0, latency, nil)
		return
	}

	plugin := e.qosRegistry.Get(r.ServiceID)

	// Extract structured data if the plugin supports it.
	//
	// extractErr is carried down to the reputation signal below: what the body
	// says is part of whether the check passed, not merely a parsing detail.
	var extracted *observe.ExtractedData
	var extractErr error
	if extractor, ok := plugin.(qos.DataExtractor); ok {
		var data *qos.ExtractedData
		data, extractErr = extractor.ExtractData(r.Endpoint, r.Request, r.Body)
		switch {
		case extractErr != nil:
			e.logger.Debug("healthcheck: extract error",
				"service_id", r.ServiceID, "endpoint", r.Endpoint, "check", r.Check, "error", extractErr)
		case data != nil:
			extracted = &observe.ExtractedData{
				BlockHeight: data.BlockHeight,
				ChainID:     data.ChainID,
				IsSyncing:   data.IsSyncing,
				IsArchival:  data.IsArchival,
			}
			// Update block height tracker if available. The height belongs to
			// the backend, so every registration in front of it reports it —
			// otherwise the un-probed siblings would look permanently
			// height-less and be filtered out of selection.
			if tracker, ok := plugin.(qos.BlockHeightTracker); ok && data.BlockHeight != nil {
				for _, sibling := range r.Siblings {
					tracker.UpdateBlockHeight(sibling, *data.BlockHeight)
				}
			}
		}
	}

	// Record reputation signal. A configured check may name the penalty its
	// failure carries; the default grading applies to everything else.
	//
	// The backend answered, so what it said grades every registration in front
	// of it — but it is still ONE probe, and the service records it once per
	// distinct reputation key. At the default per-URL granularity the siblings
	// are one key and the probe is one attempt against it; at per-endpoint each
	// registration is its own key and each gets the attempt. Looping here
	// instead would charge a backend its stake count in attempts, which is a
	// property of the stake table and not of the machine (ruling F1,
	// docs/scoring.md §3 principle 4 and §7.4).
	if e.repService != nil {
		signal := checkSignal(r.Check, r.StatusCode, extractErr, latency)
		if failed := r.StatusCode < 200 || r.StatusCode >= 300 || extractErr != nil; failed {
			if declared, ok := e.configured.Load().SignalFor(r.Check, "health_check: "+r.Check, latency); ok {
				signal = declared
			}
		}
		signal.Probe = true
		siblings := r.Siblings
		if len(siblings) == 0 {
			siblings = domain.EndpointAddrList{r.Endpoint}
		}
		if once, ok := e.repService.(reputation.OnceRecorder); ok {
			_ = once.RecordSignalOnce(ctx, r.ServiceID, siblings, rpcType, signal)
		} else {
			// A Service that cannot dedupe: the pre-F1 fan-out is still better
			// than dropping the probe.
			for _, sibling := range siblings {
				_ = e.repService.RecordSignal(ctx, r.ServiceID, sibling, rpcType, signal)
			}
		}
	}

	e.submitObservation(r.ServiceID, r.Endpoint, r.Request, r.Body, r.StatusCode, latency, extracted)
}

// severitySignal builds the signal a transport verdict carries.
func severitySignal(severity, reason string, latency time.Duration) reputation.Signal {
	switch severity {
	case heuristic.SeverityCritical:
		return reputation.NewCriticalErrorSignal(reason, latency)
	case heuristic.SeverityMajor:
		return reputation.NewMajorErrorSignal(reason, latency)
	default:
		return reputation.NewMinorErrorSignal(reason, latency)
	}
}

// submitObservation enqueues an observation for async processing.
func (e *Executor) submitObservation(
	serviceID domain.ServiceID,
	ep domain.EndpointAddr,
	reqBody, respBody []byte,
	statusCode int,
	latency time.Duration,
	extracted *observe.ExtractedData,
) {
	if e.obsQueue == nil {
		return
	}
	e.obsQueue.Submit(observe.Observation{
		ServiceID:    serviceID,
		EndpointAddr: ep,
		Timestamp:    time.Now(),
		Source:       observe.SourceHealthCheck,
		Latency:      latency,
		HTTPStatus:   statusCode,
		RequestBody:  reqBody,
		ResponseBody: respBody,
		Extracted:    extracted,
	})
}
