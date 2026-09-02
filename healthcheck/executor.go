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

// NewExecutor constructs an Executor. Interval and worker count fall back to
// defaults (30s, 4) if zero.
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
	if s := e.configured.Load().shortestInterval(); s > 0 && s < tick {
		tick = s
	}
	return tick
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
				safego.Run(e.logger, "healthcheck.cycle", func() { e.runOnce(ctx) })
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

	// Semaphore limits concurrent workers.
	sem := make(chan struct{}, e.workers)

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

		// The service's cadence: its own check_interval, else the global one.
		serviceInterval := configured.IntervalFor(serviceID)
		if serviceInterval <= 0 {
			serviceInterval = e.interval
		}

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
				if e.due(probeKey{serviceID, group.key, check.Name}, interval, tick, now, next) {
					checks = append(checks, check)
				}
			}
			if len(checks) == 0 {
				continue
			}

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
	e.cycle++
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
	result := e.probe(ctx, serviceID, ep, siblings, check)
	e.applyResult(ctx, result)
	if e.sink != nil {
		if err := e.sink.Publish(ctx, result); err != nil {
			e.logger.Warn("healthcheck: publish probe result", "service_id", serviceID, "check", check.Name, "error", err)
		}
	}
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
