// Package healthcheck orchestrates periodic active health checks across all
// configured services, using QoS plugins to generate check payloads and
// interpret responses.
package healthcheck

import (
	"context"
	"errors"
	"log/slog"
	"sync"
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
	configured *ConfiguredChecks

	// dedupByBackendURL fires one relay per unique backend URL rather than one
	// per supplier, fanning the result to the other suppliers on that URL.
	dedupByBackendURL bool
	// cycle counts completed health-check rounds. It rotates which supplier
	// represents each backend so no single registration carries every probe.
	cycle uint64

	cancel context.CancelFunc
	wg     sync.WaitGroup
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
	return &Executor{
		protocol:          protocol,
		endpoints:         endpoints,
		sessions:          sessions,
		qosRegistry:       qosReg,
		repService:        repSvc,
		obsQueue:          obsQueue,
		logger:            logger,
		interval:          interval,
		workers:           workers,
		dedupByBackendURL: true,
	}
}

// SetBackendURLDedup turns per-backend deduplication on or off. On by default;
// see HealthCheckConfig.DisableBackendURLDedup for why.
func (e *Executor) SetBackendURLDedup(enabled bool) {
	e.dedupByBackendURL = enabled
}

// SetConfiguredChecks attaches operator-declared checks. Passing nil, or not
// calling this at all, leaves only the plugin's checks running.
func (e *Executor) SetConfiguredChecks(c *ConfiguredChecks) {
	e.configured = c
}

// Start begins the background health check loop. It is safe to call Start
// only once; subsequent calls do nothing.
func (e *Executor) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(e.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Per tick, not per loop: a recovery that wrapped the whole
				// loop would contain the panic and still leave the ticker
				// dead, which is a stopped health checker that logged once.
				safego.Run(e.logger, "healthcheck.cycle", func() { e.runOnce(ctx) })
			case <-ctx.Done():
				return
			}
		}
	}()
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
	services := e.sessions.ConfiguredServices()
	if len(services) == 0 {
		return
	}

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

		eps, err := e.endpoints.AvailableEndpoints(ctx, serviceID, domain.RPCTypeJSONRPC)
		if err != nil {
			e.logger.Warn("healthcheck: failed to get endpoints",
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
			checks := append(checker.HealthChecks(probe), e.configured.For(serviceID)...)
			if len(checks) == 0 {
				continue
			}

			sem <- struct{}{}
			e.wg.Add(1)
			go func() {
				defer safego.Recover(e.logger, "healthcheck.endpoint")
				defer e.wg.Done()
				defer func() { <-sem }()
				e.checkEndpoint(ctx, serviceID, probe, group.endpoints, plugin, checks)
			}()
		}
	}

	e.cycle++
}

// backendGroup is the set of endpoints sharing one backend URL.
type backendGroup struct {
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
	if !e.dedupByBackendURL {
		groups := make([]backendGroup, 0, len(eps))
		for _, ep := range eps {
			groups = append(groups, backendGroup{endpoints: domain.EndpointAddrList{ep}})
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
			groups = append(groups, backendGroup{endpoints: domain.EndpointAddrList{ep}})
			continue
		}
		if idx, ok := byURL[url]; ok {
			groups[idx].endpoints = append(groups[idx].endpoints, ep)
			continue
		}
		byURL[url] = len(groups)
		groups = append(groups, backendGroup{endpoints: domain.EndpointAddrList{ep}})
	}
	return groups
}

// checkEndpoint runs all health checks against probe and applies the results to
// every endpoint sharing its backend (probe included).
func (e *Executor) checkEndpoint(
	ctx context.Context,
	serviceID domain.ServiceID,
	probe domain.EndpointAddr,
	siblings domain.EndpointAddrList,
	plugin qos.Plugin,
	checks []qos.HealthCheck,
) {
	for _, check := range checks {
		e.sendCheck(ctx, serviceID, probe, siblings, plugin, check)
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

// transportSignal grades a health check that failed before any response
// existed, using the same evidence and verdict as the relay path
// (heuristic.AnalyzeTransportError) instead of a fixed severity.
//
// A dead host must not look healthier to health checks than it does to
// relays. Beta observed exactly that gap: every transport failure here was
// graded a flat major error, so a DNS-dead host — critical on the relay
// path via the same heuristic — took ~7 health-check cycles to fall below
// the probation threshold, and stayed "vouched for" (reputation.Vouched)
// the whole time, absorbing a method a block had diverted onto it.
//
// The second return is false when nothing should be recorded: a result with
// ShouldPenalize == false means the executor's own context ended (a
// client-cancelled check), which is not evidence about the endpoint.
func transportSignal(checkName string, err error, ctxErr error, latency time.Duration) (reputation.Signal, bool) {
	result := heuristic.AnalyzeTransportError(err, ctxErr)
	if !result.ShouldPenalize {
		return reputation.Signal{}, false
	}
	reason := "health_check: " + checkName + ": " + result.Reason
	switch result.PenaltySeverity {
	case heuristic.SeverityCritical:
		return reputation.NewCriticalErrorSignal(reason, latency), true
	case heuristic.SeverityMajor:
		return reputation.NewMajorErrorSignal(reason, latency), true
	default:
		return reputation.NewMinorErrorSignal(reason, latency), true
	}
}

// sendCheck sends a single health check, processes the response, and records
// reputation signals and observations.
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
) {
	start := time.Now()

	resp, err := e.protocol.SendRelay(ctx, serviceID, ep, check.Payload)
	latency := time.Since(start)

	if err != nil {
		e.logger.Debug("healthcheck: relay error",
			"service_id", serviceID,
			"endpoint", ep,
			"check", check.Name,
			"error", err,
		)
		// Penalize only the endpoint that actually failed. A relay error can be
		// the supplier's session or signing rather than the backend, and there
		// is no response to tell the two apart — blaming the backend's other
		// registrations for it would eject healthy ones.
		if e.repService != nil {
			if signal, ok := transportSignal(check.Name, err, ctx.Err(), latency); ok {
				_ = e.repService.RecordSignal(ctx, serviceID, ep, check.Payload.RPCType(), signal)
			}
		}
		e.submitObservation(serviceID, ep, check.Payload.Bytes(), nil, 0, latency, nil)
		return
	}

	// Extract structured data if the plugin supports it.
	//
	// extractErr is carried down to the reputation signal below: what the body
	// says is part of whether the check passed, not merely a parsing detail.
	var extracted *observe.ExtractedData
	var extractErr error
	if extractor, ok := plugin.(qos.DataExtractor); ok {
		var data *qos.ExtractedData
		data, extractErr = extractor.ExtractData(ep, check.Payload.Bytes(), resp.Body)
		switch {
		case extractErr != nil:
			e.logger.Debug("healthcheck: extract error",
				"service_id", serviceID,
				"endpoint", ep,
				"check", check.Name,
				"error", extractErr,
			)
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
				for _, sibling := range siblings {
					tracker.UpdateBlockHeight(sibling, *data.BlockHeight)
				}
			}
		}
	}

	// Record reputation signal. A configured check may name the penalty its
	// failure carries; the default grading applies to everything else.
	// The backend answered, so what it said grades every registration in front
	// of it. At per-URL key granularity these all resolve to one score anyway;
	// recording them individually is what keeps the finer granularities honest.
	if e.repService != nil {
		signal := checkSignal(check.Name, resp.HTTPStatusCode, extractErr, latency)
		if failed := resp.HTTPStatusCode < 200 || resp.HTTPStatusCode >= 300 || extractErr != nil; failed {
			if configured, ok := e.configured.SignalFor(check.Name, "health_check: "+check.Name, latency); ok {
				signal = configured
			}
		}
		for _, sibling := range siblings {
			_ = e.repService.RecordSignal(ctx, serviceID, sibling, check.Payload.RPCType(), signal)
		}
	}

	e.submitObservation(serviceID, ep, check.Payload.Bytes(), resp.Body, resp.HTTPStatusCode, latency, extracted)
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
