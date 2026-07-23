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
		protocol:    protocol,
		endpoints:   endpoints,
		sessions:    sessions,
		qosRegistry: qosReg,
		repService:  repSvc,
		obsQueue:    obsQueue,
		logger:      logger,
		interval:    interval,
		workers:     workers,
	}
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
				e.runOnce(ctx)
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

		for _, ep := range eps {
			ep := ep // capture for goroutine
			// Plugin checks first: they feed block height and chain ID
			// tracking, so they must run even when a config adds its own.
			checks := append(checker.HealthChecks(ep), e.configured.For(serviceID)...)
			if len(checks) == 0 {
				continue
			}

			sem <- struct{}{}
			e.wg.Add(1)
			go func() {
				defer e.wg.Done()
				defer func() { <-sem }()
				e.checkEndpoint(ctx, serviceID, ep, plugin, checks)
			}()
		}
	}
}

// checkEndpoint runs all health checks for a single endpoint.
func (e *Executor) checkEndpoint(
	ctx context.Context,
	serviceID domain.ServiceID,
	ep domain.EndpointAddr,
	plugin qos.Plugin,
	checks []qos.HealthCheck,
) {
	for _, check := range checks {
		e.sendCheck(ctx, serviceID, ep, plugin, check)
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

// sendCheck sends a single health check, processes the response, and records
// reputation signals and observations.
func (e *Executor) sendCheck(
	ctx context.Context,
	serviceID domain.ServiceID,
	ep domain.EndpointAddr,
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
		if e.repService != nil {
			_ = e.repService.RecordSignal(ctx, serviceID, ep,
				reputation.NewMajorErrorSignal("health_check: "+check.Name, latency))
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
			// Update block height tracker if available.
			if tracker, ok := plugin.(qos.BlockHeightTracker); ok && data.BlockHeight != nil {
				tracker.UpdateBlockHeight(ep, *data.BlockHeight)
			}
		}
	}

	// Record reputation signal. A configured check may name the penalty its
	// failure carries; the default grading applies to everything else.
	if e.repService != nil {
		signal := checkSignal(check.Name, resp.HTTPStatusCode, extractErr, latency)
		if failed := resp.HTTPStatusCode < 200 || resp.HTTPStatusCode >= 300 || extractErr != nil; failed {
			if configured, ok := e.configured.SignalFor(check.Name, "health_check: "+check.Name, latency); ok {
				signal = configured
			}
		}
		_ = e.repService.RecordSignal(ctx, serviceID, ep, signal)
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
