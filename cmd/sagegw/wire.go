// Package main provides the SAGE gateway binary.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/crossvalidation"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/healthcheck"

	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/methodblock"
	"github.com/pokt-network/sage/metrics"
	"github.com/pokt-network/sage/observe"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/protocol/mock"
	"github.com/pokt-network/sage/protocol/shannon"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/cosmos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/qos/noop"
	"github.com/pokt-network/sage/qos/solana"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/responsecache"
	"github.com/pokt-network/sage/router"
	"github.com/pokt-network/sage/tuning"
)

// relayBackend is the protocol surface needed by the middleware chain, the
// health check executor, and the router readiness handlers. Both the Shannon
// protocol and the mock backend satisfy it.
type relayBackend interface {
	protocol.Relayer
	protocol.EndpointProvider
	protocol.SessionManager
}

// App holds all components for lifecycle management.
type App struct {
	Router *router.Router
	// Admin is the runtime control plane. It is not mounted on Router — it has
	// no authentication, so it listens separately (config.AdminConfig) rather
	// than on whatever port relays arrive on. main serves it.
	Admin *router.AdminAPI
	// Protocol is the Shannon protocol when running against the real network;
	// nil in mock mode (WS relays are disabled in mock mode).
	Protocol  *shannon.Protocol
	RepSvc    reputation.Service
	ObsQueue  *observe.Queue
	CrossVal  *crossvalidation.Validator
	Leader    *healthcheck.LeaderElector
	HealthExe *healthcheck.Executor
	Redis     *redis.Client
	Metrics   *metrics.Recorder
	Logger    *slog.Logger
}

// methodBlockLister adapts methodblock.Store to metrics.MethodBlockLister so
// metrics does not import methodblock (nor the reverse).
type methodBlockLister struct{ store *methodblock.Store }

// ActiveMethodBlocks reports the live method blocks for a service, translated
// into the metrics package's own type.
func (l methodBlockLister) ActiveMethodBlocks(serviceID string) []metrics.MethodBlock {
	active := l.store.Active(serviceID)
	out := make([]metrics.MethodBlock, len(active))
	for i, b := range active {
		out[i] = metrics.MethodBlock{Host: b.Host, Method: b.Method}
	}
	return out
}

// serviceIDsFrom lists every configured service ID. It bounds the service_id
// metric label — see metrics.NewRecorder.
func serviceIDsFrom(cfg *config.Config) []domain.ServiceID {
	services := cfg.Gateway.AllServices()
	ids := make([]domain.ServiceID, 0, len(services))
	for _, svc := range services {
		ids = append(ids, domain.ServiceID(svc.ID))
	}
	return ids
}

// Build constructs the full SAGE application from configuration.
func Build(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*App, error) {
	app := &App{Logger: logger}

	// 1. Redis (optional)
	var redisClient *redis.Client
	if cfg.Redis.Address != "" {
		redisClient = redis.NewClient(&redis.Options{
			Addr:         cfg.Redis.Address,
			Password:     cfg.Redis.Password,
			DB:           cfg.Redis.DB,
			PoolSize:     cfg.Redis.PoolSize,
			DialTimeout:  cfg.Redis.DialTimeout,
			ReadTimeout:  cfg.Redis.ReadTimeout,
			WriteTimeout: cfg.Redis.WriteTimeout,
		})
		if err := redisClient.Ping(ctx).Err(); err != nil {
			logger.Warn("Redis connection failed, running in local-only mode", "error", err)
			redisClient.Close()
			redisClient = nil
		} else {
			logger.Info("Redis connected", "address", cfg.Redis.Address)
		}
		app.Redis = redisClient
	}

	// 2. Feature flags. cfg.FeatureFlags is the overrides an operator set; both
	// stores fall back to featureflag.DefaultFlags for anything absent, so the
	// map is passed through as-is. Config carries flags as an open map, so a
	// misspelled or PATH-only flag name would otherwise be accepted silently —
	// warn on each here, the way main warns on cfg.Ignored.
	for name := range cfg.FeatureFlags {
		if !featureflag.IsKnownFlag(name) {
			logger.Warn("feature flag ignored: SAGE has no such flag, and setting it has no effect", "flag", name)
		}
	}
	var flags featureflag.FlagStore
	flags = featureflag.NewMemoryStore(cfg.FeatureFlags)
	if redisClient != nil {
		flags = featureflag.NewRedisStore(redisClient, cfg.FeatureFlags)
	}

	// 3. Reputation
	timeline := reputation.NewTimeline(100)
	var repStorage reputation.Storage
	repStorage = reputation.NewMemoryStorage()
	if redisClient != nil {
		var err error
		repStorage, err = reputation.NewRedisStorage(redisClient, "sage:reputation:")
		if err != nil {
			logger.Warn("Redis reputation storage failed, using memory", "error", err)
			repStorage = reputation.NewMemoryStorage()
		}
	}
	initialScore := 100.0
	if cfg.Gateway.Reputation.InitialScore > 0 {
		initialScore = float64(cfg.Gateway.Reputation.InitialScore)
	}
	// A misspelled granularity must not fall through to the default: it would
	// silently change what scores are attached to, and nothing downstream could
	// tell the difference until an incident.
	keyGranularity := cfg.Gateway.Reputation.KeyGranularity
	if !reputation.ValidKeyGranularity(keyGranularity) {
		return nil, fmt.Errorf(
			"reputation_config.key_granularity %q is not recognised (want one of: %s, %s, %s, %s)",
			keyGranularity,
			reputation.KeyPerURL, reputation.KeyPerEndpoint,
			reputation.KeyPerDomain, reputation.KeyPerSupplier,
		)
	}
	repSvc := reputation.NewService(repStorage, timeline, reputation.ServiceConfig{
		InitialScore:   initialScore,
		MaxScore:       100,
		KeyGranularity: keyGranularity,
	})
	repSvc.Start()
	app.RepSvc = repSvc

	// 4. Protocol backend: Shannon (default) or mock (benchmarks/testing).
	var proto relayBackend
	if cfg.Protocol.Type == config.ProtocolTypeMock {
		serviceIDs := make([]domain.ServiceID, 0, len(cfg.Gateway.AllServices()))
		for _, svc := range cfg.Gateway.AllServices() {
			serviceIDs = append(serviceIDs, domain.ServiceID(svc.ID))
		}
		proto = mock.New(
			serviceIDs,
			cfg.Protocol.Mock.EndpointCount,
			cfg.Protocol.Mock.Latency,
			cfg.Protocol.Mock.ResponseBody,
			logger,
		)
	} else {
		shannonProto, err := shannon.New(*cfg, logger)
		if err != nil {
			return nil, err
		}
		shannonProto.StartBlockPoller(ctx)
		app.Protocol = shannonProto
		proto = shannonProto
	}

	// 5. QoS registry
	qosReg := qos.NewRegistry()
	for _, svc := range cfg.Gateway.AllServices() {
		var plugin qos.Plugin
		switch domain.ServiceType(svc.Type) {
		case domain.ServiceTypeEVM:
			evmCfg := evm.Config{
				SyncAllowance:   svc.SyncAllowance,
				ExpectedChainID: svc.ChainID,
			}
			if err := evmCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %q: %w", svc.ID, err)
			}
			plugin = evm.NewPlugin(logger, evmCfg)
		case domain.ServiceTypeCosmos:
			rpcTypes := make([]domain.RPCType, len(svc.RPCTypes))
			for i, rt := range svc.RPCTypes {
				rpcTypes[i] = domain.RPCType(rt)
			}
			cosmosCfg := cosmos.Config{
				SyncAllowance:     svc.SyncAllowance,
				SupportedRPCTypes: rpcTypes,
				ExpectedChainID:   svc.ChainID,
			}
			if err := cosmosCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %q: %w", svc.ID, err)
			}
			plugin = cosmos.NewPlugin(logger, cosmosCfg)
		case domain.ServiceTypeSolana:
			plugin = solana.NewPlugin(logger, svc.SyncAllowance)
		default:
			plugin = noop.NewPlugin(logger, svc.SyncAllowance)
		}
		_ = qosReg.Register(domain.ServiceID(svc.ID), plugin)
	}
	logger.Info("QoS plugins registered", "count", qosReg.Count())

	// 6. Circuit breaker
	cb := circuitbreaker.New(circuitbreaker.WithRedis(redisClient), circuitbreaker.WithLogger(logger))

	// Circuit-breaker state is derived at scrape time rather than pushed: breaks
	// expire lazily, so there is no event a gauge could hang off. See
	// metrics.BreakerCollector.
	prometheus.MustRegister(metrics.NewBreakerCollector(cb, serviceIDsFrom(cfg)))

	// Reputation scores are likewise derived at scrape time rather than pushed.
	// A pushed gauge keyed on an endpoint identity never evicts, so every
	// supplier registration that has ever been scored would keep costing heap
	// and scrape bytes until restart. See metrics.ScoreCollector.
	prometheus.MustRegister(metrics.NewScoreCollector(repSvc, serviceIDsFrom(cfg)))

	// 6b. Method blocks: per-host, per-method memory consulted at selection.
	// Local memory only — see the methodblock package doc.
	blocks := methodblock.New(
		methodblock.WithTTL(cfg.Gateway.MethodBlocks.EffectiveTTL()),
		methodblock.WithEscalation(cfg.Gateway.MethodBlocks.EffectiveEscalation()),
		methodblock.WithLogger(logger),
	)
	blocks.StartSweep(ctx)
	prometheus.MustRegister(metrics.NewMethodBlockCollector(methodBlockLister{blocks}, serviceIDsFrom(cfg)))

	// A recovered panic is contained, not harmless — surface it as a metric so
	// it can be alerted on rather than only appearing in logs.
	prometheus.MustRegister(metrics.NewPanicCollector())

	// blocked_domains is compiled inside the Shannon protocol, where endpoints
	// are handed out. Validate it here too, so a malformed entry fails at
	// startup under every backend rather than only the one that reads it.
	if err := shannon.ValidateBlockedDomains(cfg.Gateway.BlockedDomains); err != nil {
		return nil, err
	}

	// 7. Observation pipeline
	obsHandler := observe.NewDefaultHandler(qosReg, logger)
	obsQueue := observe.NewQueue(observe.QueueConfig{
		Enabled:     cfg.Gateway.ObservationPipeline.Enabled,
		SampleRate:  cfg.Gateway.ObservationPipeline.SampleRate,
		WorkerCount: cfg.Gateway.ObservationPipeline.WorkerCount,
		QueueSize:   cfg.Gateway.ObservationPipeline.QueueSize,
	}, obsHandler, logger)
	obsQueue.Start(ctx)
	app.ObsQueue = obsQueue

	// 8. Response cache
	respCache := responsecache.NewCache(10000)

	// 9. Cross-validation
	crossVal := crossvalidation.NewValidator(logger)
	crossVal.Start(ctx)
	app.CrossVal = crossVal

	// 10. Metrics recorder
	recorder := metrics.NewRecorder(serviceIDsFrom(cfg))
	app.Metrics = recorder

	// The breaker's failure-rate gate keys on hostname; every relay counter
	// keys on service. Exposing the gate's own inputs is the only way to tell
	// one bad host behind an operator from an operator that is bad everywhere.
	cb.SetOutcomeHook(func(serviceID, brokenDomain, outcome string) {
		recorder.RecordCircuitBreakerOutcome(domain.ServiceID(serviceID), brokenDomain, outcome)
	})

	// Supplier blacklists and relay miner errors are recorded by the protocol
	// itself: both are decided inside response validation, below the middleware
	// chain that carries the recorder.
	if app.Protocol != nil {
		app.Protocol.SetMetrics(recorder)
	}

	// The reputation pool-collapse guard serves a below-threshold endpoint when
	// no endpoint clears the floor. That keeps the service up, which is the
	// point — but it must not be silent, or a pool that has degraded to
	// "everything is bad" looks identical to a healthy one.
	repSvc.SetCollapseHook(func(serviceID domain.ServiceID) {
		recorder.RecordDegraded(serviceID, "reputation_pool_collapse")
	})

	// Per-operator concentration cap. Gated per relay so an operator can turn
	// it off at runtime — globally or for one service — without a deploy.
	repSvc.SetOperatorCap(
		reputation.OperatorCapConfig{
			MaxShare:            cfg.Gateway.Reputation.MaxOperatorShare,
			TwoOperatorMaxShare: cfg.Gateway.Reputation.MaxOperatorShareTwoOperators,
			DisplacementCeiling: cfg.Gateway.Reputation.OperatorDisplacementCeiling,
		},
		func(ctx context.Context, serviceID domain.ServiceID) bool {
			return flags.IsEnabled(ctx, featureflag.FlagOperatorAwareSelection, serviceID)
		},
	)

	// 11. Per-service config functions.
	//
	// Each reads the config value and then lets the tuning store override it,
	// which is what makes a knob changeable without a restart: the middlewares
	// call these per request, so the next relay picks up whatever the admin API
	// last stored. A knob that is NOT read through a closure like this cannot be
	// made runtime-changeable by registering it — see the tuning package doc.
	tuningStore := tuning.NewStore()
	retryFn := newRetryFn(cfg, tuningStore)
	timeoutFn := newTimeoutFn(cfg, tuningStore)

	// 12. Build middleware chain.
	//
	// Each middleware registers under its canonical name; the chain is then
	// composed in the order the config asks for (gateway_config.middleware_chain),
	// falling back to relay.DefaultChainOrder. Registration order here is
	// irrelevant — the chain order is the one below.
	//
	// FIRST in the order is outermost (runs first). SendRelay is terminal (never
	// calls next) and must be last. BuildChain enforces the ordering invariants
	// at startup; see relay/chain_order.go.
	trustedProxies, err := middleware.ParseTrustedProxies(cfg.Router.TrustedProxies)
	if err != nil {
		return nil, fmt.Errorf("router_config.trusted_proxies: %w", err)
	}

	mwReg := relay.NewMiddlewareRegistry()
	mwReg.Register(relay.MWShadow, func() relay.Middleware { return middleware.Shadow(flags) })
	mwReg.Register(relay.MWTracing, func() relay.Middleware { return middleware.Tracing(flags) })
	mwReg.Register(relay.MWTimeout, func() relay.Middleware { return middleware.Timeout(timeoutFn) })
	mwReg.Register(relay.MWRequestID, func() relay.Middleware { return middleware.RequestID() })
	mwReg.Register(relay.MWClientIP, func() relay.Middleware { return middleware.ClientIP(trustedProxies) })
	mwReg.Register(relay.MWMetrics, func() relay.Middleware { return middleware.Metrics(recorder) })
	mwReg.Register(relay.MWParse, func() relay.Middleware { return middleware.Parse(qosReg) })
	mwReg.Register(relay.MWValidate, func() relay.Middleware {
		return middleware.Validate(cfg.Gateway.AllServices())
	})
	mwReg.Register(relay.MWCache, func() relay.Middleware { return middleware.Cache(flags, respCache) })
	mwReg.Register(relay.MWBatch, func() relay.Middleware {
		return middleware.Batch(cfg.Concurrency.MaxConcurrentRelays, cfg.Concurrency.MaxBatchPayloads)
	})
	mwReg.Register(relay.MWSingleflight, func() relay.Middleware { return middleware.Singleflight(flags) })
	mwReg.Register(relay.MWObserve, func() relay.Middleware {
		return middleware.Observe(flags, obsQueue, repSvc)
	})
	mwReg.Register(relay.MWCrossValidate, func() relay.Middleware {
		return middleware.CrossValidate(flags, crossVal)
	})
	mwReg.Register(relay.MWRetry, func() relay.Middleware { return middleware.Retry(flags, retryFn) })
	mwReg.Register(relay.MWHedge, func() relay.Middleware { return middleware.Hedge(flags, retryFn) })
	mwReg.Register(relay.MWSupplierAffinity, func() relay.Middleware {
		return middleware.SupplierAffinity(flags, 10*time.Second)
	})
	mwReg.Register(relay.MWCircuitBreak, func() relay.Middleware {
		return middleware.CircuitBreak(cb, proto, flags, recorder)
	})
	mwReg.Register(relay.MWMethodBlocks, func() relay.Middleware {
		return middleware.MethodBlocks(blocks, qosReg, flags, repSvc, recorder)
	})
	mwReg.Register(relay.MWSelectEndpoint, func() relay.Middleware {
		return middleware.SelectEndpoint(repSvc, proto, qosReg, flags)
	})
	mwReg.Register(relay.MWDebugLog, func() relay.Middleware { return middleware.DebugLog(flags) })
	mwReg.Register(relay.MWHeuristic, func() relay.Middleware { return middleware.Heuristic(flags) })
	mwReg.Register(relay.MWSendRelay, func() relay.Middleware { return middleware.SendRelay(proto) })

	order := cfg.Gateway.EffectiveMiddlewareChain()
	if len(order) == 0 {
		order = relay.DefaultChainOrder()
	} else {
		logger.Info("middleware chain set by config", "chain", order)
	}

	// A middleware that is registered but unnamed by the chain does not run. That
	// is legitimate (a config may deliberately drop tracing), but it is also how
	// someone adds a middleware, forgets the order, and concludes it is broken.
	for _, name := range mwReg.RegisteredNames() {
		if !slices.Contains(order, name) {
			logger.Warn("middleware is registered but not in the chain, so it will not run",
				"middleware", name)
		}
	}

	chain, err := mwReg.BuildChain(order)
	if err != nil {
		return nil, fmt.Errorf("build middleware chain: %w", err)
	}

	// send_relay is the only middleware that actually relays. Without it the
	// chain parses, selects an endpoint, and hands the request to the registry's
	// terminal, which errors — a gateway that answers nothing. That is a config
	// mistake, so say so once at startup rather than once per request forever.
	//
	// Checked after BuildChain so that a chain naming something unknown is told
	// so, rather than being told send_relay is missing — which is true, but is
	// the consequence rather than the mistake.
	if !slices.Contains(order, relay.MWSendRelay) {
		return nil, fmt.Errorf("build middleware chain: %q is missing from the configured chain; "+
			"without it no request is ever relayed", relay.MWSendRelay)
	}

	// 13. Health checks
	leader := healthcheck.NewLeaderElector(redisClient, logger)
	leader.Start(ctx)
	app.Leader = leader

	healthExe := healthcheck.NewExecutor(
		proto, proto, proto,
		qosReg, repSvc, obsQueue,
		30*time.Second, 4, logger,
	)

	// Health checks declared in YAML, in addition to the plugin's own. A rule
	// that could not be built is skipped and said out loud — a check silently
	// missing reads to an operator as a check that is passing.
	configuredChecks, checkWarnings := healthcheck.BuildConfiguredChecks(cfg.Gateway.HealthChecks)
	for _, warning := range checkWarnings {
		logger.Warn("health check config: " + warning)
	}
	healthExe.SetConfiguredChecks(configuredChecks)
	healthExe.SetBackendURLDedup(!cfg.Gateway.HealthChecks.DisableBackendURLDedup)

	healthExe.Start(ctx)
	app.HealthExe = healthExe

	// 14. External block height fetchers
	for _, svc := range cfg.Gateway.AllServices() {
		if len(svc.ExternalBlockSources) == 0 {
			continue
		}
		fetcher := healthcheck.NewExternalBlockFetcher(domain.ServiceID(svc.ID), svc.ExternalBlockSources, logger)
		heights := fetcher.Start(ctx)
		plugin := qosReg.Get(domain.ServiceID(svc.ID))
		if tracker, ok := plugin.(qos.BlockHeightTracker); ok {
			safego.Go(logger, "external.blockheight.fanin", func() {
				for h := range heights {
					tracker.UpdateBlockHeight("external", h.Height)
				}
			})
		}
	}

	// 15. WebSocket relayer — single public entry point for WS upgrades.
	// Requires the concrete Shannon protocol (per-frame signing); in mock mode
	// it stays nil and the router answers WS upgrades with 503.
	var wsRelayer router.WebSocketOpener
	if app.Protocol != nil {
		// Each field resolves its own default. The struct-wide check this
		// replaced applied defaults only when *nothing* was set, so tuning one
		// WebSocket setting silently zeroed the other two — taking WS
		// observability with it, with no error and no log line.
		wsCfg := cfg.WebSocket
		wsRelayer = shannon.NewWSRelayer(shannon.WSRelayerDeps{
			Protocol:                   app.Protocol,
			Reputation:                 repSvc,
			Observe:                    obsQueue,
			Flags:                      flags,
			Logger:                     logger,
			FrameObservationSampleRate: wsCfg.EffectiveFrameObservationSampleRate(),
			CloseObservationSampleRate: wsCfg.EffectiveCloseObservationSampleRate(),
			MaxConcurrentConnections:   wsCfg.EffectiveMaxConcurrentConnections(),
		})
	}

	// 16. Admin API + Router
	app.Admin = router.NewAdminAPI(flags, repSvc, timeline, cb, blocks, qosReg, tuningStore, logger)
	app.Router = router.New(cfg.Router, chain, proto, wsRelayer, logger)

	return app, nil
}

// newRetryFn resolves retry/hedge settings for a service: the config value,
// then whatever the tuning store has been told to override.
//
// The middlewares call this per request, which is the whole mechanism behind
// changing a knob without a restart — the next relay reads whatever the admin
// API last stored. A setting captured once at wire time cannot be made
// runtime-changeable by registering a knob for it; see the tuning package doc.
func newRetryFn(cfg *config.Config, store *tuning.Store) func(domain.ServiceID) config.RetryConfig {
	return func(serviceID domain.ServiceID) config.RetryConfig {
		base := cfg.Gateway.EffectiveDefaults().Retry
		if svc := cfg.Gateway.GetServiceConfig(string(serviceID)); svc != nil {
			base = svc.EffectiveRetry(cfg.Gateway.EffectiveDefaults())
		}
		base.MaxRetries = store.Int(tuning.KnobRetryMaxRetries, serviceID, base.MaxRetries)
		base.MaxLatency = store.Duration(tuning.KnobRetryMaxLatency, serviceID, base.MaxLatency)
		base.HedgeDelay = store.Duration(tuning.KnobHedgeDelay, serviceID, base.HedgeDelay)
		return base
	}
}

// newTimeoutFn resolves the per-relay timeout for a service, with the same
// config-then-override layering as newRetryFn.
func newTimeoutFn(cfg *config.Config, store *tuning.Store) func(domain.ServiceID) time.Duration {
	return func(serviceID domain.ServiceID) time.Duration {
		base := cfg.Gateway.EffectiveDefaults().Timeout.RelayTimeout
		if svc := cfg.Gateway.GetServiceConfig(string(serviceID)); svc != nil {
			base = svc.EffectiveTimeout(cfg.Gateway.EffectiveDefaults()).RelayTimeout
		}
		return store.Duration(tuning.KnobRelayTimeout, serviceID, base)
	}
}
