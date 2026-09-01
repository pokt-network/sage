// Package main provides the SAGE gateway binary.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/sage/blocklist"
	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/crossvalidation"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
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
	"github.com/pokt-network/sage/traffic"
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
	Protocol *shannon.Protocol
	RepSvc   reputation.Service
	ObsQueue *observe.Queue
	CrossVal *crossvalidation.Validator
	// Config is the current config snapshot. Build stores the boot config here;
	// a reload (POST /admin/reload, not yet implemented) swaps it with
	// Config.Store, and every closure that resolves a per-service knob
	// (newRetryFn, newTimeoutFn) loads it fresh on each call so the next relay
	// after a reload sees the new value. A field read once at wire time and
	// captured in a closure cannot be made reloadable this way — see the
	// tuning package doc.
	Config atomic.Pointer[config.Config]
	// ConfigPath is the -config flag value main started with. Build does not
	// set it — it has no such flag, only a *config.Config — so main sets it
	// after Build returns. A reload re-reads from this path; empty means the
	// config came from GATEWAY_CONFIG as inline YAML, and there is nothing to
	// re-read (see reload.ErrNoConfigFile).
	ConfigPath string
	// reloadMu serialises Reload. Two reloads racing would interleave their
	// apply steps and both report success.
	reloadMu sync.Mutex
	// Flags, MethodBlocks and HealthExe are the runtime seams a reload writes
	// through. They are held here rather than captured in a closure for the
	// same reason Config is an atomic pointer: a seam nothing can reach is not
	// a seam.
	Flags        featureflag.FlagStore
	MethodBlocks *methodblock.Store
	// blockedDomains is the operator domain ban's swap point. Nil under the
	// mock backend, which hands out endpoints without consulting one — a
	// reload says so rather than reporting the section applied.
	blockedDomains blockedDomainSetter
	// blocklist is the same manager, for the admin API's routes.
	blocklist *blocklist.Manager
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

// blocklistPollInterval is how often each replica re-reads the admin-set
// domain bans from Redis. Same order as the drain refresh: a ban set on one
// replica is in force everywhere within it.
const blocklistPollInterval = 5 * time.Second

// drainLister adapts drain.Store to metrics.DrainLister so metrics does not
// import drain (nor the reverse).
type drainLister struct{ store drain.Store }

// ActiveDrains reports the live operator drains for a service, translated
// into the metrics package's own type.
func (l drainLister) ActiveDrains(serviceID string) []metrics.DrainEntry {
	active := l.store.Active(context.Background(), domain.ServiceID(serviceID))
	out := make([]metrics.DrainEntry, len(active))
	for i, e := range active {
		out[i] = metrics.DrainEntry{Domain: e.Operator, RPCType: string(e.RPCType)}
	}
	return out
}

// trafficSummaryLister adapts traffic.Sampler to metrics.TrafficSummaryLister
// so metrics does not import traffic (nor the reverse).
type trafficSummaryLister struct{ sampler *traffic.Sampler }

// PreviousWindow reports serviceID's previous (complete) request-sample
// window, translated into the metrics package's own return shape. The staleness
// rule that decides whether there is anything to report lives in the sampler,
// with the windows it is about.
func (l trafficSummaryLister) PreviousWindow(serviceID string) (distinctRatio, top1Share float64, ok bool) {
	return l.sampler.PreviousWindow(domain.ServiceID(serviceID))
}

// defaultHealthCheckInterval is how often the leader probes every backend when
// active_health_checks.interval is unset. The effective interval also sizes
// a booting follower's replay window (two intervals).
const defaultHealthCheckInterval = 30 * time.Second

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
	// Everything a config can be wrong about that this package can check,
	// before any of it is acted on. The same function guards a reload, so a
	// file the runtime accepts is a file the binary would boot with.
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	app := &App{Logger: logger}
	app.Config.Store(cfg)

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
	app.Flags = flags

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
	// key_granularity is checked in validateConfig, above: a misspelled value
	// must not fall through to the default, because it silently changes what
	// scores attach to.
	// Write-behind goes to Redis from the leader only: several replicas each
	// writing their own view of a key was last-writer-wins, nobody's view.
	// The elector is built below; the closure reads it once it exists.
	var leader *healthcheck.LeaderElector
	repStorage = reputation.NewLeaderOnlyStorage(repStorage, func() bool {
		return leader == nil || leader.IsLeader()
	})
	repSvc := reputation.NewService(repStorage, timeline, reputation.ServiceConfig{
		InitialScore:   initialScore,
		MaxScore:       100,
		KeyGranularity: cfg.Gateway.Reputation.KeyGranularity,
		Impacts:        cfg.Gateway.Reputation.Impacts(),
		Rate:           cfg.Gateway.Reputation.RateConfig(),
		Selector:       cfg.Gateway.Reputation.SelectorConfig(),
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
		).WithFailureRates(cfg.Protocol.Mock.FailureRates)
	} else {
		shannonProto, err := shannon.New(*cfg, logger)
		if err != nil {
			return nil, err
		}
		shannonProto.StartBlockPoller(ctx)
		app.Protocol = shannonProto
		proto = shannonProto

		// The domain ban's swap point is the blocklist manager, not the
		// protocol: it owns the union of the config list and the admin-set
		// bans, so a reload (which rewrites the config half) and an admin
		// PUT (the other half) never overwrite each other. Redis when
		// available so a ban reaches every replica and outlives a restart;
		// memory otherwise, and the admin API says which.
		var backend blocklist.Backend = blocklist.NewMemoryBackend()
		var blOpts []blocklist.Option
		if redisClient != nil {
			backend = blocklist.NewRedisBackend(redisClient)
			blOpts = append(blOpts, blocklist.WithPollInterval(blocklistPollInterval), blocklist.WithShared(true))
		}
		blocked := blocklist.New(shannonProto, backend, cfg.Gateway.BlockedDomains, blOpts...)
		if err := blocked.Start(ctx); err != nil {
			// The config half is already compiled into the protocol; what
			// failed is reading the admin half back. Not fatal: bans set
			// while Redis is away land locally and the poll catches up.
			logger.Warn("blocked domains: reading admin-set bans from redis failed, starting with the config list", "error", err)
		}
		app.blockedDomains = blocked
		app.blocklist = blocked
		prometheus.MustRegister(metrics.NewBlockedDomainsGauge(blocked.Len))
	}

	// 5. QoS registry
	qosReg := qos.NewRegistry()
	for _, svc := range cfg.Gateway.AllServices() {
		// The plugin configs are built by the same helpers validateConfig used
		// to check them, so what was validated is what gets constructed.
		var plugin qos.Plugin
		switch domain.ServiceType(svc.Type) {
		case domain.ServiceTypeEVM:
			plugin = evm.NewPlugin(logger, evmConfigFor(svc))
		case domain.ServiceTypeCosmos:
			plugin = cosmos.NewPlugin(logger, cosmosConfigFor(svc))
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
	prometheus.MustRegister(metrics.NewScoreCollector(repSvc, serviceIDsFrom(cfg), 100))
	// The timeline is bounded by design; this is the gauge that proves it.
	prometheus.MustRegister(metrics.NewTimelineKeysGauge(timeline.Len))

	// 6b. Method blocks: per-host, per-method memory consulted at selection.
	// Local memory only — see the methodblock package doc.
	blocks := methodblock.New(
		methodblock.WithTTL(cfg.Gateway.MethodBlocks.EffectiveTTL()),
		methodblock.WithEscalation(cfg.Gateway.MethodBlocks.EffectiveEscalation()),
		methodblock.WithLogger(logger),
	)
	blocks.StartSweep(ctx)
	app.MethodBlocks = blocks
	prometheus.MustRegister(metrics.NewMethodBlockCollector(methodBlockLister{blocks}, serviceIDsFrom(cfg)))

	// 6c. Request-shape sampler: per-service traffic diversity, sampled and
	// windowed — see package traffic. Local memory only, like method blocks.
	sampler := traffic.New()
	prometheus.MustRegister(metrics.NewTrafficCollector(trafficSummaryLister{sampler}, serviceIDsFrom(cfg)))

	// 6d. Operator drain store: shared through Redis when available, else
	// process-local memory — see package drain.
	//
	// Only built when there is a Shannon protocol to enforce it. The mock
	// backend hands out endpoints without consulting a drain store, so a store
	// wired there would take the request, store the entry and answer
	// `applied: true` for a drain that benches nothing — the admin API
	// reporting a bench that is not happening. With no store, the drain routes
	// answer 503 and say so.
	var drainStore drain.Store
	if app.Protocol != nil {
		if redisClient != nil {
			redisDrains := drain.NewRedisStore(redisClient, drain.WithLogger(logger))
			redisDrains.Start(ctx)
			drainStore = redisDrains
		} else {
			drainStore = drain.NewMemoryStore()
		}
		app.Protocol.SetDrains(drainStore)
		prometheus.MustRegister(metrics.NewDrainCollector(drainLister{drainStore}, serviceIDsFrom(cfg)))
	}

	// A recovered panic is contained, not harmless — surface it as a metric so
	// it can be alerted on rather than only appearing in logs.
	prometheus.MustRegister(metrics.NewPanicCollector())

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

	// Count what actually reached reputation. The gap between relays served and
	// signals recorded is the thing scoring gets wrong quietly — an endpoint
	// with 40000 attempts, all of them probes, scores like a well-tested one.
	repSvc.SetSignalHook(func(serviceID domain.ServiceID, rpcType domain.RPCType, signal reputation.SignalType, probe bool) {
		recorder.RecordReputationAttempt(serviceID, string(rpcType), string(signal), probe)
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
	retryFn := newRetryFn(app.Config.Load, tuningStore)
	timeoutFn := newTimeoutFn(app.Config.Load, tuningStore)

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
	rpcTypesFn := func(svcID domain.ServiceID) []string {
		if sc := cfg.Gateway.GetServiceConfig(string(svcID)); sc != nil {
			return sc.RPCTypes
		}
		return nil
	}
	mwReg.Register(relay.MWParse, func() relay.Middleware { return middleware.ParseWithServices(qosReg, rpcTypesFn) })
	mwReg.Register(relay.MWValidate, func() relay.Middleware {
		return middleware.Validate(cfg.Gateway.AllServices())
	})
	mwReg.Register(relay.MWCache, func() relay.Middleware { return middleware.CacheWithRecorder(flags, respCache, recorder) })
	mwReg.Register(relay.MWBatch, func() relay.Middleware {
		return middleware.Batch(cfg.Concurrency.MaxConcurrentRelays, cfg.Concurrency.MaxBatchPayloads, flags, repSvc)
	})
	mwReg.Register(relay.MWSingleflight, func() relay.Middleware { return middleware.Singleflight(flags) })
	mwReg.Register(relay.MWObserve, func() relay.Middleware {
		return middleware.Observe(flags, obsQueue, repSvc, sampler)
	})
	mwReg.Register(relay.MWCrossValidate, func() relay.Middleware {
		return middleware.CrossValidate(flags, crossVal)
	})
	mwReg.Register(relay.MWRetry, func() relay.Middleware { return middleware.RetryWithRecorder(flags, retryFn, recorder) })
	mwReg.Register(relay.MWHedge, func() relay.Middleware { return middleware.HedgeWithRecorder(flags, retryFn, recorder) })
	mwReg.Register(relay.MWSupplierAffinity, func() relay.Middleware {
		return middleware.SupplierAffinity(flags, 10*time.Second)
	})
	mwReg.Register(relay.MWCircuitBreak, func() relay.Middleware {
		return middleware.CircuitBreak(cb, proto, flags, recorder)
	})
	mwReg.Register(relay.MWMethodBlocks, func() relay.Middleware {
		return middleware.MethodBlocks(blocks, qosReg, proto, flags, repSvc, recorder)
	})
	mwReg.Register(relay.MWSelectEndpoint, func() relay.Middleware {
		return middleware.SelectEndpoint(repSvc, proto, qosReg, flags)
	})
	mwReg.Register(relay.MWScore, func() relay.Middleware { return middleware.Score(flags, repSvc) })
	mwReg.Register(relay.MWDebugLog, func() relay.Middleware { return middleware.DebugLog(flags) })
	mwReg.Register(relay.MWHeuristic, func() relay.Middleware { return middleware.Heuristic(flags) })
	mwReg.Register(relay.MWSendRelay, func() relay.Middleware { return middleware.SendRelay(proto) })

	order := cfg.Gateway.EffectiveMiddlewareChain()
	if len(order) == 0 {
		order = relay.DefaultChainOrder()
	} else {
		logger.Info("middleware chain set by config", "chain", order)
	}

	// A chain that observes but does not score records no reputation under
	// scoring_v2 — see warnIfUnscoredChain.
	if msg, unscored := warnIfUnscoredChain(order); unscored {
		logger.Warn(msg)
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

	// 13. Health checks
	leader = healthcheck.NewLeaderElector(redisClient, logger)
	leader.Start(ctx)
	app.Leader = leader

	healthCheckInterval := cfg.Gateway.HealthChecks.Interval
	if healthCheckInterval <= 0 {
		healthCheckInterval = defaultHealthCheckInterval
	}
	healthExe := healthcheck.NewExecutor(
		proto, proto, proto,
		qosReg, repSvc, obsQueue,
		healthCheckInterval, 4, logger,
	)
	logger.Info("health checks: probe interval", "interval", healthCheckInterval)
	for _, svc := range cfg.Gateway.HealthChecks.Local {
		if svc.CheckInterval > 0 && svc.CheckInterval < healthCheckInterval {
			logger.Warn("health checks: service probes faster than the global interval; each probe is a paid relay",
				"service_id", svc.ServiceID, "check_interval", svc.CheckInterval, "interval", healthCheckInterval)
		}
	}

	// Health checks declared in YAML, in addition to the plugin's own. A rule
	// that could not be built is skipped and said out loud — a check silently
	// missing reads to an operator as a check that is passing.
	configuredChecks, checkWarnings := healthcheck.BuildConfiguredChecks(cfg.Gateway.HealthChecks)
	for _, warning := range checkWarnings {
		logger.Warn("health check config: " + warning)
	}
	healthExe.SetConfiguredChecks(configuredChecks)
	healthExe.SetBackendURLDedup(!cfg.Gateway.HealthChecks.DisableBackendURLDedup)
	// Probe once, apply everywhere: only the leader sends probe relays (each
	// one is paid for from the app's stake); it publishes every result to a
	// Redis stream and every replica applies them. Without Redis the elector
	// is always leader and there is nothing to publish to — unchanged.
	healthExe.SetLeader(leader)
	healthExe.SetResultRecorder(recorder)
	if redisClient != nil {
		probeStream := healthcheck.NewRedisProbeStream(redisClient, leader.ID(), 2*healthCheckInterval)
		healthExe.SetProbeSink(probeStream)
		healthExe.SetProbeSource(probeStream)
	}

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
			Metrics:                    metrics.NewWebSocketMetrics(serviceIDsFrom(cfg)),
			QoS:                        qosReg,
		})
	}

	// 16. Admin API + Router
	//
	// endpoints is proto, which already satisfies protocol.EndpointProvider
	// for the relay chain — the same value the middleware chain's
	// SelectEndpoint and CircuitBreak use.
	app.Admin = router.NewAdminAPI(flags, repSvc, timeline, cb, blocks, drainStore, proto, cfg.Admin.EffectiveMaxDrain(), qosReg, tuningStore, app, sampler, logger)
	// The WS relayer also answers the admin rebind route. Type-asserted
	// rather than typed: wsRelayer is the router's opener interface, nil
	// under the mock backend, and the rebinder is the same object.
	if rb, ok := wsRelayer.(router.WSRebinder); ok {
		app.Admin.SetWebSocketRebinder(rb)
	}
	if app.blocklist != nil {
		app.Admin.SetBlocklist(app.blocklist)
	}
	app.Router = router.New(cfg.Router, chain, proto, wsRelayer, logger)
	app.Router.SetClientMetrics(recorder)
	// Gate /ready on reputation warm-up so a fresh or rolled pod is not put
	// into the Service until it can steer selection. Only when health checks
	// are enabled — otherwise nothing warms and readiness must not stall.
	if cfg.Gateway.HealthChecks.Enabled {
		app.Router.SetWarmup(healthExe)
	}

	return app, nil
}

// newRetryFn resolves retry/hedge settings for a service: the config value,
// then whatever the tuning store has been told to override.
//
// The middlewares call this per request, which is the whole mechanism behind
// changing a knob without a restart — the next relay reads whatever the admin
// API last stored. cfgFn is App.Config.Load, so a reload that swaps the
// stored config (App.Config.Store) is picked up the same way: the next call
// re-reads the pointer rather than a config captured once at wire time. A
// setting NOT read through a closure like this cannot be made
// runtime-changeable by registering a knob for it; see the tuning package doc.
func newRetryFn(cfgFn func() *config.Config, store *tuning.Store) func(domain.ServiceID) config.RetryConfig {
	return func(serviceID domain.ServiceID) config.RetryConfig {
		cfg := cfgFn()
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
func newTimeoutFn(cfgFn func() *config.Config, store *tuning.Store) func(domain.ServiceID) time.Duration {
	return func(serviceID domain.ServiceID) time.Duration {
		cfg := cfgFn()
		base := cfg.Gateway.EffectiveDefaults().Timeout.RelayTimeout
		if svc := cfg.Gateway.GetServiceConfig(string(serviceID)); svc != nil {
			base = svc.EffectiveTimeout(cfg.Gateway.EffectiveDefaults()).RelayTimeout
		}
		return store.Duration(tuning.KnobRelayTimeout, serviceID, base)
	}
}
