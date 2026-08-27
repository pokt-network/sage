package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on http.DefaultServeMux
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/reload"
	"github.com/pokt-network/sage/router"
)

var (
	Version   string
	Commit    string
	BuildDate string
)

func main() {
	log.Printf(`{"level":"info","message":"SAGE 🌿 gateway starting..."}`)

	// Parse flags
	configPath := flag.String("config", "", "path to config YAML file")
	flag.Parse()

	// Load config
	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf(`{"level":"fatal","error":"%v","message":"failed to load config"}`, err)
	}

	// Initialize logger
	level := parseLogLevel(cfg.Logger.Level)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// Config keys SAGE parsed but has no field for. Not fatal — SAGE is meant to
	// load a PATH config unmodified, so a key describing a feature it does not
	// have is expected. Saying so is the point: an ignored key looks live to
	// whoever wrote it.
	for _, f := range cfg.Ignored {
		logger.Warn("config key ignored: SAGE does not implement this setting, and it has no effect", "detail", f)
	}

	// The quieter half: keys that DO parse into a field and are read by
	// nothing. These are worse than unknown keys, because they survive the
	// round trip and show up in GET /admin/config looking live.
	for _, f := range cfg.Inert {
		logger.Warn("config key has no effect: SAGE parses this setting but nothing reads it", "detail", f)
	}

	// The third case: a setting that is read, does something, and probably
	// does not do what whoever wrote it expected. It loads — refusing a PATH
	// config over it would break the compatibility promise — so this line is
	// the only place it is ever mentioned.
	for _, f := range cfg.Warnings {
		logger.Warn("config setting is probably not what was meant", "detail", f)
	}

	// Background context for all services
	ctx, cancel := context.WithCancel(context.Background())

	// Build the application
	app, err := Build(ctx, cfg, logger)
	if err != nil {
		log.Fatalf(`{"level":"fatal","error":"%v","message":"failed to build application"}`, err)
	}
	// Build only takes the already-parsed *config.Config, not the path it came
	// from, so a reload (which needs to re-read the file) is threaded through
	// here instead. Empty when config came from GATEWAY_CONFIG rather than
	// -config: that source is inline YAML, not a path, so there is nothing to
	// re-read from disk.
	app.ConfigPath = *configPath

	// Log startup summary
	services := cfg.Gateway.AllServices()
	serviceIDs := make([]string, len(services))
	for i, s := range services {
		serviceIDs[i] = s.ID
	}
	versionInfo := "dev"
	if Version != "" {
		versionInfo = Version
		if Commit != "" && len(Commit) > 7 {
			versionInfo += " (" + Commit[:7] + ")"
		}
	}

	logger.Info("SAGE gateway initialized",
		"version", versionInfo,
		"services", len(services),
		"service_ids", strings.Join(serviceIDs, ", "),
		"port", cfg.Router.Port,
	)

	// pprof server on its own port (config: metrics_config.pprof_addr). Serves
	// http.DefaultServeMux, which carries only the /debug/pprof handlers
	// registered by the blank import above.
	//
	// Off unless configured: /debug/pprof hands out heap dumps and goroutine
	// stacks to anyone who can reach the port, with no authentication. A heap
	// dump contains whatever the process holds in memory, signing keys
	// included, and /debug/pprof/profile will happily block for 30s on request.
	if cfg.Metrics.PprofAddr != "" {
		if !isLoopbackAddr(cfg.Metrics.PprofAddr) {
			logger.Warn("pprof is reachable from outside this host: /debug/pprof exposes heap dumps (which contain signing keys) and has no authentication — bind it to localhost or firewall the port",
				"addr", cfg.Metrics.PprofAddr,
			)
		}
		safego.Go(logger, "server.pprof", func() {
			logger.Info("pprof listening", "addr", cfg.Metrics.PprofAddr)
			if err := http.ListenAndServe(cfg.Metrics.PprofAddr, nil); err != nil {
				logger.Warn("pprof server stopped", "error", err)
			}
		})
	}

	// Prometheus metrics on their own listener (config: metrics_config.
	// prometheus_addr). Until now these were recorded on the hot path and never
	// exposed: nothing mounted the handler, so prometheus_addr was dead config.
	//
	// Its own mux, deliberately NOT http.DefaultServeMux — that one carries the
	// /debug/pprof handlers from the blank import above, so serving metrics off
	// it would publish heap dumps on the metrics port to whoever scrapes it.
	// Metrics are meant to be reachable from outside the pod; pprof is not.
	if cfg.Metrics.PrometheusAddr != "" && app.Metrics != nil {
		safego.Go(logger, "server.metrics", func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", app.Metrics)
			logger.Info("metrics listening", "addr", cfg.Metrics.PrometheusAddr, "path", "/metrics")
			if err := http.ListenAndServe(cfg.Metrics.PrometheusAddr, mux); err != nil {
				logger.Warn("metrics server stopped", "error", err)
			}
		})
	}

	// Admin API on its own listener (config: admin_config.addr, default
	// localhost:9091). It used to be mounted on the relay mux, which published an
	// unauthenticated control plane on the relay port: PUT /admin/flags/{flag}
	// takes any flag name, and shadow_mode alone stops the gateway serving
	// anything. Only network topology was keeping that off the internet, which is
	// exactly the protection that disappears if SAGE is ever moved to the edge.
	//
	// Its own mux, deliberately NOT http.DefaultServeMux — that one carries the
	// /debug/pprof handlers from the blank import above.
	if cfg.Admin.Addr != "" && app.Admin != nil {
		// config.ValidateAdmin has already refused the one combination that
		// cannot be allowed to start — no token on a non-loopback address — so
		// by here the API is either authenticated or unreachable from off-host.
		adminToken := cfg.Admin.EffectiveAuthToken()
		if adminToken == "" {
			logger.Warn("admin API is unauthenticated and reachable from this host only: anyone with local access can toggle feature flags, reset reputation and clear circuit breakers — set admin_config.auth_token or "+config.EnvAdminToken+" to require a bearer token",
				"addr", cfg.Admin.Addr,
			)
		}
		safego.Go(logger, "server.admin", func() {
			// Two muxes, and the split is the security boundary. Every data
			// route sits behind the bearer check; the dashboard page does not,
			// because a browser cannot attach an Authorization header to a
			// top-level navigation — requiring one for the page would make the
			// UI unreachable precisely when a token is configured. The page
			// itself carries no gateway state; it asks for the token and then
			// calls the authenticated API with it.
			apiMux := http.NewServeMux()
			app.Admin.RegisterRoutes(apiMux)

			mux := http.NewServeMux()
			app.Admin.RegisterUIRoutes(mux)
			mux.Handle("/", router.RequireAuth(adminToken, apiMux))

			logger.Info("admin listening",
				"addr", cfg.Admin.Addr,
				"authenticated", adminToken != "",
				"ui", "http://"+cfg.Admin.Addr+"/admin/ui",
			)
			if err := http.ListenAndServe(cfg.Admin.Addr, mux); err != nil {
				logger.Warn("admin server stopped", "error", err)
			}
		})
	}

	// Start HTTP server (blocking in goroutine)
	errCh := make(chan error, 1)
	go func() {
		// Call, not Go: main selects on errCh, so a recovery that sent nothing
		// would hang the process instead of crashing it.
		errCh <- safego.Call(logger, "server.relay", app.Router.Start)
	}()

	logger.Info("SAGE gateway ready",
		"relay", fmt.Sprintf("http://localhost:%d/v1", cfg.Router.Port),
		"health", fmt.Sprintf("http://localhost:%d/health", cfg.Router.Port),
		"admin", fmt.Sprintf("http://%s/admin/flags", cfg.Admin.Addr),
	)

	// Wait for shutdown signal or server error. SIGHUP is in the same set but
	// is not a shutdown: it re-reads the config file, the same way POST
	// /admin/reload does, and the loop goes back to waiting. Handling it here
	// rather than in its own goroutine keeps signal handling in one place, and
	// keeps a reload from racing the shutdown path.
	// Buffered for four: signal.Notify drops a signal rather than blocking
	// when the channel is full, and a reload takes long enough (a file read,
	// a validation pass, several swaps) that a SIGTERM arriving during one
	// must not be the signal that gets dropped.
	stop := make(chan os.Signal, 4)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

waiting:
	for {
		select {
		case sig := <-stop:
			if sig == syscall.SIGHUP {
				reloadOnSignal(ctx, app, logger)
				continue waiting
			}
			logger.Info("Shutdown signal received", "signal", sig.String())
			break waiting
		case err := <-errCh:
			logger.Error("Server error", "error", err)
			break waiting
		}
	}

	// Graceful shutdown
	logger.Info("Shutting down SAGE...")
	cancel() // stop all background services

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := app.Router.Shutdown(shutdownCtx); err != nil {
		logger.Error("Router shutdown error", "error", err)
	}

	if app.Protocol != nil {
		app.Protocol.StopBlockPoller()
	}
	if app.Leader != nil {
		_ = app.Leader.Stop()
	}
	if app.HealthExe != nil {
		app.HealthExe.Stop()
	}
	if app.ObsQueue != nil {
		app.ObsQueue.Stop()
	}
	if app.Redis != nil {
		app.Redis.Close()
	}

	logger.Info("SAGE exited")
}

// reloadOnSignal performs a SIGHUP reload and says what happened.
//
// It never exits. A config file that has been edited into an invalid state is
// a thing an operator fixes and re-signals; a gateway that killed itself over
// it would turn a typo into an outage, having been serving traffic perfectly
// well a moment earlier.
//
// needs_restart is logged at WARN rather than INFO on purpose: it is the half
// of the reload that did NOT happen, and it is the half an operator is most
// likely to assume did.
func reloadOnSignal(ctx context.Context, app *App, logger *slog.Logger) {
	result, err := app.Reload(ctx)
	switch {
	case errors.Is(err, reload.ErrNoConfigFile):
		logger.Warn("SIGHUP ignored: "+reload.ErrNoConfigFile.Error(),
			"hint", "start with -config <path> for SIGHUP and POST /admin/reload to have a file to re-read")
		return
	case err != nil:
		logger.Error("SIGHUP: config reload refused, nothing changed", "error", err)
		return
	}

	logger.Info("SIGHUP: config reloaded", "applied", result.Applied)
	if len(result.NeedsRestart) > 0 {
		logger.Warn("config sections changed that a running gateway cannot apply — they are NOT in effect until a restart",
			"needs_restart", result.NeedsRestart)
	}
	for _, warning := range result.Warnings {
		logger.Warn("config reload: " + warning)
	}
	for _, f := range result.Ignored {
		logger.Warn("config key ignored: SAGE does not implement this setting, and it has no effect", "detail", f)
	}
	for _, f := range result.Inert {
		logger.Warn("config key has no effect: SAGE parses this setting but nothing reads it", "detail", f)
	}
}

func loadConfig(path string) (*config.Config, error) {
	if path != "" {
		return config.LoadFromFile(path)
	}
	// Try environment variable
	cfg, err := config.LoadFromEnv()
	if err == nil {
		return cfg, nil
	}
	return nil, fmt.Errorf("no config: provide -config flag or GATEWAY_CONFIG env var")
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// isLoopbackAddr reports whether a listen address reaches only this host.
//
// A bare port (":6060") binds every interface, so an empty host counts as
// exposed, not as loopback — that is the case worth warning about, and the one
// that looks harmless in a config file.
func isLoopbackAddr(addr string) bool {
	return config.IsLoopbackAddr(addr)
}
