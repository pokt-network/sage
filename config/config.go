// Package config loads and validates SAGE configuration from YAML files.
// It parses the same config.yaml format used by PATH, but uses value types
// internally (no pointer fields, no nil checks).
package config

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// Config is the root configuration for SAGE.
type Config struct {
	Redis       RedisConfig       `yaml:"redis_config"`
	Router      RouterConfig      `yaml:"router_config"`
	Logger      LoggerConfig      `yaml:"logger_config"`
	Metrics     MetricsConfig     `yaml:"metrics_config"`
	Admin       AdminConfig       `yaml:"admin_config"`
	Concurrency ConcurrencyConfig `yaml:"concurrency_config"`
	FullNode    FullNodeConfig    `yaml:"full_node_config"`
	Gateway     GatewayConfig     `yaml:"gateway_config"`
	// FeatureFlags carries only the flags an operator set. Anything absent
	// falls back to featureflag.DefaultFlags, so this is an override map and
	// not a list of the flags that exist. Admin API changes take precedence
	// over it at runtime.
	FeatureFlags FeatureFlags    `yaml:"feature_flags"`
	WebSocket    WebSocketConfig `yaml:"websocket_config"`
	Protocol     ProtocolConfig  `yaml:"protocol"`

	// Ignored lists config keys that were present in the YAML and that SAGE
	// does not implement. They are not an error — SAGE is a restructured fork
	// of PATH and is meant to load a PATH config unmodified, which means
	// tolerating keys that describe features it does not have.
	//
	// They must not be silent either. A key like active_health_checks.local
	// parses, does nothing, and leaves the operator believing their rules are
	// live. Callers are expected to log these at startup; see cmd/sagegw.
	//
	// Not a YAML field: it describes the parse, not the configuration.
	Ignored []string `yaml:"-"`

	// Inert lists config keys that were present in the YAML, parsed into a
	// field, and are read by nothing. Ignored covers the loud half of PATH
	// compatibility — a key with no Go field; this is the quiet half, where a
	// setting survives the round trip and appears in GET /admin/config while
	// deciding nothing. See inert.go.
	//
	// Also not a YAML field, and reported at startup the same way.
	Inert []string `yaml:"-"`

	// Unimplemented lists keys with no Go field at all where the bare
	// "unknown key" line would leave an operator guessing. Ignored already
	// names every such key; these are the few where the true answer needs a
	// second sentence saying what governs the behaviour instead, because the
	// key looks like it decides something somebody is actively reasoning
	// about. See unimplementedFields in inert.go.
	//
	// Not a YAML field. Reported at startup alongside Ignored and Inert.
	Unimplemented []string `yaml:"-"`

	// Warnings lists settings that load, are read, and are probably not what
	// the operator meant — as operator-facing sentences saying what the
	// gateway will actually do with them.
	//
	// The distinction from a validation error is deliberate. SAGE must load a
	// PATH config unmodified, and a PATH config in production today can hold a
	// reputation block whose tier thresholds do not descend; PATH accepts it
	// and the selector copes (it classifies probation first, so a band simply
	// ends up empty). Refusing to boot on it would break the compatibility
	// promise over something that works. Saying nothing would leave an
	// operator believing in a tier that never gets an endpoint. So: it loads,
	// and it says so.
	//
	// Also not a YAML field, and reported at startup the same way.
	Warnings []string `yaml:"-"`
}

// Protocol backend types.
const (
	ProtocolTypeShannon = "shannon"
	ProtocolTypeMock    = "mock"
)

// ProtocolConfig selects the relay backend.
type ProtocolConfig struct {
	// Type is "shannon" (default; empty means shannon) or "mock". Mock serves
	// canned responses in-process — for load testing and benchmarks only.
	Type string             `yaml:"type"`
	Mock MockProtocolConfig `yaml:"mock"`

	// GRPCMode selects the framing for gRPC relays: "native" (HTTP/2), "web"
	// (gRPC-Web over HTTP/1.1), or empty for auto.
	//
	// Auto is the zero value because neither fixed choice is right everywhere:
	// SAGE next to the relay miners can speak native gRPC, while SAGE behind an
	// ingress that terminates HTTP/2 and forwards HTTP/1.1 cannot — the miner
	// answers such a call with "gRPC requires HTTP/2". Auto tries native once
	// per supplier host and remembers the answer.
	GRPCMode string `yaml:"grpc_mode"`
}

// MockProtocolConfig tunes the mock backend.
type MockProtocolConfig struct {
	// EndpointCount is the number of synthetic endpoints advertised per
	// service. Default: 10.
	EndpointCount int `yaml:"endpoint_count"`
	// Latency is the simulated supplier response time per relay. Default: 0.
	Latency time.Duration `yaml:"latency"`
	// ResponseBody overrides the canned JSON-RPC response when non-empty.
	ResponseBody string `yaml:"response_body"`
	// FailureRates injects a chronic fault: FailureRates[i] is the probability
	// that endpoint i answers a relay with an HTTP 200 and an empty body — the
	// mainnet empty-response defect, which the heuristic grades critical and
	// supplier-attributed. Endpoints past the end of the list never fail.
	// Each value must be in [0, 1]. Exists to exercise the chronic-failure rate
	// term (docs/scoring.md §7.3) without a network. Default: none.
	FailureRates []float64 `yaml:"failure_rates"`
}

// WebSocketConfig controls WS-specific behavior.
// Zero values are sensible defaults (see DefaultWebSocketConfig).
type WebSocketConfig struct {
	// FrameObservationSampleRate is the fraction of routine WS frames submitted
	// to the observation pipeline. Frames that trip heuristic analysis are
	// always submitted regardless. Default: 0.01 (1%).
	FrameObservationSampleRate float64 `yaml:"frame_observation_sample_rate"`
	// CloseObservationSampleRate is the fraction of bridge-close events
	// submitted to the observation pipeline. Default: 1.0 (always).
	CloseObservationSampleRate float64 `yaml:"close_observation_sample_rate"`
	// MaxConcurrentConnections caps live WebSocket connections held open by this
	// gateway. Default: 10000. A negative value disables the limit.
	//
	// Zero means "use the default" rather than "no limit": an unbounded count of
	// long-lived connections is the failure this exists to prevent, so it must
	// not be what you get by saying nothing. Disabling is available, but has to
	// be asked for.
	MaxConcurrentConnections int `yaml:"max_concurrent_connections"`
}

// RedisConfig configures the Redis connection.
//
// Redis is optional everywhere in SAGE. With it, reputation scores, feature
// flags and circuit-breaker state are shared across gateway instances; without
// it each instance keeps its own, which is a smaller pool of experience but a
// working gateway. Nothing on the relay path may hard-require it.
type RedisConfig struct {
	// Address is the Redis host:port. **Empty disables Redis entirely** — the
	// gateway runs local-only rather than failing to start.
	Address string `yaml:"address"`
	// Password authenticates to Redis. Empty means no AUTH.
	Password string `yaml:"password"`
	// DB is the Redis logical database number. Default: 0.
	DB int `yaml:"db"`
	// PoolSize caps pooled connections to Redis. Default: 10.
	PoolSize int `yaml:"pool_size"`
	// DialTimeout bounds establishing a Redis connection. Default: 5s.
	DialTimeout time.Duration `yaml:"dial_timeout"`
	// ReadTimeout bounds a single Redis read. Zero takes the client default.
	ReadTimeout time.Duration `yaml:"read_timeout"`
	// WriteTimeout bounds a single Redis write. Zero takes the client default.
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

// RouterConfig controls the HTTP server.
type RouterConfig struct {
	// Port is the public relay listener, serving /v1 plus the health and
	// readiness endpoints. Default: 3069. The admin API and Prometheus listen
	// elsewhere on purpose — see AdminConfig and MetricsConfig.
	Port int `yaml:"port"`
	// ReadTimeout bounds reading a request. Default: 30s.
	ReadTimeout time.Duration `yaml:"read_timeout"`
	// WriteTimeout bounds writing a response. Default: 30s.
	//
	// It does not apply to an upgraded WebSocket connection, which is
	// long-lived by definition; WS deadlines live in the websockets package.
	WriteTimeout time.Duration `yaml:"write_timeout"`
	// IdleTimeout bounds how long a keep-alive connection may sit unused.
	// Default: 120s.
	IdleTimeout time.Duration `yaml:"idle_timeout"`
	// WebsocketMessageBufferSize is parsed and not implemented. WebSocket
	// buffering is fixed in the websockets package; the field exists so a PATH
	// config carrying the key still loads.
	WebsocketMessageBufferSize int `yaml:"websocket_message_buffer_size"`

	// TrustedProxies lists the CIDR ranges of proxies in front of the gateway
	// (haproxy, a load balancer, a CDN). X-Forwarded-For is believed only when a
	// request's immediate peer is inside one of these ranges; from any other peer
	// it is client-supplied and ignored when resolving ctx.ClientIP.
	//
	// Empty means trust no proxy, so the client is always the direct peer. That
	// is the un-spoofable default, and the safe zero value: the dangerous
	// mistake is trusting a proxy that is not actually there, which lets a client
	// forge its address — never the reverse. Set this to the addresses SAGE
	// actually sits behind, not to 0.0.0.0/0.
	TrustedProxies []string `yaml:"trusted_proxies"`
}

// LoggerConfig controls logging.
type LoggerConfig struct {
	// Level is the minimum level logged: "debug", "info", "warn" or "error".
	// Default: "info".
	Level string `yaml:"level"`
}

// MetricsConfig controls the Prometheus and pprof listeners.
type MetricsConfig struct {
	// PrometheusAddr is where /metrics is served. Default: ":9090". Its own
	// listener, so scrape access never implies relay or admin access.
	PrometheusAddr string `yaml:"prometheus_addr"`
	// PprofAddr is where /debug/pprof is served. **Empty means off, and is the
	// default.**
	//
	// Deliberately not defaulted to an address. A heap dump contains whatever
	// is in memory, and this process holds gateway and application signing
	// keys — so an unconfigured gateway must not be serving one. Turning it on
	// is a thing an operator types on purpose, ideally bound to loopback.
	PprofAddr string `yaml:"pprof_addr"`
}

// DefaultAdminAddr is where the admin API listens when admin_config.addr is
// unset: loopback, so it is reachable from this host and nowhere else.
//
// The admin API has no authentication. Anyone who can reach it can flip feature
// flags — enabling shadow_mode alone stops the gateway serving anything — reset
// reputation, and clear circuit breakers. It used to share the relay port, which
// meant "wherever relays are reachable, so is that", and only network topology
// was keeping it off the internet.
//
// Loopback is therefore the default rather than a bare port: the unconfigured
// state has to be the safe one, and exposing it must be a thing an operator
// typed on purpose.
const DefaultAdminAddr = "localhost:9091"

// EnvAdminToken carries the admin API bearer token, and takes precedence over
// admin_config.auth_token.
//
// The env var exists so the token never has to be in the YAML at all: a config
// file is the artifact most likely to be committed, templated, or shipped in an
// image, and a shared secret that lives there leaks by copy rather than by
// attack. The config field remains for setups that keep the whole config in a
// secret store already.
const EnvAdminToken = "SAGE_ADMIN_TOKEN"

// MinAdminTokenLength is the shortest admin token SAGE will accept.
//
// A bearer token guards flag flips that can stop the gateway serving anything,
// so a token short enough to guess is worse than none: it reads as protection.
// 32 characters is what `openssl rand -hex 16` produces, which is the command
// an operator is most likely to reach for; 16 is the floor, leaving room for a
// passphrase somebody actually typed.
const MinAdminTokenLength = 16

// AdminConfig controls the admin API listener.
type AdminConfig struct {
	// Addr is the listen address for the admin API. Empty takes
	// DefaultAdminAddr. Binding anywhere non-loopback without an auth token is
	// refused at startup.
	Addr string `yaml:"addr"`

	// AuthToken is the bearer token the admin API requires, compared against
	// the Authorization header. Empty means no authentication, which is only
	// allowed while the API is bound to loopback. EnvAdminToken overrides it.
	AuthToken string `yaml:"auth_token"`

	// MaxDrain caps how long an operator drain (POST
	// /admin/reputation/drain/{serviceID}) may run for. A request naming a
	// longer duration is refused, not clamped, so an operator who typed 72h
	// learns the ceiling instead of silently getting a day. Zero takes
	// EffectiveMaxDrain's default of 24h.
	MaxDrain time.Duration `yaml:"max_drain"`
}

// EffectiveMaxDrain returns the ceiling on an operator drain's duration. Zero
// takes DefaultMaxDrain (24h) rather than meaning "unbounded" — an unbounded
// drain is exactly the kind of admin mistake the ceiling exists to catch.
func (a AdminConfig) EffectiveMaxDrain() time.Duration {
	if a.MaxDrain <= 0 {
		return DefaultMaxDrain
	}
	return a.MaxDrain
}

// DefaultMaxDrain is the ceiling EffectiveMaxDrain returns when
// admin_config.max_drain is unset.
const DefaultMaxDrain = 24 * time.Hour

// IsLoopbackAddr reports whether a listen address reaches only this host.
//
// An address with no host (":9091") is NOT loopback: it binds every interface,
// which is the case most likely to be typed by accident.
func IsLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// ValidateAdmin enforces the one coupling between the admin listener and its
// credential: the admin API may be unauthenticated, or reachable from off-host,
// but not both. It used to only warn, which meant the unsafe combination
// started fine and said so in a log line nobody reads twice.
func ValidateAdmin(cfg AdminConfig) error {
	token := cfg.EffectiveAuthToken()
	if token != "" && len(token) < MinAdminTokenLength {
		return fmt.Errorf(
			"admin token is %d characters, minimum is %d: a guessable token on a control plane that can stop the gateway serving is worse than none, because it reads as protection",
			len(token), MinAdminTokenLength)
	}
	if token == "" && cfg.Addr != "" && !IsLoopbackAddr(cfg.Addr) {
		return fmt.Errorf(
			"admin_config.addr is %q, which is reachable from outside this host, and no admin token is set: set admin_config.auth_token or %s (openssl rand -hex 16), or bind the admin API to localhost",
			cfg.Addr, EnvAdminToken)
	}
	return nil
}

// EffectiveAuthToken returns the admin bearer token, preferring EnvAdminToken
// over the config field. An empty result means the admin API is unauthenticated
// — valid on loopback, refused anywhere else (see cmd/sagegw).
func (a AdminConfig) EffectiveAuthToken() string {
	if env := strings.TrimSpace(os.Getenv(EnvAdminToken)); env != "" {
		return env
	}
	return strings.TrimSpace(a.AuthToken)
}

// ConcurrencyConfig controls parallel processing limits.
type ConcurrencyConfig struct {
	// MaxConcurrentRelays is a global ceiling on relay goroutines in flight
	// across all batch requests. Default: 10000.
	MaxConcurrentRelays int `yaml:"max_concurrent_relays"`
	// MaxBatchPayloads caps payloads in one batch request. Must be <=
	// MaxConcurrentRelays. Default: 5500.
	MaxBatchPayloads int `yaml:"max_batch_payloads"`

	// NOTE: PATH's max_parallel_endpoints has no field here on purpose. It means
	// "how many endpoints to query in parallel per request", and SAGE has no
	// such feature — it races endpoints via the Hedge middleware, configured
	// per-service by hedge_delay, not by a count. A PATH config carrying the key
	// is reported at startup through Config.Ignored rather than silently
	// accepted. It was previously wired into the batch fan-out, which is not
	// what it means and left batches running one payload at a time.
}

// FullNodeConfig configures the Shannon blockchain full node connection.
type FullNodeConfig struct {
	// RPCURL is the CometBFT RPC endpoint of the Shannon full node, used for
	// block height and chain queries.
	RPCURL     string     `yaml:"rpc_url"`
	GRPCConfig GRPCConfig `yaml:"grpc_config"`
	// LazyMode is parsed and not implemented. In PATH it selects between
	// caching and per-request session lookups.
	LazyMode bool `yaml:"lazy_mode"`
	// SessionRolloverBlocks is parsed and not implemented. SAGE handles session
	// rollover in protocol/shannon rather than from a configured block count.
	SessionRolloverBlocks int         `yaml:"session_rollover_blocks"`
	CacheConfig           CacheConfig `yaml:"cache_config"`
}

// GRPCConfig configures the gRPC connection to the full node.
//
// This is the connection SAGE uses to read chain state — sessions, apps,
// suppliers. It is not the transport for relays to suppliers; see
// ProtocolConfig.GRPCMode for that.
type GRPCConfig struct {
	// HostPort is the full node's gRPC endpoint, as host:port.
	HostPort string `yaml:"host_port"`
	// Insecure disables TLS on the full node gRPC connection. Appropriate for
	// a node on localhost or a trusted network, not across the internet.
	Insecure bool `yaml:"insecure"`
}

// CacheConfig controls session caching.
type CacheConfig struct {
	// SessionTTL is parsed and not implemented. Session lifetime in SAGE
	// follows the protocol's own session boundaries rather than a wall-clock
	// TTL, so there is nothing for this to tune.
	SessionTTL time.Duration `yaml:"session_ttl"`
}

// FeatureFlags is the initial feature-flag state from config: a partial map of
// flag name to enabled, carrying only what an operator set. Unset flags fall
// back to featureflag.DefaultFlags, which is the single place a flag is defined
// — config does not enumerate the known flags, so adding one never touches here.
//
// Runtime overrides via the admin API take precedence and persist in Redis.
//
// A map, not a struct, on purpose. A struct would be a second list of every flag
// to keep in sync with featureflag.DefaultFlags, and — because config value
// types treat the zero value as "unset" only when the WHOLE struct is zero —
// setting one flag in YAML would drop every other flag to false instead of its
// default. The map has neither problem: unknown keys are reported at startup
// (see cmd/sagegw), and an unset flag is simply absent, so its default stands.
type FeatureFlags map[string]bool

// DefaultMaxConcurrentWSConnections caps live WebSocket connections per
// gateway.
//
// Each connection costs a handful of goroutines, two sockets and buffers —
// call it 50-100KB — so 10000 is roughly 0.5-1GB in the worst case. It is
// deliberately far above any normal load: this is a backstop against runaway,
// not a throttle, and a ceiling that trips during ordinary traffic would be
// worse than none at all. Tune it down to observed concurrent load if you want
// it to mean more than "something has gone very wrong".
const DefaultMaxConcurrentWSConnections = 10000

// DefaultWebSocketConfig returns WebSocketConfig with sensible defaults.
func DefaultWebSocketConfig() WebSocketConfig {
	return WebSocketConfig{
		FrameObservationSampleRate: DefaultFrameObservationSampleRate,
		CloseObservationSampleRate: DefaultCloseObservationSampleRate,
		MaxConcurrentConnections:   DefaultMaxConcurrentWSConnections,
	}
}

// EffectiveMaxConcurrentConnections resolves the configured cap: zero takes the
// default, and a negative value disables the limit.
//
// A method rather than a field rewrite because the WebSocketConfig defaults are
// applied all-or-nothing (see cmd/sagegw.Build): setting any single field in
// YAML skips the whole default block. Resolving here means a config that sets
// only, say, a sample rate still gets the connection cap.
func (c WebSocketConfig) EffectiveMaxConcurrentConnections() int {
	if c.MaxConcurrentConnections == 0 {
		return DefaultMaxConcurrentWSConnections
	}
	return c.MaxConcurrentConnections
}

// Default sample rates for the WebSocket observation pipeline.
const (
	// DefaultFrameObservationSampleRate keeps routine frame observation at 1%.
	// Frames that trip heuristic analysis are submitted regardless, so this
	// governs how much *ordinary* traffic is sampled, and a busy subscription
	// produces a lot of it.
	DefaultFrameObservationSampleRate = 0.01
	// DefaultCloseObservationSampleRate is 1.0: a bridge closing is rare and
	// each one carries a reason, so there is nothing to gain by sampling.
	DefaultCloseObservationSampleRate = 1.0
)

// EffectiveFrameObservationSampleRate resolves the routine-frame sample rate.
//
// Zero takes the default rather than meaning "never sample", because the
// defaults are applied all-or-nothing: setting any single WebSocket field in
// YAML skips the whole default block (see cmd/sagegw.Build), so a config that
// tunes only the connection cap would otherwise silently drop frame sampling to
// zero and take WebSocket observability with it. Resolving per field means each
// setting stands on its own.
//
// A deployment that genuinely wants no sampling sets a negative value.
func (c WebSocketConfig) EffectiveFrameObservationSampleRate() float64 {
	if c.FrameObservationSampleRate == 0 {
		return DefaultFrameObservationSampleRate
	}
	if c.FrameObservationSampleRate < 0 {
		return 0
	}
	return c.FrameObservationSampleRate
}

// EffectiveCloseObservationSampleRate resolves the bridge-close sample rate.
// Zero takes the default; a negative value disables sampling. See
// EffectiveFrameObservationSampleRate for why zero cannot mean "off".
func (c WebSocketConfig) EffectiveCloseObservationSampleRate() float64 {
	if c.CloseObservationSampleRate == 0 {
		return DefaultCloseObservationSampleRate
	}
	if c.CloseObservationSampleRate < 0 {
		return 0
	}
	return c.CloseObservationSampleRate
}
