package healthcheck

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
)

// ConfiguredChecks holds the health checks an operator declared in YAML,
// indexed by service.
//
// They are additional to the checks a QoS plugin provides, never a replacement.
// The plugin's checks are what keep block height and chain ID tracking fed, so
// a config that switched them off would quietly degrade endpoint selection —
// config adds probes, it does not take them away.
type ConfiguredChecks struct {
	byService map[domain.ServiceID][]qos.HealthCheck
	// intervals is each service's own probe cadence (local[].check_interval),
	// kept whether or not the block's checks are enabled: it is the service's
	// cadence, not a property of the check list.
	intervals map[domain.ServiceID]time.Duration
	// signals maps a check name to the penalty its failure carries, since
	// qos.HealthCheck has nowhere to put one.
	signals map[string]string
}

// BuildConfiguredChecks converts the config blocks into health checks. It
// returns the checks plus the problems worth telling an operator about — a
// malformed check is skipped rather than fatal, because one bad rule should not
// stop a gateway from starting.
func BuildConfiguredChecks(cfg config.HealthCheckConfig) (*ConfiguredChecks, []string) {
	out := &ConfiguredChecks{
		byService: make(map[domain.ServiceID][]qos.HealthCheck),
		intervals: make(map[domain.ServiceID]time.Duration),
		signals:   make(map[string]string),
	}

	var warnings []string
	for _, svc := range cfg.Local {
		if svc.ServiceID == "" {
			warnings = append(warnings, "health check block with no service_id was skipped")
			continue
		}
		if svc.CheckInterval > 0 {
			out.intervals[domain.ServiceID(svc.ServiceID)] = svc.CheckInterval
		}
		if svc.IsDeclaredButOff() {
			warnings = append(warnings, fmt.Sprintf(
				"service %q declares %d health check(s) but is not enabled, so none of them run",
				svc.ServiceID, len(svc.Checks)))
			continue
		}
		if !svc.Enabled {
			continue
		}

		serviceID := domain.ServiceID(svc.ServiceID)
		for _, check := range svc.Checks {
			built, err := buildCheck(svc.ServiceID, check)
			if err != nil {
				warnings = append(warnings, err.Error())
				continue
			}
			out.byService[serviceID] = append(out.byService[serviceID], built)
			if check.ReputationSignal != "" {
				out.signals[built.Name] = check.ReputationSignal
			}
		}
	}

	return out, warnings
}

// buildCheck turns one configured rule into a health check payload.
func buildCheck(serviceID string, check config.HealthCheck) (qos.HealthCheck, error) {
	if check.Name == "" {
		return qos.HealthCheck{}, fmt.Errorf(
			"service %q has a health check with no name, which is skipped", serviceID)
	}

	rpcType, err := parseCheckRPCType(check.Type)
	if err != nil {
		return qos.HealthCheck{}, fmt.Errorf(
			"service %q health check %q: %w, so it is skipped", serviceID, check.Name, err)
	}

	method := strings.ToUpper(check.Method)
	if method == "" {
		method = http.MethodPost
	}

	path := check.Path
	if path == "" {
		path = "/"
	}

	// The name is namespaced so two services can both define "eth_blockNumber"
	// without their reputation signals colliding in the signal map.
	name := serviceID + ":" + check.Name

	payload := domain.NewPayload([]byte(check.Body), rpcType, check.Name).
		WithHTTP(path, method)

	return qos.HealthCheck{Name: name, Payload: payload}, nil
}

// parseCheckRPCType maps the configured type name onto an RPC type. Empty means
// JSON-RPC, which is what nearly every health check is.
func parseCheckRPCType(name string) (domain.RPCType, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "json_rpc", "jsonrpc":
		return domain.RPCTypeJSONRPC, nil
	case "rest":
		return domain.RPCTypeREST, nil
	case "comet_bft", "cometbft":
		return domain.RPCTypeCometBFT, nil
	case "websocket", "grpc":
		// Both need a live connection or an HTTP/2 call rather than a one-shot
		// relay, which is not what the executor does.
		return "", fmt.Errorf("type %q is not supported for health checks", name)
	default:
		return "", fmt.Errorf("unknown type %q", name)
	}
}

// IntervalFor returns the service's own probe cadence, or 0 when it runs at the
// global interval.
func (c *ConfiguredChecks) IntervalFor(serviceID domain.ServiceID) time.Duration {
	if c == nil {
		return 0
	}
	return c.intervals[serviceID]
}

// shortestInterval is the smallest per-service cadence, or 0 when none is set.
func (c *ConfiguredChecks) shortestInterval() time.Duration {
	if c == nil {
		return 0
	}
	var min time.Duration
	for _, d := range c.intervals {
		if d > 0 && (min == 0 || d < min) {
			min = d
		}
	}
	return min
}

// For returns the configured checks for a service, or nil.
func (c *ConfiguredChecks) For(serviceID domain.ServiceID) []qos.HealthCheck {
	if c == nil {
		return nil
	}
	return c.byService[serviceID]
}

// SignalFor grades a failure of the named check according to its configured
// reputation_signal. An unnamed or unknown signal falls back to minor, matching
// the executor's default for a check that simply failed.
func (c *ConfiguredChecks) SignalFor(checkName, reason string, latency time.Duration) (reputation.Signal, bool) {
	if c == nil {
		return reputation.Signal{}, false
	}
	name, ok := c.signals[checkName]
	if !ok {
		return reputation.Signal{}, false
	}
	switch strings.ToLower(name) {
	case "critical_error":
		return reputation.NewCriticalErrorSignal(reason, latency), true
	case "major_error":
		return reputation.NewMajorErrorSignal(reason, latency), true
	case "minor_error":
		return reputation.NewMinorErrorSignal(reason, latency), true
	default:
		return reputation.Signal{}, false
	}
}
