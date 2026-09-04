package config

import (
	"fmt"
	"strconv"
	"strings"
)

// applyRetryDisabled honours `retry_config: {enabled: false}` wherever the
// YAML wrote it, and says so.
//
// A value-typed bool cannot tell an absent key from a false one, and PATH's
// rule — absent means on, false means off — needs exactly that distinction.
// So the decision is read from the decoded YAML tree, where presence is a
// fact, and applied to the struct the path names. Any block this does not
// know how to reach is reported rather than skipped: a switch that is
// sometimes honoured is worse than one that never is.
func applyRetryDisabled(cfg *Config, tree any) []string {
	var out []string
	for _, path := range retryDisabledPaths(tree) {
		block := retryBlockAt(cfg, path)
		if block == nil {
			out = append(out, path+".enabled: false could not be applied to that block, and retries there follow max_retries")
			continue
		}
		had := block.MaxRetries
		block.disable()
		if had > 0 {
			out = append(out, fmt.Sprintf("%s.enabled is false, so retries are off there and its max_retries (%d) is not used", path, had))
		}
	}
	return out
}

// retryDisabledPaths lists every retry_config block in the tree that carries
// an explicit `enabled: false`, as dotted paths with list indexes.
func retryDisabledPaths(tree any) []string {
	var out []string
	var walk func(node any, path string)
	walk = func(node any, path string) {
		switch n := node.(type) {
		case map[string]any:
			for key, child := range n {
				childPath := key
				if path != "" {
					childPath = path + "." + key
				}
				if key == "retry_config" {
					if m, ok := child.(map[string]any); ok {
						if v, ok := m["enabled"].(bool); ok && !v {
							out = append(out, childPath)
						}
					}
					continue
				}
				walk(child, childPath)
			}
		case []any:
			for i, child := range n {
				walk(child, fmt.Sprintf("%s[%d]", path, i))
			}
		}
	}
	walk(tree, "")
	return out
}

// retryBlockAt resolves a dotted path to the RetryConfig it names, or nil for
// a shape this does not know. The known shapes are the ones a PATH config can
// carry a retry_config under.
func retryBlockAt(cfg *Config, path string) *RetryConfig {
	g := &cfg.Gateway
	switch path {
	case "gateway_config.retry_config":
		return &g.Retry
	case "gateway_config.defaults.retry_config":
		return &g.Defaults.Retry
	case "gateway_config.unified_services.defaults.retry_config":
		return &g.UnifiedServices.Defaults.Retry
	}
	if i, ok := serviceIndex(path, "gateway_config.services["); ok && i < len(g.Services) {
		return &g.Services[i].Retry
	}
	if i, ok := serviceIndex(path, "gateway_config.unified_services.services["); ok && i < len(g.UnifiedServices.Services) {
		return &g.UnifiedServices.Services[i].Retry
	}
	return nil
}

// serviceIndex parses "<prefix>N].retry_config" into N.
func serviceIndex(path, prefix string) (int, bool) {
	rest, ok := strings.CutPrefix(path, prefix)
	if !ok {
		return 0, false
	}
	idx, ok := strings.CutSuffix(rest, "].retry_config")
	if !ok {
		return 0, false
	}
	i, err := strconv.Atoi(idx)
	if err != nil || i < 0 {
		return 0, false
	}
	return i, true
}

// applyHealthChecksDisabled honours `active_health_checks: {enabled: false}`
// the same way: by the key's presence in the YAML, since PATH's default for
// an absent key is on and a value-typed bool cannot say which it saw.
func applyHealthChecksDisabled(cfg *Config, tree any) []string {
	v, present := boolAt(tree, "gateway_config", "active_health_checks", "enabled")
	if !present || v {
		return nil
	}
	cfg.Gateway.HealthChecks.disabled = true
	return []string{"gateway_config.active_health_checks.enabled is false, so no health-check probes are sent: selection is graded by client traffic alone, and readiness does not wait for a warm-up"}
}

// boolAt reads a bool at a mapping path in the decoded YAML tree, reporting
// whether the key was written at all.
func boolAt(tree any, path ...string) (value, present bool) {
	node := tree
	for _, key := range path {
		m, ok := node.(map[string]any)
		if !ok {
			return false, false
		}
		node, ok = m[key]
		if !ok {
			return false, false
		}
	}
	v, ok := node.(bool)
	return v, ok
}
