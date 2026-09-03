package config

import (
	"fmt"
	"sort"
	"strings"
)

// inertField names a config key SAGE parses into a struct field and then does
// not act on.
//
// Config.Ignored already catches the loud half of PATH compatibility: a key
// with no Go field at all. This is the quiet half, and it is the worse one. A
// key that parses looks accepted — it survives a round trip, it appears in
// GET /admin/config, and nothing anywhere says it does nothing. An operator
// tuning latency_profiles on SAGE today is editing a struct no code reads.
//
// The rule this enforces: SAGE may decline to implement any PATH setting, but
// it may not quietly appear to implement one.
type inertField struct {
	// Parent is the yaml key of the enclosing mapping. Empty matches any
	// parent, which is what block-level entries want; naming a parent is how
	// retry_config.connect_timeout is reported wherever retry_config appears
	// (gateway, per-service, defaults) without hand-listing the paths.
	Parent string
	// Key is the yaml key itself.
	Key string
	// Reason says what actually decides the behaviour instead. It is written
	// for the operator reading it in a startup log, not for us.
	Reason string
}

// inertFields is the registry. A field whose doc comment says it is parsed and
// not implemented belongs here; TestInertRegistryCoversDocComments fails if one
// is missing, so the two cannot drift.
//
// A block-level entry (latency_profiles, defaults.reputation_config) covers
// every key inside it: where one reason is true of the whole block, reporting
// it once is more useful than reporting eight leaves that share it. The
// gateway-level reputation-tuning blocks are the other case — most of
// signal_impacts and tiered_selection is honoured there, so only the leaves
// SAGE still ignores are listed, one reason each.
//
// Both shapes are needed for the same key: the same tier thresholds are read
// under gateway_config.reputation_config and read by nothing under
// gateway_config.defaults.reputation_config, and the path is what separates
// them.
var inertFields = []inertField{
	{Parent: "reputation_config", Key: "enabled",
		Reason: "reputation always runs; endpoint scoring is how selection works"},
	{Parent: "reputation_config", Key: "storage_type",
		Reason: "storage follows redis_config: Redis when an address is set, in-memory otherwise"},
	{Parent: "reputation_config", Key: "recovery_timeout",
		Reason: "SAGE has no cooldown; recovery happens through probation traffic, not a timer"},

	{Parent: "defaults", Key: "reputation_config",
		Reason: "reputation is configured from gateway_config.reputation_config only; the selector and the scorer are global, so a copy under defaults is read by nothing"},

	{Parent: "tiered_selection", Key: "enabled",
		Reason: "tiering always runs"},
	{Parent: "probation", Key: "enabled",
		Reason: "probation routing always runs"},
	{Parent: "probation", Key: "recovery_multiplier",
		Reason: "SAGE has no recovery multiplier; a probation endpoint recovers through the traffic it is given"},
	{Parent: "signal_impacts", Key: "recovery_success",
		Reason: "the signal type was removed; success is success (docs/scoring.md §7.2)"},
	{Parent: "signal_impacts", Key: "slow_response",
		Reason: "latency does not penalise; it is reported per key in the admin listing (docs/scoring.md §7.2)"},
	{Parent: "signal_impacts", Key: "very_slow_response",
		Reason: "latency does not penalise; it is reported per key in the admin listing (docs/scoring.md §7.2)"},

	{Key: "latency_profiles",
		Reason: "latency has reporting power only — docs/scoring.md §7.2"},
	{Parent: "", Key: "latency_profile",
		Reason: "names an entry in latency_profiles, which nothing reads"},

	{Parent: "retry_config", Key: "max_retry_latency",
		Reason: "bound total retry time with retry_config.max_latency instead"},
	{Parent: "retry_config", Key: "retry_on_timeout",
		Reason: "retryability is decided by the error's own classification, not per-cause switches"},
	{Parent: "retry_config", Key: "retry_on_connection",
		Reason: "retryability is decided by the error's own classification, not per-cause switches"},
	{Parent: "retry_config", Key: "connect_timeout",
		Reason: "the protocol's HTTP client bounds connection setup"},

	{Parent: "active_health_checks", Key: "grace_period",
		Reason: "block-consensus tolerance covers the same window"},

	{Parent: "router_config", Key: "websocket_message_buffer_size",
		Reason: "WebSocket buffering is fixed in the websockets package"},

	{Parent: "full_node_config", Key: "lazy_mode",
		Reason: "SAGE always caches sessions; there is no per-request lookup mode to select"},
	{Parent: "full_node_config", Key: "session_rollover_blocks",
		Reason: "rollover is handled in protocol/shannon from the session's own boundaries"},
	{Parent: "cache_config", Key: "session_ttl",
		Reason: "session lifetime follows the protocol's session boundaries, not a wall-clock TTL"},
}

// InertKeys returns the inert keys actually present in the parsed YAML, as
// operator-facing sentences. Order is stable so a startup log reads the same
// way twice.
//
// tree is the raw YAML decoded into generic maps and slices, which is how the
// keys an operator wrote are distinguished from the fields a struct has: a
// zero-valued field says nothing about whether anyone set it.
func InertKeys(tree any) []string {
	var found []string
	walkRegistry(tree, "", "", inertFields, "is parsed but not implemented", &found)
	sort.Strings(found)
	return found
}

// unimplementedFields names config keys SAGE has NO field for, where "unknown
// key" is a true answer and an unhelpful one.
//
// Config.Ignored already reports every key with no matching field, and for
// most of them that is enough: the key describes a feature SAGE does not have
// and the operator can see the name. These are the ones where the honest
// answer needs a second sentence, because the key looks like it governs
// something an operator is actively reasoning about — and they will reason
// about it wrongly without being told what governs it instead.
//
// The bar for an entry here is that somebody has actually been misled, not
// that a key looks confusing. active_health_checks.external qualifies: on
// 2026-09-03 an operator investigating probe cadence had to ask whether the 69
// rules in the file it points at were setting the health-check tick, because
// nothing in the startup log said they were not being read at all.
var unimplementedFields = []inertField{
	{
		Parent: "active_health_checks",
		Key:    "external",
		Reason: "SAGE does not fetch the remote rule file, so none of its rules or their check_interval values have any effect. " +
			"Probing is governed by active_health_checks.interval, by local[].check_interval per service, " +
			"and by each QoS plugin's own checks. See docs/path-compat.md",
	},
}

// UnimplementedKeys reports the keys in a decoded YAML tree that are
// registered above, each with what governs the behaviour instead.
func UnimplementedKeys(tree any) []string {
	var found []string
	walkRegistry(tree, "", "", unimplementedFields, "is not implemented", &found)
	sort.Strings(found)
	return found
}

// walkRegistry descends the YAML tree, reporting any key in the given
// registry. path is the dotted path to the current node and parent is the key
// of the mapping that holds it — sequence indexes are left out of the parent so
// an entry under services[3] still matches Parent: "retry_config". verb is how
// the finding is phrased, since the two registries describe different failures.
func walkRegistry(node any, path, parent string, registry []inertField, verb string, found *[]string) {
	switch n := node.(type) {
	case map[string]any:
		for key, child := range n {
			childPath := key
			if path != "" {
				childPath = path + "." + key
			}
			if reason, ok := matchField(registry, parent, key); ok {
				*found = append(*found, fmt.Sprintf("%s %s: %s", childPath, verb, reason))
				// Do not descend: a block-level entry has already said what
				// every key inside it would say.
				continue
			}
			walkRegistry(child, childPath, key, registry, verb, found)
		}

	case []any:
		for i, child := range n {
			walkRegistry(child, fmt.Sprintf("%s[%d]", path, i), parent, registry, verb, found)
		}
	}
}

// matchInert reports whether (parent, key) is registered. An entry with an
// empty Parent matches whatever holds it.
func matchInert(parent, key string) (string, bool) {
	return matchField(inertFields, parent, key)
}

// matchField reports whether (parent, key) appears in a registry. An entry with
// an empty Parent matches any.
func matchField(registry []inertField, parent, key string) (string, bool) {
	for _, f := range registry {
		if f.Key != key {
			continue
		}
		if f.Parent == "" || f.Parent == parent {
			return f.Reason, true
		}
	}
	return "", false
}

// InertReason reports whether the yaml key under parent is registered as
// parsed-but-inert, and why. The config reference generator reads it so the
// docs say what the startup log says.
func InertReason(parent, key string) (string, bool) {
	return matchInert(parent, key)
}

// InertRegistryKeys returns every registered key, for tests and for anything
// that needs to know the whole set rather than what one config triggered.
func InertRegistryKeys() []string {
	out := make([]string, 0, len(inertFields))
	for _, f := range inertFields {
		out = append(out, strings.TrimPrefix(f.Parent+"."+f.Key, "."))
	}
	sort.Strings(out)
	return out
}
