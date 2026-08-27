package main

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/reload"
	"github.com/pokt-network/sage/tuning"
)

// reloadTestYAML is a config Build can satisfy in-process (mock backend: no
// fullnode, no identity, no Redis) that also exercises every seam a reload has
// — per-service retry, a flag override, health checks, blocked domains and
// method-block knobs.
//
// It is a string rather than a fixture file because every test here works by
// editing one line of it and writing it back: the edit is the test.
const reloadTestYAML = `
protocol:
  type: mock
  mock:
    endpoint_count: 3
concurrency_config:
  max_concurrent_relays: 100
  max_batch_payloads: 10
feature_flags:
  tracing: true
gateway_config:
  gateway_mode: centralized
  defaults:
    retry_config:
      max_retries: 2
      hedge_delay: 500ms
    timeout_config:
      relay_timeout: 10s
  method_blocks:
    ttl: 1m
    escalation_threshold: 4
  blocked_domains:
    - domain: op-alpha.example
  services:
    - id: eth
      type: evm
      rpc_types:
        - json_rpc
      sync_allowance: 5
      chain_id: "0x1"
`

// writeReloadConfig writes yaml to a temp file and returns its path.
func writeReloadConfig(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// rewriteReloadConfig replaces the file at path with yaml, the way an operator
// editing the config in place does.
func rewriteReloadConfig(t *testing.T, path, yaml string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}
}

// buildReloadApp builds an App from yaml written to a temp file, with
// ConfigPath set the way main sets it.
func buildReloadApp(t *testing.T, yaml string) (*App, string) {
	t.Helper()
	freshRegistry(t)
	path := writeReloadConfig(t, yaml)
	cfg, err := config.LoadFromFile(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	app, err := Build(t.Context(), cfg, testLogger())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	app.ConfigPath = path
	return app, path
}

// TestReload_NoConfigFile: a gateway booted from GATEWAY_CONFIG has inline
// YAML, not a path. Re-reading is not a thing it can do, and saying "that
// failed to validate" would be a lie — the route turns this into a 409.
func TestReload_NoConfigFile(t *testing.T) {
	app, _ := buildReloadApp(t, reloadTestYAML)
	app.ConfigPath = ""

	_, err := app.Reload(t.Context())
	if !errors.Is(err, reload.ErrNoConfigFile) {
		t.Fatalf("err = %v, want reload.ErrNoConfigFile", err)
	}
}

// TestReload_HedgeDelayApplies is the whole reason the config snapshot went
// behind an atomic pointer: a knob the middlewares read per request has to
// come back different after the file changes.
//
// Revert-check: drop the Config.Store from Reload and this fails — the diff
// still says "applied", but the closure keeps returning 500ms.
func TestReload_HedgeDelayApplies(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)
	retryFn := newRetryFn(app.Config.Load, tuning.NewStore())

	if got := retryFn("eth").HedgeDelay; got != 500*time.Millisecond {
		t.Fatalf("hedge delay before reload = %s, want the configured 500ms", got)
	}

	rewriteReloadConfig(t, path, strings.Replace(reloadTestYAML, "hedge_delay: 500ms", "hedge_delay: 25ms", 1))

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !slices.Contains(res.Applied, "gateway_config.defaults.retry_config") {
		t.Errorf("applied = %v, want the key path that was edited", res.Applied)
	}
	if len(res.NeedsRestart) != 0 {
		t.Errorf("needs_restart = %v, want nothing — only a tunable knob changed", res.NeedsRestart)
	}
	if got := retryFn("eth").HedgeDelay; got != 25*time.Millisecond {
		t.Fatalf("hedge delay after reload = %s, want the new 25ms", got)
	}
}

// TestReload_PerServiceRetryApplies: the closures resolve a service's own
// retry_config too, so editing one service's knob is applied, not a restart.
func TestReload_PerServiceRetryApplies(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)
	retryFn := newRetryFn(app.Config.Load, tuning.NewStore())

	rewriteReloadConfig(t, path, strings.Replace(reloadTestYAML,
		"      sync_allowance: 5",
		"      sync_allowance: 5\n      retry_config:\n        max_retries: 9", 1))

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !slices.Contains(res.Applied, "gateway_config.services[eth].retry_config") {
		t.Errorf("applied = %v, want the service's own key path", res.Applied)
	}
	if len(res.NeedsRestart) != 0 {
		t.Errorf("needs_restart = %v, want nothing", res.NeedsRestart)
	}
	if got := retryFn("eth").MaxRetries; got != 9 {
		t.Fatalf("eth max_retries = %d, want the new 9", got)
	}
}

// TestReload_ChainIDNeedsRestart: chain_id is compiled into the QoS plugin at
// wire time. There is no seam, and a reload that quietly swapped the snapshot
// would leave an operator believing the new assertion was live.
func TestReload_ChainIDNeedsRestart(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)

	rewriteReloadConfig(t, path, strings.Replace(reloadTestYAML, `chain_id: "0x1"`, `chain_id: "0x89"`, 1))

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !slices.Contains(res.NeedsRestart, "gateway_config.services[eth].chain_id") {
		t.Fatalf("needs_restart = %v, want gateway_config.services[eth].chain_id", res.NeedsRestart)
	}
	if len(res.Applied) != 0 {
		t.Errorf("applied = %v, want nothing — chain_id has no seam", res.Applied)
	}
}

// TestReload_AddedServiceNeedsRestart: the service list is a rebuild, not a
// reload — QoS plugins, metric label sets and the validate middleware are all
// built from it once.
func TestReload_AddedServiceNeedsRestart(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)

	rewriteReloadConfig(t, path, reloadTestYAML+`    - id: poly
      type: evm
      rpc_types:
        - json_rpc
`)

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !slices.Contains(res.NeedsRestart, "gateway_config.services") {
		t.Fatalf("needs_restart = %v, want gateway_config.services", res.NeedsRestart)
	}
}

// TestReload_InvalidFileChangesNothing: a file that would not boot is refused
// whole. Half-applying a config is worse than not applying one, because
// nothing afterwards says which half.
func TestReload_InvalidFileChangesNothing(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)
	before := app.Config.Load()

	// A tab is never valid YAML indentation, so this fails in the decoder
	// rather than in validation — the earliest way a file can be wrong.
	rewriteReloadConfig(t, path, "gateway_config:\n\tservices: []\n")

	if _, err := app.Reload(t.Context()); err == nil {
		t.Fatal("Reload accepted YAML that does not parse")
	}
	if app.Config.Load() != before {
		t.Error("the config snapshot changed even though the reload failed")
	}
}

// TestReload_InvalidBlockedDomainChangesNothing: the file parses, so only the
// validation boot runs catches it. That validation has to run on reload too,
// or the runtime accepts a config the binary would refuse to start with.
func TestReload_InvalidBlockedDomainChangesNothing(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)
	before := app.Config.Load()

	rewriteReloadConfig(t, path, strings.Replace(reloadTestYAML,
		"    - domain: op-alpha.example",
		"    - domain: op-alpha.example\n      rpc_types:\n        - not_a_real_rpc_type", 1))

	_, err := app.Reload(t.Context())
	if err == nil {
		t.Fatal("Reload accepted a blocked_domains entry boot would refuse")
	}
	if app.Config.Load() != before {
		t.Error("the config snapshot changed even though the reload failed")
	}
}

// TestReload_RemovedFlagOverrideReturnsToDefault: a flag dropped from the file
// has to go back to featureflag.DefaultFlags, which means Delete and not
// merely re-Setting whatever is still written down.
//
// Revert-check: skip the Delete pass and this fails — tracing stays true
// forever, and removing a line from the config does nothing.
func TestReload_RemovedFlagOverrideReturnsToDefault(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)

	if !app.Flags.IsEnabled(t.Context(), featureflag.FlagTracing, "eth") {
		t.Fatal("tracing should start enabled — the config file overrides it to true")
	}

	rewriteReloadConfig(t, path, strings.Replace(reloadTestYAML, "feature_flags:\n  tracing: true\n", "", 1))

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !slices.Contains(res.Applied, "feature_flags") {
		t.Errorf("applied = %v, want feature_flags", res.Applied)
	}
	if app.Flags.IsEnabled(t.Context(), featureflag.FlagTracing, "eth") {
		t.Fatal("tracing is still on: the override was re-applied instead of deleted, so DefaultFlags never came back")
	}
}

// TestReload_MethodBlockKnobsApply: the store's own setters are the seam, so
// the knobs land without a restart.
func TestReload_MethodBlockKnobsApply(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)

	rewriteReloadConfig(t, path, strings.Replace(reloadTestYAML, "ttl: 1m", "ttl: 90s", 1))

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !slices.Contains(res.Applied, "gateway_config.method_blocks") {
		t.Errorf("applied = %v, want gateway_config.method_blocks", res.Applied)
	}
}

// TestReload_HealthChecksApplyWithWarnings: the check set is rebuilt and
// handed to the running executor, and a rule that could not be built comes
// back in the response.
//
// The warning is the load-bearing half. A check that silently vanished reads
// to an operator exactly like a check that is passing.
func TestReload_HealthChecksApplyWithWarnings(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)

	rewriteReloadConfig(t, path, reloadTestYAML+`  active_health_checks:
    enabled: true
    local:
      - service_id: eth
        enabled: true
        checks:
          - name: chain_id
            method: eth_chainId
      - service_id: eth-declared-but-off
        checks:
          - name: never_runs
            method: eth_blockNumber
`)

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !slices.Contains(res.Applied, "gateway_config.active_health_checks") {
		t.Errorf("applied = %v, want gateway_config.active_health_checks", res.Applied)
	}
	if len(res.Warnings) == 0 {
		t.Fatal("warnings is empty, but one block declares checks it never runs")
	}
	if !strings.Contains(strings.Join(res.Warnings, "; "), "eth-declared-but-off") {
		t.Errorf("warnings = %v, want the block that declares checks and is not enabled", res.Warnings)
	}
}

// TestReload_UnchangedFileAppliesNothing: a reload of the same file must not
// claim to have done anything. "applied" is a report, not a checklist.
func TestReload_UnchangedFileAppliesNothing(t *testing.T) {
	app, _ := buildReloadApp(t, reloadTestYAML)

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(res.Applied) != 0 || len(res.NeedsRestart) != 0 {
		t.Fatalf("applied = %v, needs_restart = %v, want both empty", res.Applied, res.NeedsRestart)
	}
	// Never null in JSON.
	if res.Applied == nil || res.NeedsRestart == nil || res.Ignored == nil ||
		res.Inert == nil || res.Warnings == nil {
		t.Error("a Result slice came back nil; the JSON would carry null instead of []")
	}
}

// TestReload_ReportsIgnoredAndInert: the reload is the moment an operator is
// looking at the file they just edited, so the parse's own complaints belong
// in the response rather than only in a startup log nobody re-reads.
func TestReload_ReportsIgnoredAndInert(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)

	rewriteReloadConfig(t, path, reloadTestYAML+`  latency_profiles:
    fast:
      fast_threshold: 1s
coordination:
  enabled: true
`)

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(res.Ignored) == 0 {
		t.Error("ignored is empty, but the file carries a key SAGE has no field for")
	}
	if len(res.Inert) == 0 {
		t.Error("inert is empty, but the file carries latency_profiles, which nothing reads")
	}
}

// TestDiffConfig_NewTopLevelFieldNeedsRestart is the fail-safe pin.
//
// The diff walks config.Config by reflection and names, per section, what it
// knows how to apply. Everything else falls through to needs_restart — so a
// top-level field added to config.Config later is reported as needing a
// restart by default, rather than being silently dropped from the reload
// report because nobody remembered to add it to a hand-written list.
//
// The test mutates each top-level field in turn and insists the diff notices.
func TestDiffConfig_NewTopLevelFieldNeedsRestart(t *testing.T) {
	// The two fields with runtime seams are covered by their own tests above,
	// and the two parse-metadata fields describe the parse rather than the
	// configuration. Everything else has to be fail-safe.
	seams := map[string]bool{"Gateway": true, "FeatureFlags": true}
	metadata := map[string]bool{"Ignored": true, "Inert": true}

	typ := reflect.TypeOf(config.Config{})
	for i := range typ.NumField() {
		field := typ.Field(i)
		if seams[field.Name] || metadata[field.Name] {
			continue
		}
		t.Run(field.Name, func(t *testing.T) {
			old := &config.Config{}
			next := &config.Config{}
			mutated := mutateField(reflect.ValueOf(next).Elem().Field(i))
			if !mutated {
				t.Skipf("no scalar leaf to mutate in %s", field.Name)
			}

			d := diffConfig(old, next)
			key := yamlKey(field)
			named := slices.ContainsFunc(d.needsRestart, func(s string) bool {
				return s == key || strings.HasPrefix(s, key+".")
			})
			if !named {
				t.Errorf("changing %s (%s) produced needs_restart %v; a section with no seam must be reported",
					field.Name, key, d.needsRestart)
			}
		})
	}
}

// TestDiffConfig_NewGatewayFieldNeedsRestart pins the same fail-safe one level
// down, where most of the config actually lives.
func TestDiffConfig_NewGatewayFieldNeedsRestart(t *testing.T) {
	seams := map[string]bool{
		"Retry": true, "Defaults": true, "HealthChecks": true,
		"BlockedDomains": true, "MethodBlocks": true,
		"Services": true, "UnifiedServices": true,
	}

	typ := reflect.TypeOf(config.GatewayConfig{})
	for i := range typ.NumField() {
		field := typ.Field(i)
		if seams[field.Name] {
			continue
		}
		t.Run(field.Name, func(t *testing.T) {
			old := &config.Config{}
			next := &config.Config{}
			if !mutateField(reflect.ValueOf(&next.Gateway).Elem().Field(i)) {
				t.Skipf("no scalar leaf to mutate in %s", field.Name)
			}

			d := diffConfig(old, next)
			key := "gateway_config." + yamlKey(field)
			if !slices.Contains(d.needsRestart, key) {
				t.Errorf("changing gateway_config.%s produced needs_restart %v, want %s",
					field.Name, d.needsRestart, key)
			}
		})
	}
}

// mutateField gives v a value different from its zero one, descending into
// structs, slices and maps until it finds a leaf it can set. It reports
// whether it managed to change anything.
func mutateField(v reflect.Value) bool {
	if !v.CanSet() {
		return false
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString("changed")
		return true
	case reflect.Bool:
		v.SetBool(!v.Bool())
		return true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(v.Int() + 7)
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(v.Uint() + 7)
		return true
	case reflect.Float32, reflect.Float64:
		v.SetFloat(v.Float() + 0.5)
		return true
	case reflect.Slice:
		elem := reflect.New(v.Type().Elem()).Elem()
		mutateField(elem)
		v.Set(reflect.Append(v, elem))
		return true
	case reflect.Map:
		key := reflect.New(v.Type().Key()).Elem()
		if !mutateField(key) {
			return false
		}
		val := reflect.New(v.Type().Elem()).Elem()
		mutateField(val)
		v.Set(reflect.MakeMap(v.Type()))
		v.SetMapIndex(key, val)
		return true
	case reflect.Struct:
		for i := range v.NumField() {
			if mutateField(v.Field(i)) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// failingBlocklist stands in for a protocol backend whose blocklist swap goes
// wrong after the file has already validated.
type failingBlocklist struct{ err error }

// SetBlockedDomains always fails.
func (f failingBlocklist) SetBlockedDomains(_ []config.BlockedDomain) error { return f.err }

// TestReload_SeamFailureIsAWarningNotARefusal is the difference between an
// honest report and a lie.
//
// Once the first section has been written the file is partly in effect
// whatever happens next. Returning an error there would make the route answer
// 400 and SIGHUP log "nothing changed" about a gateway that had changed — and
// throw away the only record of which half. A seam that fails becomes a
// warning naming its own key, and the sections after it still run.
func TestReload_SeamFailureIsAWarningNotARefusal(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)
	app.blockedDomains = failingBlocklist{err: errors.New("swap refused")}
	retryFn := newRetryFn(app.Config.Load, tuning.NewStore())

	edited := strings.Replace(reloadTestYAML, "    - domain: op-alpha.example",
		"    - domain: op-beta.example", 1)
	edited = strings.Replace(edited, "hedge_delay: 500ms", "hedge_delay: 25ms", 1)
	edited = strings.Replace(edited, "ttl: 1m", "ttl: 90s", 1)
	rewriteReloadConfig(t, path, edited)

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("a failing seam must not refuse the whole reload: %v", err)
	}

	joined := strings.Join(res.Warnings, "; ")
	if !strings.Contains(joined, "gateway_config.blocked_domains") || !strings.Contains(joined, "swap refused") {
		t.Errorf("warnings = %v, want the failing section named with its error", res.Warnings)
	}
	if slices.Contains(res.Applied, "gateway_config.blocked_domains") {
		t.Error("blocked_domains is listed as applied, but the swap failed")
	}
	// Everything else still happened, and the snapshot was stored.
	if !slices.Contains(res.Applied, "gateway_config.method_blocks") {
		t.Errorf("applied = %v, want the sections after the failure to have run", res.Applied)
	}
	if got := retryFn("eth").HedgeDelay; got != 25*time.Millisecond {
		t.Errorf("hedge delay = %s, want the reload to have stored the snapshot anyway", got)
	}
}

// TestReload_MissingSeamIsAWarning: under the mock backend there is no
// blocklist to swap, and with no method-block store there is nothing to tune.
// Both are "changed and NOT in effect", which is a warning, not silence.
func TestReload_MissingSeamIsAWarning(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)
	app.MethodBlocks = nil // app.blockedDomains is already nil in mock mode

	edited := strings.Replace(reloadTestYAML, "    - domain: op-alpha.example",
		"    - domain: op-beta.example", 1)
	edited = strings.Replace(edited, "ttl: 1m", "ttl: 90s", 1)
	rewriteReloadConfig(t, path, edited)

	res, err := app.Reload(t.Context())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	joined := strings.Join(res.Warnings, "; ")
	for _, key := range []string{"gateway_config.blocked_domains", "gateway_config.method_blocks"} {
		if !strings.Contains(joined, key) {
			t.Errorf("warnings = %v, want %s named as not in effect", res.Warnings, key)
		}
		if slices.Contains(res.Applied, key) {
			t.Errorf("%s is listed as applied, but there is no seam for it", key)
		}
	}
}

// TestReload_ChainWithoutSendRelayIsRefused: send_relay is a pure statement
// about the configured order, so a reload has to refuse the same file boot
// would. The route turns any non-ErrNoConfigFile error into 400.
func TestReload_ChainWithoutSendRelayIsRefused(t *testing.T) {
	app, path := buildReloadApp(t, reloadTestYAML)
	before := app.Config.Load()

	rewriteReloadConfig(t, path, reloadTestYAML+`  middleware_chain:
    - parse
    - select_endpoint
`)

	_, err := app.Reload(t.Context())
	if err == nil {
		t.Fatal("Reload accepted a chain with no send_relay — nothing would ever be relayed")
	}
	if errors.Is(err, reload.ErrNoConfigFile) {
		t.Fatal("the error must not be ErrNoConfigFile, or the route would answer 409 instead of 400")
	}
	if !strings.Contains(err.Error(), "send_relay") {
		t.Errorf("err = %v, want it to name the missing send_relay", err)
	}
	if app.Config.Load() != before {
		t.Error("the config snapshot changed even though the reload was refused")
	}
}

// TestDiffConfig_DefaultsDescendLeafWise: gateway_config.defaults is not one
// thing. retry_config and timeout_config are resolved per request; the
// reputation_config sitting next to them is read by nothing at all, so
// reporting the whole block as applied would be a claim about a dead field.
func TestDiffConfig_DefaultsDescendLeafWise(t *testing.T) {
	old := &config.Config{}
	next := &config.Config{}
	next.Gateway.Defaults.Retry.HedgeDelay = time.Second
	next.Gateway.Defaults.Timeout.RelayTimeout = time.Second
	next.Gateway.Defaults.Reputation.InitialScore = 42

	d := diffConfig(old, next)

	for _, want := range []string{"gateway_config.defaults.retry_config", "gateway_config.defaults.timeout_config"} {
		if !slices.Contains(d.defaults, want) {
			t.Errorf("defaults = %v, want %s", d.defaults, want)
		}
	}
	if !slices.Contains(d.needsRestart, "gateway_config.defaults.reputation_config") {
		t.Errorf("needs_restart = %v, want the reputation block nothing reads", d.needsRestart)
	}
	if slices.Contains(d.defaults, "gateway_config.defaults.reputation_config") {
		t.Error("defaults claims to apply reputation_config, which nothing reads")
	}
}
