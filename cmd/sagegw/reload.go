package main

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/healthcheck"
	"github.com/pokt-network/sage/reload"
)

// Key paths a reload reports, in the vocabulary of the YAML an operator wrote.
// The same strings appear in Applied and in NeedsRestart, so a section moving
// from one list to the other (because someone built it a seam) reads as the
// same section rather than as a rename.
//
// The retry/timeout knobs have no constant: they are scattered across
// gateway_config.retry_config, gateway_config.defaults, and each service's own
// block, and Applied names the path that was actually edited rather than the
// seam they happen to share. An operator reading the response is looking for
// the line they changed.
const (
	keyFeatureFlags   = "feature_flags"
	keyHealthChecks   = "gateway_config.active_health_checks"
	keyBlockedDomains = "gateway_config.blocked_domains"
	keyMethodBlocks   = "gateway_config.method_blocks"
)

// blockedDomainSetter swaps the operator domain ban.
//
// An interface rather than a *shannon.Protocol because it names the capability
// a reload needs rather than the backend that happens to have it: the mock
// protocol hands out endpoints without consulting a blocklist, so under it
// this is nil and a changed blocked_domains has nowhere to go.
type blockedDomainSetter interface {
	SetBlockedDomains(entries []config.BlockedDomain) error
}

// Reload re-reads the config file SAGE started with, refuses it if it would
// not boot, and applies the parts of it that have a runtime seam.
//
// The parts that do not have one are the point. A gateway's service list, its
// listeners, its signing keys and its middleware chain are built once, and a
// reload that swapped the snapshot underneath them would leave an operator
// believing an edit was live. Those sections are named in Result.NeedsRestart
// instead — changed, and not in effect.
//
// Validation is whole-file and happens before anything is applied: a config
// that would not start the binary does not half-start it either. Tuning
// overrides set through the admin API are untouched, so an operator's runtime
// override still wins over the reloaded base — that is what the override layer
// promises, and a reload is not a revocation of it.
//
// Serialised by reloadMu: two reloads racing would interleave their apply
// steps, and the losing one would still report success.
func (a *App) Reload(ctx context.Context) (reload.Result, error) {
	res := reload.NewResult()

	if a.ConfigPath == "" {
		return res, reload.ErrNoConfigFile
	}

	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()

	// The only two hard failures. Both happen before anything has been
	// written, so "refused, nothing changed" is the literal truth.
	next, err := config.LoadFromFile(a.ConfigPath)
	if err != nil {
		return res, err
	}
	if err := validateConfig(next); err != nil {
		return res, err
	}

	res.Ignored = append(res.Ignored, next.Ignored...)
	res.Inert = append(res.Inert, next.Inert...)
	// Settings that load and probably do not mean what they look like. Carried
	// here for the same reason as Ignored and Inert: a reload is the moment an
	// operator is most likely to have just introduced one, and the startup log
	// that would otherwise have said so has already scrolled past.
	res.Warnings = append(res.Warnings, next.Warnings...)

	current := a.Config.Load()
	diff := diffConfig(current, next)
	res.NeedsRestart = append(res.NeedsRestart, diff.needsRestart...)

	// From here on nothing aborts the reload. Once the first section has been
	// written the file is partly in effect whatever happens next, so returning
	// an error would throw away the only record of which parts — and the
	// caller would report "nothing changed" about a gateway that had changed.
	// A seam that fails becomes a warning naming its own key, and the sections
	// after it still get their chance.

	// The knobs the middlewares resolve per request (retry, hedge, timeout).
	// Nothing to call: they read App.Config on every relay, so the Store at the
	// end of this function is the apply.
	res.Applied = append(res.Applied, diff.defaults...)

	if diff.flags {
		res.Warnings = append(res.Warnings, a.applyFlags(ctx, current.FeatureFlags, next.FeatureFlags)...)
		res.Applied = append(res.Applied, keyFeatureFlags)
	}

	if diff.healthChecks {
		if a.HealthExe == nil {
			res.Warnings = append(res.Warnings, unavailable(keyHealthChecks, "no health check executor is running"))
		} else {
			checks, warnings := healthcheck.BuildConfiguredChecks(next.Gateway.HealthChecks)
			for _, warning := range warnings {
				res.Warnings = append(res.Warnings, "health check config: "+warning)
			}
			a.HealthExe.SetConfiguredChecks(checks)
			a.HealthExe.SetBackendURLDedup(!next.Gateway.HealthChecks.DisableBackendURLDedup)
			res.Applied = append(res.Applied, keyHealthChecks)
		}
	}

	if diff.blockedDomains {
		if a.blockedDomains == nil {
			res.Warnings = append(res.Warnings,
				unavailable(keyBlockedDomains, "this gateway is running the mock protocol backend, which does not consult a blocklist"))
		} else if err := a.blockedDomains.SetBlockedDomains(next.Gateway.BlockedDomains); err != nil {
			// validateConfig has already compiled these entries, so a failure
			// here means the swap itself went wrong rather than the file.
			res.Warnings = append(res.Warnings, failed(keyBlockedDomains, err))
		} else {
			res.Applied = append(res.Applied, keyBlockedDomains)
		}
	}

	if diff.methodBlocks {
		if a.MethodBlocks == nil {
			res.Warnings = append(res.Warnings, unavailable(keyMethodBlocks, "no method-block store is wired"))
		} else {
			a.MethodBlocks.SetTTL(next.Gateway.MethodBlocks.EffectiveTTL())
			a.MethodBlocks.SetEscalation(next.Gateway.MethodBlocks.EffectiveEscalation())
			res.Applied = append(res.Applied, keyMethodBlocks)
		}
	}

	// Last, and unconditional: the snapshot is what the per-request closures
	// read, and it also carries the Ignored/Inert report for whatever
	// GET /admin/config grows into.
	//
	// Unconditional even when a section was reported as needing a restart —
	// the closures only ever read the knobs, so the worst a stale-looking
	// snapshot can do is resolve retry and timeout for a service the file no
	// longer lists (which still routes, from the wire-time service list) out
	// of the defaults instead of its own removed block. Withholding the swap
	// to avoid that would cost every applied knob in the same file.
	a.Config.Store(next)

	return res, nil
}

// failed phrases a seam that was reached and did not take the new value.
func failed(key string, err error) string {
	return fmt.Sprintf("%s changed but could not be applied, and is NOT in effect: %v", key, err)
}

// unavailable phrases a seam this process does not have at all.
func unavailable(key, why string) string {
	return fmt.Sprintf("%s changed but is NOT in effect: %s", key, why)
}

// applyFlags re-applies the feature-flag overrides the file carries and
// removes the ones it no longer does.
//
// The removal half is the one that is easy to leave out and impossible to
// notice: config flags are overrides on featureflag.DefaultFlags, so deleting
// a line from the file has to delete the override, not leave the last value
// standing. Set alone would make every flag an operator has ever written
// permanent.
//
// A runtime flag flip made through the admin API is overwritten by this, which
// is deliberate and unlike the tuning overrides: a flag has exactly one place
// it is stored, so re-applying the file is the only thing "reload" can mean
// for it.
func (a *App) applyFlags(ctx context.Context, old, next config.FeatureFlags) []string {
	if a.Flags == nil {
		return nil
	}

	var warnings []string

	names := make([]string, 0, len(next))
	for name := range next {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		if !featureflag.IsKnownFlag(name) {
			warnings = append(warnings,
				fmt.Sprintf("feature flag %q ignored: SAGE has no such flag, and setting it has no effect", name))
		}
		if err := a.Flags.Set(ctx, name, next[name]); err != nil {
			warnings = append(warnings, fmt.Sprintf("feature flag %q could not be set: %v", name, err))
		}
	}

	removed := make([]string, 0, len(old))
	for name := range old {
		if _, still := next[name]; !still {
			removed = append(removed, name)
		}
	}
	slices.Sort(removed)

	for _, name := range removed {
		// DeleteGlobal, not Delete: config carries global values only, so
		// wiping the per-service overrides an operator set through the admin
		// API would let a deleted line in a file revoke a decision it never
		// made.
		if err := a.Flags.DeleteGlobal(ctx, name); err != nil {
			warnings = append(warnings, fmt.Sprintf("feature flag %q could not be cleared: %v", name, err))
		}
	}

	return warnings
}

// configDiff is what one config differs from another by, expressed as the
// sections a reload knows how to apply plus a list of key paths it does not.
type configDiff struct {
	// defaults lists the key paths that changed and land on the snapshot
	// swap: gateway_config.retry_config, gateway_config.defaults.retry_config
	// and .timeout_config, and each service's own two blocks. They share one
	// seam and are reported as the paths that were edited, because the seam is
	// our word for it and the path is the operator's.
	defaults []string
	// flags is gateway-wide feature_flags.
	flags bool
	// healthChecks is gateway_config.active_health_checks.
	healthChecks bool
	// blockedDomains is gateway_config.blocked_domains.
	blockedDomains bool
	// methodBlocks is gateway_config.method_blocks.
	methodBlocks bool
	// needsRestart names every section that changed and has no seam, sorted
	// and deduplicated.
	needsRestart []string
}

// restart records a key path that changed with nothing to apply it to.
func (d *configDiff) restart(key string) {
	d.needsRestart = append(d.needsRestart, key)
}

// applyDefault records a key path that changed and is resolved per request
// from the config snapshot.
func (d *configDiff) applyDefault(key string) {
	d.defaults = append(d.defaults, key)
}

// diffConfig compares two configs section by section.
//
// It walks config.Config by reflection rather than comparing a hand-written
// list of sections, and the default branch of every switch below is
// needs_restart. That is the fail-safe that matters: a config field added
// later is reported as needing a restart without anyone remembering this file
// exists. The opposite default — unknown fields silently dropped — would make
// a reload quietly claim to have applied a section it never looked at.
func diffConfig(old, next *config.Config) configDiff {
	var d configDiff

	oldValue := reflect.ValueOf(*old)
	nextValue := reflect.ValueOf(*next)
	typ := oldValue.Type()

	for i := range typ.NumField() {
		field := typ.Field(i)
		switch field.Name {
		case "Ignored", "Inert", "Warnings":
			// Not configuration: these describe the parse of the file, and are
			// reported to the operator on their own.

		case "Gateway":
			d.diffGateway(old.Gateway, next.Gateway)

		case "FeatureFlags":
			d.flags = !reflect.DeepEqual(old.FeatureFlags, next.FeatureFlags)

		case "Admin":
			// Leaf granularity, because the fields differ in kind: addr and
			// auth_token are the listener SAGE is already serving on, while
			// max_drain is a ceiling read once at wire time. An operator
			// reading "admin_config" would not know which of them they moved.
			d.diffLeaves("admin_config", old.Admin, next.Admin)

		default:
			if !reflect.DeepEqual(oldValue.Field(i).Interface(), nextValue.Field(i).Interface()) {
				d.restart(yamlKey(field))
			}
		}
	}

	slices.Sort(d.needsRestart)
	d.needsRestart = slices.Compact(d.needsRestart)
	slices.Sort(d.defaults)
	d.defaults = slices.Compact(d.defaults)
	return d
}

// diffGateway walks gateway_config, which is where the seams are.
func (d *configDiff) diffGateway(old, next config.GatewayConfig) {
	oldValue := reflect.ValueOf(old)
	nextValue := reflect.ValueOf(next)
	typ := oldValue.Type()

	for i := range typ.NumField() {
		field := typ.Field(i)
		differs := !reflect.DeepEqual(oldValue.Field(i).Interface(), nextValue.Field(i).Interface())

		switch field.Name {
		case "Retry":
			// gateway_config.retry_config: the production config's own retry
			// block, folded into EffectiveDefaults.
			if differs {
				d.applyDefault("gateway_config.retry_config")
			}
		case "Defaults":
			d.diffDefaults("gateway_config.defaults", old.Defaults, next.Defaults)
		case "HealthChecks":
			d.healthChecks = d.healthChecks || differs
		case "BlockedDomains":
			d.blockedDomains = d.blockedDomains || differs
		case "MethodBlocks":
			d.methodBlocks = d.methodBlocks || differs
		case "Services":
			d.diffServices("gateway_config.services", old.Services, next.Services)
		case "UnifiedServices":
			d.diffUnified(old.UnifiedServices, next.UnifiedServices)
		default:
			if differs {
				d.restart("gateway_config." + yamlKey(field))
			}
		}
	}
}

// diffUnified walks gateway_config.unified_services, the newer PATH layout.
// It carries its own defaults and service list, which have the same seams as
// the gateway-level ones — EffectiveDefaults and AllServices read whichever
// format the config used.
func (d *configDiff) diffUnified(old, next config.UnifiedServicesConfig) {
	oldValue := reflect.ValueOf(old)
	nextValue := reflect.ValueOf(next)
	typ := oldValue.Type()

	for i := range typ.NumField() {
		field := typ.Field(i)
		differs := !reflect.DeepEqual(oldValue.Field(i).Interface(), nextValue.Field(i).Interface())

		switch field.Name {
		case "Defaults":
			d.diffDefaults("gateway_config.unified_services.defaults", old.Defaults, next.Defaults)
		case "Services":
			d.diffServices("gateway_config.unified_services.services", old.Services, next.Services)
		default:
			if differs {
				d.restart("gateway_config.unified_services." + yamlKey(field))
			}
		}
	}
}

// diffServices compares two service lists by ID.
//
// A service added or removed is the whole list changing: the QoS registry, the
// metric label sets and the validate middleware are all built from it once, so
// there is nothing finer to report and nothing to apply. Only services present
// in both are worth looking inside.
func (d *configDiff) diffServices(prefix string, old, next []config.ServiceConfig) {
	oldByID := servicesByID(old)
	nextByID := servicesByID(next)

	ids := make([]string, 0, len(nextByID))
	for id := range nextByID {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	if len(oldByID) != len(nextByID) {
		d.restart(prefix)
	}
	for _, id := range ids {
		before, existed := oldByID[id]
		if !existed {
			d.restart(prefix)
			continue
		}
		d.diffService(fmt.Sprintf("%s[%s]", prefix, id), before, nextByID[id])
	}
	for id := range oldByID {
		if _, still := nextByID[id]; !still {
			d.restart(prefix)
		}
	}
}

// servicesByID indexes a service list. A duplicate ID keeps the first entry,
// matching GetServiceConfig, which returns the first match.
func servicesByID(services []config.ServiceConfig) map[string]config.ServiceConfig {
	out := make(map[string]config.ServiceConfig, len(services))
	for _, svc := range services {
		if _, seen := out[svc.ID]; seen {
			continue
		}
		out[svc.ID] = svc
	}
	return out
}

// diffService compares one service's settings.
//
// retry_config and timeout_config are resolved per request by the same
// closures the gateway defaults feed, so they land on the defaults seam.
// Everything else — the QoS type, the chain-id assertion, the sync allowance,
// the external block sources — is compiled into a plugin or a poller at wire
// time and needs the process rebuilt.
func (d *configDiff) diffService(prefix string, old, next config.ServiceConfig) {
	oldValue := reflect.ValueOf(old)
	nextValue := reflect.ValueOf(next)
	typ := oldValue.Type()

	for i := range typ.NumField() {
		field := typ.Field(i)
		differs := !reflect.DeepEqual(oldValue.Field(i).Interface(), nextValue.Field(i).Interface())

		switch field.Name {
		case "ID":
			// The key the two entries were matched on; it cannot differ.
		case "Retry", "Timeout":
			if differs {
				d.applyDefault(prefix + "." + yamlKey(field))
			}
		default:
			if differs {
				d.restart(prefix + "." + yamlKey(field))
			}
		}
	}
}

// diffDefaults walks a ServiceDefaults block.
//
// Not compared whole, because its three fields do not share a fate:
// retry_config and timeout_config are resolved per request by the closures,
// while reputation_config sitting under defaults is read by nothing at all —
// reputation is configured from gateway_config.reputation_config. Reporting
// the block as applied would be a claim about a field nothing reads.
func (d *configDiff) diffDefaults(prefix string, old, next config.ServiceDefaults) {
	oldValue := reflect.ValueOf(old)
	nextValue := reflect.ValueOf(next)
	typ := oldValue.Type()

	for i := range typ.NumField() {
		field := typ.Field(i)
		if reflect.DeepEqual(oldValue.Field(i).Interface(), nextValue.Field(i).Interface()) {
			continue
		}
		switch field.Name {
		case "Retry", "Timeout":
			d.applyDefault(prefix + "." + yamlKey(field))
		default:
			d.restart(prefix + "." + yamlKey(field))
		}
	}
}

// diffLeaves reports every differing field of a struct that has no seam at
// all, one key path per field.
func (d *configDiff) diffLeaves(prefix string, old, next any) {
	oldValue := reflect.ValueOf(old)
	nextValue := reflect.ValueOf(next)
	typ := oldValue.Type()

	for i := range typ.NumField() {
		if !reflect.DeepEqual(oldValue.Field(i).Interface(), nextValue.Field(i).Interface()) {
			d.restart(prefix + "." + yamlKey(typ.Field(i)))
		}
	}
}

// yamlKey returns the key an operator actually types for a struct field.
//
// Read from the yaml tag rather than derived from the field name, so the
// reload report names the config as written rather than as Go spells it — the
// difference between "gateway_config.owned_apps_private_keys_hex" and
// "gateway_config.ownedappsprivatekeys".
func yamlKey(field reflect.StructField) string {
	tag := field.Tag.Get("yaml")
	name, _, _ := strings.Cut(tag, ",")
	if name == "" || name == "-" {
		return strings.ToLower(field.Name)
	}
	return name
}
