package router

import (
	"context"
	"errors"
	"net/http"

	"github.com/pokt-network/sage/reload"
)

// Reloader re-reads the config file the gateway started with and applies the
// parts of it that have a runtime seam.
//
// An interface, and a small one, because the implementation lives in
// cmd/sagegw — that package builds the whole dependency graph and imports this
// one, so the dependency cannot run the other way. reload.Result is in its own
// package for the same reason.
type Reloader interface {
	// Reload applies what it can and reports what it could not, or returns an
	// error if the file would not boot. reload.ErrNoConfigFile means the
	// gateway has no file to re-read at all.
	Reload(ctx context.Context) (reload.Result, error)
}

// handleReload re-reads the config file the gateway started with (`-config`),
// validates it exactly as startup does, and applies the sections that have a
// runtime seam: the retry/hedge/timeout knobs, `feature_flags`,
// `active_health_checks`, `blocked_domains` and the `method_blocks` knobs.
//
// The response is the honest account. `applied` names the key paths that took
// effect, as they are written in the file — `gateway_config.defaults.retry_config`,
// `gateway_config.services[eth].timeout_config` — because an operator reading
// it is looking for the line they changed. `needs_restart` names what changed
// and did not take effect: a new service, a new listener, a new signing key, a
// different `chain_id`. `ignored` and `inert` repeat the parse's own
// complaints about the file just read.
//
// `warnings` is the third outcome, and the one worth reading. Validation is
// whole-file and runs first, so a file that would not start the binary is
// refused (400) with nothing changed. After that nothing aborts: once the
// first section has been written the file is partly in effect whatever happens
// next, so a section whose seam fails or is absent in this process is reported
// as a warning naming its own key — still a 200, and the sections after it
// still get their chance. Answering 400 there would tell an operator "nothing
// changed" about a gateway that had changed, and throw away the record of
// which half.
//
// A gateway started from `GATEWAY_CONFIG` has inline YAML rather than a path
// and answers 409 — there is nothing to re-read, which is not the same as a
// file that failed to validate. 501 means this build has no reloader wired at
// all.
//
// Runtime tuning overrides (`/admin/tuning`) survive a reload and still win
// over the reloaded base. Feature flags are different: `feature_flags` in the
// file is an override layer on the compiled defaults, so it is re-applied from
// the file and a flag flipped globally through this API is overwritten by what
// is written down. Per-service flag overrides are left alone — config carries
// global values only, and a deleted line in a file must not revoke a decision
// it never made.
//
// `SIGHUP` does the same thing.
func (a *AdminAPI) handleReload(w http.ResponseWriter, req *http.Request) {
	if a.reloader == nil {
		writeJSONError(w, http.StatusNotImplemented, "config reload is not available in this build")
		return
	}

	result, err := a.reloader.Reload(req.Context())
	switch {
	case errors.Is(err, reload.ErrNoConfigFile):
		// 409 rather than 400: the file is not the problem, and telling an
		// operator their config failed to validate when there is no config
		// file would send them to edit something that is not being read.
		writeJSONError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		a.logger.Warn("admin: config reload refused", "error", err)
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	a.logger.Info("admin: config reloaded",
		"applied", result.Applied,
		"needs_restart", result.NeedsRestart,
		"warnings", len(result.Warnings),
	)
	writeJSON(w, http.StatusOK, result)
}
