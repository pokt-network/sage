package router

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/override"
)

// LogLevelOverrideKey is where the admin-set log level lives in the override
// store. cmd/sagegw watches it so every replica follows, and a restarted
// process starts at it.
const LogLevelOverrideKey = "log_level"

// SetLogLevel hands the admin API the process's log level and the level the
// config (or SAGE_LOG_LEVEL) asked for, which DELETE returns to. cmd/sagegw
// builds the logger on a slog.LevelVar and passes it here after Build; an
// AdminAPI without one answers the log-level routes with 503.
func (a *AdminAPI) SetLogLevel(lv *slog.LevelVar, base slog.Level) {
	a.logLevel = lv
	a.logLevelBase = base
}

// SetOverrides hands the admin API the override store the runtime seams
// persist through. Nil means the seams are per process.
func (a *AdminAPI) SetOverrides(store override.Store) { a.overrides = store }

// handleGetLogLevel returns the level the process is logging at right now.
//
// `level` is the live value; `base` is what the config or SAGE_LOG_LEVEL
// asked for; `override` is the persisted admin setting, if any, and
// `persisted` whether such settings reach other replicas and survive a
// restart (Redis) or live on this replica only.
func (a *AdminAPI) handleGetLogLevel(w http.ResponseWriter, req *http.Request) {
	if a.logLevel == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "log level is not adjustable on this instance")
		return
	}
	out := map[string]any{
		"level":     levelName(a.logLevel.Level()),
		"base":      levelName(a.logLevelBase),
		"persisted": a.overrides != nil && a.overrides.Shared(),
	}
	if a.overrides != nil {
		if v, ok, err := a.overrides.Get(req.Context(), LogLevelOverrideKey); err == nil && ok {
			out["override"] = v
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// handleSetLogLevel changes the log level without a restart.
//
// Body: `{"level": "debug"}`, one of debug, info, warn, error. Applied to this
// process at once and written to the override store, from which every
// replica picks it up within the watch interval and a restarted process
// starts at it; with no Redis the store is this process only. DELETE
// /admin/log-level returns to the config's level. The `debug_log` feature
// flag, which logs request and response bodies per service, only produces
// output while this level is debug.
func (a *AdminAPI) handleSetLogLevel(w http.ResponseWriter, req *http.Request) {
	if a.logLevel == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "log level is not adjustable on this instance")
		return
	}
	var body struct {
		Level string `json:"level"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	lvl, ok := config.ParseLogLevel(body.Level)
	if !ok {
		writeJSONError(w, http.StatusBadRequest, "level must be one of debug, info, warn, error")
		return
	}
	if a.overrides != nil {
		ctx, cancel := context.WithTimeout(req.Context(), 3*time.Second)
		defer cancel()
		if err := a.overrides.Set(ctx, LogLevelOverrideKey, levelName(lvl)); err != nil {
			a.logger.Error("admin: persist log level", "error", err)
			writeJSONError(w, http.StatusInternalServerError, "log level not persisted, not applied: "+err.Error())
			return
		}
	}
	previous := a.logLevel.Level()
	a.logLevel.Set(lvl)
	// Said at Warn so it is on record whatever the level was or is now.
	a.logger.Warn("admin: log level changed", "from", levelName(previous), "to", levelName(lvl), "persisted", a.overrides != nil && a.overrides.Shared())
	writeJSON(w, http.StatusOK, map[string]any{"level": levelName(lvl), "previous": levelName(previous), "persisted": a.overrides != nil && a.overrides.Shared()})
}

// handleClearLogLevel removes the admin override and returns the process to
// the config's level (or SAGE_LOG_LEVEL's).
//
// Every replica follows through the override store; the body says whether
// there was an override to remove.
func (a *AdminAPI) handleClearLogLevel(w http.ResponseWriter, req *http.Request) {
	if a.logLevel == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "log level is not adjustable on this instance")
		return
	}
	removed := false
	if a.overrides != nil {
		ctx, cancel := context.WithTimeout(req.Context(), 3*time.Second)
		defer cancel()
		if _, ok, _ := a.overrides.Get(ctx, LogLevelOverrideKey); ok {
			removed = true
		}
		if err := a.overrides.Delete(ctx, LogLevelOverrideKey); err != nil {
			a.logger.Error("admin: clear log level override", "error", err)
			writeJSONError(w, http.StatusInternalServerError, "override not removed: "+err.Error())
			return
		}
	}
	previous := a.logLevel.Level()
	a.logLevel.Set(a.logLevelBase)
	a.logger.Warn("admin: log level override cleared", "from", levelName(previous), "to", levelName(a.logLevelBase))
	writeJSON(w, http.StatusOK, map[string]any{"level": levelName(a.logLevelBase), "previous": levelName(previous), "removed": removed})
}

// levelName is the config spelling of a slog level, for responses.
func levelName(l slog.Level) string {
	switch {
	case l <= slog.LevelDebug:
		return "debug"
	case l <= slog.LevelInfo:
		return "info"
	case l <= slog.LevelWarn:
		return "warn"
	default:
		return "error"
	}
}
