package router

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/pokt-network/sage/config"
)

// SetLogLevel hands the admin API the process's log level so PUT
// /admin/log-level can move it. cmd/sagegw builds the logger on a
// slog.LevelVar and passes it here after Build; an AdminAPI without one
// answers the log-level routes with 503 rather than pretending.
func (a *AdminAPI) SetLogLevel(lv *slog.LevelVar) { a.logLevel = lv }

// handleGetLogLevel returns the level the process is logging at right now.
//
// This is the live value, which may differ from logger_config.level: the
// SAGE_LOG_LEVEL environment variable overrides the file at startup, and PUT
// /admin/log-level moves it at runtime.
func (a *AdminAPI) handleGetLogLevel(w http.ResponseWriter, _ *http.Request) {
	if a.logLevel == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "log level is not adjustable on this instance")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"level": levelName(a.logLevel.Level())})
}

// handleSetLogLevel changes the process's log level without a restart.
//
// Body: `{"level": "debug"}`, one of debug, info, warn, error. The change
// applies to this instance only and does not survive a restart: the process
// comes back at logger_config.level, or at SAGE_LOG_LEVEL if that is set. It
// exists for the ten-minute look at a live problem: raise it, capture, put it
// back. The `debug_log` feature flag, which logs request and response bodies
// per service, only produces output while this level is debug.
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
	previous := a.logLevel.Level()
	a.logLevel.Set(lvl)
	// Said at Warn so it is on record whatever the level was or is now.
	a.logger.Warn("admin: log level changed", "from", levelName(previous), "to", levelName(lvl))
	writeJSON(w, http.StatusOK, map[string]any{"level": levelName(lvl), "previous": levelName(previous)})
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
