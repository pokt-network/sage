package shannon

import (
	"log/slog"
	"os"
)

// newTestLogger returns a slog.Logger that writes to stderr at debug level.
func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}
