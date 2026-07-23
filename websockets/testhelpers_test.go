package websockets

import (
	"log/slog"
	"os"
)

// newTestLogger returns a slog logger that discards output during tests.
// Set the SAGE_TEST_VERBOSE environment variable to enable log output.
func newTestLogger() *slog.Logger {
	if os.Getenv("SAGE_TEST_VERBOSE") != "" {
		return slog.Default()
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 10}))
}
