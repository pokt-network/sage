// Package safego runs work on a goroutine that cannot take the process down.
//
// net/http recovers a panic in the goroutine serving a request, so a nil map or
// a bad type assertion deep in the middleware chain costs one 500 and a log
// line. That protection ends at the goroutine boundary. Every `go` statement in
// this codebase leaves it: a hedge arm runs the same middleware chain on its
// own goroutine, so the identical panic on the identical request kills the
// gateway instead of failing the request — the outcome decided by whether
// hedging happened to fire. Background workers (health checks, the session
// poller, WebSocket read loops, the reputation writer) have no recovery at all,
// so one malformed supplier response ends the process for every other client.
//
// Crash-fast is a defensible policy. What is not defensible is applying it by
// accident, to a subset chosen by which goroutine a panic lands on. This package
// makes the choice uniform: a panic on a detached goroutine is logged with its
// stack, counted, and contained.
package safego

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
)

// panics counts recovered panics across the process. Exposed through Panics for
// the metrics layer, which reads it at scrape time rather than being pushed to:
// a panic is rare enough that a counter read on scrape costs nothing, and this
// package must not depend on the metrics package (the metrics package's own
// workers want to run under safego).
var panics atomic.Uint64

// Panics returns how many panics have been recovered since start. A non-zero
// value is worth an alert: nothing here is expected to panic, and a recovered
// panic means a request or a background task was abandoned partway.
func Panics() uint64 {
	return panics.Load()
}

// Go runs fn on a new goroutine, recovering and logging any panic.
//
// name identifies the work in the log line and should be stable — it is what an
// operator greps for. logger may be nil, in which case slog.Default is used;
// losing the log line is not a reason to lose the recovery.
func Go(logger *slog.Logger, name string, fn func()) {
	go Run(logger, name, fn)
}

// Run is Go without the goroutine: it runs fn on the calling goroutine under
// the same recovery. Use it when the caller already has a goroutine to run in
// (a worker loop body, a `go` statement that must also do cleanup) so the
// recovery wraps each unit of work rather than the whole loop — a loop that
// dies on its first panic is still a stopped worker, just a quieter one.
func Run(logger *slog.Logger, name string, fn func()) {
	defer Recover(logger, name)
	fn()
}

// Recover is the deferred half of Run, for call sites that need to keep their
// own `go func()` (to capture loop variables, close over a WaitGroup, or run
// their own defers in a particular order). Call it as the FIRST deferred
// statement in the goroutine:
//
//	go func() {
//		defer safego.Recover(logger, "hedge.primary")
//		defer cancel()
//		…
//	}()
//
// Defers run last-in-first-out, so listing it first means it runs last and can
// still contain a panic raised by the other defers.
func Recover(logger *slog.Logger, name string) {
	r := recover()
	if r == nil {
		return
	}
	panics.Add(1)
	if logger == nil {
		logger = slog.Default()
	}
	logger.Error("recovered from panic on a background goroutine",
		"work", name,
		"panic", r,
		"stack", string(debug.Stack()),
	)
}

// Call runs fn on the calling goroutine and turns a panic into an error.
//
// This is the variant for request-shaped work, and the distinction matters. A
// hedge arm that merely recovered would never send its result, and the arm
// waiting on that channel would block until its context expired — trading a
// crashed process for a hung request and a leaked goroutine, which is not an
// improvement. Returning an error instead lets the caller do what it already
// does with a failed arm: let the other one win, or report the failure.
//
// The returned error wraps ErrPanic, so a caller that wants to treat "the code
// is broken" differently from "the supplier is broken" can tell them apart.
func Call(logger *slog.Logger, name string, fn func() error) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		panics.Add(1)
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error("recovered from panic while handling a request",
			"work", name,
			"panic", r,
			"stack", string(debug.Stack()),
		)
		err = fmt.Errorf("%w in %s: %v", ErrPanic, name, r)
	}()
	return fn()
}

// ErrPanic marks an error that was produced by recovering a panic rather than
// by anything the request or the network did.
var ErrPanic = errors.New("recovered panic")

// GoCtx is Go for work that takes a context, which most background loops do.
func GoCtx(ctx context.Context, logger *slog.Logger, name string, fn func(context.Context)) {
	Go(logger, name, func() { fn(ctx) })
}
