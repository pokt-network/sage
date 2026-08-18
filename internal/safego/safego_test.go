package safego

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func testLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	var mu sync.Mutex
	return slog.New(slog.NewTextHandler(&lockedWriter{w: &buf, mu: &mu}, nil)), &buf
}

type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// Run is synchronous, so the recovery has completed by the time it returns —
// which makes the counter and the log line safe to assert on directly.
func TestRun_ContainsPanic(t *testing.T) {
	logger, buf := testLogger()
	before := Panics()

	Run(logger, "test.work", func() { panic("boom") })

	if got := Panics() - before; got != 1 {
		t.Errorf("Panics() delta = %d, want 1", got)
	}
	out := buf.String()
	if !strings.Contains(out, "test.work") || !strings.Contains(out, "boom") {
		t.Errorf("log line lost the work name or the panic value: %s", out)
	}
	if !strings.Contains(out, "safego.") {
		t.Error("no stack in the log line — a contained panic with no stack is nearly as hard to diagnose as a crash")
	}
}

// The same for the detached form. Reaching the assertion at all is most of the
// point: an unrecovered panic on that goroutine takes the test binary with it,
// so this test cannot fail politely — it either passes or the run dies.
func TestGo_ContainsPanic(t *testing.T) {
	before := Panics()

	Go(nil, "test.detached", func() { panic("boom") })

	deadline := time.Now().Add(2 * time.Second)
	for Panics() == before {
		if time.Now().After(deadline) {
			t.Fatal("panic was never recovered")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestGo_NilLoggerStillRecovers(t *testing.T) {
	done := make(chan struct{})
	Go(nil, "test.nil-logger", func() {
		defer close(done)
		panic("boom")
	})
	<-done
	// Losing the log line must not mean losing the recovery.
}

func TestRun_ReturnsNormallyWithoutPanic(t *testing.T) {
	ran := false
	Run(nil, "test.ok", func() { ran = true })
	if !ran {
		t.Error("fn did not run")
	}
}

// Call is the variant for request-shaped work: the caller must still get a
// result, because a caller waiting on a channel that never receives is a hung
// request rather than a contained failure.
func TestCall_ConvertsPanicToError(t *testing.T) {
	logger, _ := testLogger()

	err := Call(logger, "test.request", func() error {
		panic("boom")
	})

	if err == nil {
		t.Fatal("Call returned nil for a panicking fn")
	}
	if !errors.Is(err, ErrPanic) {
		t.Errorf("error %v does not wrap ErrPanic, so callers cannot tell a bug from a bad supplier", err)
	}
	if !strings.Contains(err.Error(), "test.request") {
		t.Errorf("error %v does not name the work", err)
	}
}

func TestCall_PassesThroughNormalResults(t *testing.T) {
	sentinel := errors.New("upstream refused")

	if err := Call(nil, "test.ok", func() error { return nil }); err != nil {
		t.Errorf("Call returned %v for a successful fn", err)
	}
	err := Call(nil, "test.err", func() error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Errorf("Call replaced a real error with %v", err)
	}
	if errors.Is(err, ErrPanic) {
		t.Error("a returned error must not be reported as a panic")
	}
}

func TestGoCtx_PassesTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := make(chan error, 1)
	GoCtx(ctx, nil, "test.ctx", func(c context.Context) { got <- c.Err() })

	if err := <-got; !errors.Is(err, context.Canceled) {
		t.Errorf("context did not arrive intact: %v", err)
	}
}
