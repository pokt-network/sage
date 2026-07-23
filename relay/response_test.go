package relay

import (
	"net/http/httptest"
	"sync"
	"testing"
)

// One HTTPResponseWriter is shared by every clone of a Context — Clone is a
// shallow copy — and batch/hedge hand those clones to concurrent goroutines.
// So concurrent SetHeader is not a hypothetical: it is how the type is used.
//
// Without the mutex this is a slice append from N goroutines at once. Run under
// -race it is a hard failure; unprotected it can also lose headers outright, or
// tear the slice header mid-realloc.
func TestHTTPResponseWriter_ConcurrentSetHeader(t *testing.T) {
	w := NewHTTPResponseWriter(httptest.NewRecorder())

	const goroutines = 64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Same key from every goroutine: the real pattern, where several
			// concurrent attempts each try to mark the response degraded.
			w.SetHeader(HeaderDegraded, "true")
		}()
	}
	wg.Wait()

	if len(w.headers) != 1 {
		t.Errorf("headers = %d, want exactly 1 — the same key must dedupe, not append per goroutine", len(w.headers))
	}
}

// Distinct keys exercise the append path rather than the overwrite path.
func TestHTTPResponseWriter_ConcurrentSetHeader_DistinctKeys(t *testing.T) {
	w := NewHTTPResponseWriter(httptest.NewRecorder())

	keys := []string{"X-A", "X-B", "X-C", "X-D", "X-E", "X-F", "X-G", "X-H"}
	var wg sync.WaitGroup
	for _, k := range keys {
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				w.SetHeader(key, "v")
			}(k)
		}
	}
	wg.Wait()

	if len(w.headers) != len(keys) {
		t.Errorf("headers = %d, want %d — an unsynchronised append loses entries", len(w.headers), len(keys))
	}
}

// Write commits and latches; concurrent writers must not double-commit or race
// the written flag against SetHeader.
func TestHTTPResponseWriter_ConcurrentWriteAndSetHeader(t *testing.T) {
	rec := httptest.NewRecorder()
	w := NewHTTPResponseWriter(rec)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.SetHeader(HeaderDegraded, "true")
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = w.Write([]byte("body"))
		}()
	}
	wg.Wait()

	if got := rec.Body.String(); got != "body" {
		t.Errorf("body = %q, want it written exactly once", got)
	}
}

// SetHeader after Write is a no-op, and must stay one under concurrency.
func TestHTTPResponseWriter_SetHeaderAfterWriteIsNoop(t *testing.T) {
	rec := httptest.NewRecorder()
	w := NewHTTPResponseWriter(rec)

	_ = w.Write([]byte("body"))
	w.SetHeader(HeaderDegraded, "true")

	if rec.Header().Get(HeaderDegraded) != "" {
		t.Error("a header set after commit must not reach the client")
	}
}
