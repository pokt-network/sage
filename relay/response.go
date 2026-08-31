package relay

import (
	"net/http"
	"sync"
)

// HeaderDegraded marks a response served after fallback endpoint selection.
//
// Set by the router from Context.Degraded once the chain has returned, rather
// than by the middleware that degrades. SelectEndpoint runs inside the batch
// and hedge fan-outs, so a header written there would come from whichever
// concurrent attempt happened to degrade — including a hedge arm that lost.
// Context.Degraded is merged deliberately (see hedge.mergeContext), so reading
// it after the chain is the only way the header agrees with it.
const HeaderDegraded = "X-Degraded"

// ResponseWriter abstracts writing the final HTTP response.
// Middleware can set headers before the response is committed.
type ResponseWriter interface {
	// SetHeader sets a response header. Can be called multiple times
	// before Write. Later calls overwrite earlier ones for the same key.
	SetHeader(key, value string)

	// SetStatusCode sets the HTTP status code.
	SetStatusCode(code int)

	// Write writes the response body and commits headers + status.
	// After Write is called, SetHeader and SetStatusCode are no-ops.
	Write(body []byte) error

	// SetShadow marks this response as shadow (don't write to client).
	SetShadow(shadow bool)
}

// HTTPResponseWriter wraps a standard http.ResponseWriter.
// Pending headers are a small slice, not a map: a relay sets at most a couple
// of headers (X-Request-ID, X-Degraded), so a linear scan beats a per-request
// map allocation.
//
// Every method is safe to call concurrently. That is not defensive: one writer
// is shared by every clone of a Context, because Context.Clone is a shallow
// copy, and the batch and hedge middleware hand those clones to concurrent
// goroutines. Nothing enforces that a middleware running inside a fan-out
// leaves the writer alone — SelectEndpoint used to append to headers from N
// goroutines at once — so the type has to hold its own.
type HTTPResponseWriter struct {
	w http.ResponseWriter

	mu      sync.Mutex
	headers []headerKV
	status  int
	written bool
	shadow  bool
}

type headerKV struct{ key, value string }

// NewHTTPResponseWriter creates a new HTTPResponseWriter.
func NewHTTPResponseWriter(w http.ResponseWriter) *HTTPResponseWriter {
	return &HTTPResponseWriter{
		w:      w,
		status: http.StatusOK,
	}
}

// SetHeader stages a response header, replacing any previous value for the
// same key. Headers are buffered rather than written through, so a middleware
// that sets one on a losing hedge arm cannot leak it onto the winner's
// response. After Write the response is committed and this is a no-op.
func (w *HTTPResponseWriter) SetHeader(key, value string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.written {
		return
	}
	for i := range w.headers {
		if w.headers[i].key == key {
			w.headers[i].value = value
			return
		}
	}
	w.headers = append(w.headers, headerKV{key: key, value: value})
}

// SetStatusCode stages the response status, defaulting to 200. Ignored once
// the response has been written.
func (w *HTTPResponseWriter) SetStatusCode(code int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.written {
		w.status = code
	}
}

// Status returns the status code the client was (or will be) sent — the staged
// value before Write, the committed value after. This is the client-facing
// HTTP status, distinct from any single relay attempt's status.
func (w *HTTPResponseWriter) Status() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status
}

func (w *HTTPResponseWriter) Write(body []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.shadow {
		return nil
	}
	if w.written {
		return nil
	}
	w.written = true
	for _, h := range w.headers {
		w.w.Header().Set(h.key, h.value)
	}
	w.w.WriteHeader(w.status)
	_, err := w.w.Write(body)
	return err
}

// SetShadow suppresses the response body. A shadowed relay still runs the full
// chain and is still sent to the supplier — it is scored, observed and metered
// like any other — but the client is never written to. That is what makes
// shadow mode useful for dark-launching a supplier or measuring backend
// latency in production: the traffic is real, the exposure is not.
func (w *HTTPResponseWriter) SetShadow(shadow bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.shadow = shadow
}
