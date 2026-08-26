package heuristic

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// relayerWrap mirrors protocol/shannon/relayer.go: every transport failure
// reaches the chain as a retryable ErrTransport RelayError wrapping the cause.
func relayerWrap(err error) error {
	return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", err, true)
}

// refusedError: a port nothing listens on.
func refusedError(t *testing.T) error {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	_, err = (&http.Client{Timeout: time.Second}).Post("http://"+addr, "application/json", nil)
	if err == nil {
		t.Fatal("expected a dial error")
	}
	return relayerWrap(err)
}

// hangError: a server that accepts and never answers; the CLIENT timeout fires.
func hangError(t *testing.T) error {
	t.Helper()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	t.Cleanup(func() { close(block); srv.Close() })
	_, err := (&http.Client{Timeout: 50 * time.Millisecond}).Post(srv.URL, "application/json", nil)
	if err == nil {
		t.Fatal("expected a timeout")
	}
	return relayerWrap(err)
}

// deadlineError: same hang, but the REQUEST context's deadline fires.
func deadlineError(t *testing.T) (error, error) {
	t.Helper()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	t.Cleanup(func() { close(block); srv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
	_, err := http.DefaultClient.Do(req)
	if err == nil {
		t.Fatal("expected a deadline error")
	}
	return relayerWrap(err), ctx.Err()
}

// cancelError: the client hangs up mid-flight.
func cancelError(t *testing.T) (error, error) {
	t.Helper()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	t.Cleanup(func() { close(block); srv.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
	_, err := http.DefaultClient.Do(req)
	if err == nil {
		t.Fatal("expected a cancel error")
	}
	return relayerWrap(err), ctx.Err()
}

// dnsError: a name that cannot resolve. .invalid is reserved (RFC 2606).
func dnsError(t *testing.T) error {
	t.Helper()
	_, err := (&http.Client{Timeout: 2 * time.Second}).Post("http://nonexistent.invalid:1/", "application/json", nil)
	if err == nil {
		t.Fatal("expected a DNS error")
	}
	return relayerWrap(err)
}

func TestAnalyzeTransportError_ConnectRefusedIsHostDead(t *testing.T) {
	r := AnalyzeTransportError(refusedError(t), nil)
	if r.Attribution != AttrSupplier || !r.ShouldCircuitBreak || r.PenaltySeverity != SeverityCritical {
		t.Fatalf("refused: %+v", r)
	}
	if r.MethodBlocking {
		t.Fatal("a dead host is not a method problem")
	}
	if r.Reason != "transport_connect_failed" {
		t.Fatalf("reason = %q", r.Reason)
	}
}

func TestAnalyzeTransportError_DNSIsHostDead(t *testing.T) {
	if _, err := net.LookupHost("nonexistent.invalid"); err == nil {
		t.Skip("resolver resolves .invalid names in this sandbox")
	}
	r := AnalyzeTransportError(dnsError(t), nil)
	if !r.ShouldCircuitBreak || r.MethodBlocking {
		t.Fatalf("dns: %+v", r)
	}
}

// TestAnalyzeTransportError_DNSErrorShapeIsConnectLevel classifies a
// hand-built DNS-not-found error, wrapped exactly as production wraps it,
// without depending on the sandbox's resolver behaving any particular way.
func TestAnalyzeTransportError_DNSErrorShapeIsConnectLevel(t *testing.T) {
	err := relayerWrap(&url.Error{
		Op:  "Post",
		URL: "http://nonexistent.invalid:1/",
		Err: &net.DNSError{Err: "no such host", Name: "nonexistent.invalid", IsNotFound: true},
	})
	r := AnalyzeTransportError(err, nil)
	if !r.ShouldCircuitBreak || r.MethodBlocking {
		t.Fatalf("dns shape: %+v", r)
	}
	if r.Reason != "transport_connect_failed" {
		t.Fatalf("reason = %q", r.Reason)
	}
}

func TestAnalyzeTransportError_TimeoutAfterConnectBlocksTheMethod(t *testing.T) {
	r := AnalyzeTransportError(hangError(t), nil)
	if !r.MethodBlocking || r.ShouldCircuitBreak {
		t.Fatalf("hang: %+v", r)
	}
	if r.Attribution != AttrSupplier || r.PenaltySeverity != SeverityMajor || !r.ShouldRetry || !r.ShouldPenalize {
		t.Fatalf("hang grading: %+v", r)
	}
	if r.Reason != "transport_timeout" {
		t.Fatalf("reason = %q", r.Reason)
	}
}

func TestAnalyzeTransportError_RequestDeadlineMidAttemptIsATimeout(t *testing.T) {
	err, ctxErr := deadlineError(t)
	r := AnalyzeTransportError(err, ctxErr)
	if !r.MethodBlocking || r.ShouldCircuitBreak {
		t.Fatalf("deadline: %+v", r)
	}
}

func TestAnalyzeTransportError_ClientCancelPenalisesNobody(t *testing.T) {
	err, ctxErr := cancelError(t)
	r := AnalyzeTransportError(err, ctxErr)
	if r.Attribution != AttrClient || r.ShouldPenalize || r.ShouldRetry || r.ShouldCircuitBreak || r.MethodBlocking {
		t.Fatalf("cancel: %+v", r)
	}
	if r.Reason != "client_cancelled" {
		t.Fatalf("reason = %q", r.Reason)
	}
}

// A cancel and a timeout can coincide on an unhedged attempt. The cancel wins:
// whatever the host was doing, nobody is waiting for the answer.
func TestAnalyzeTransportError_CancelWinsOverTimeout(t *testing.T) {
	r := AnalyzeTransportError(hangError(t), context.Canceled)
	if r.Attribution != AttrClient || r.MethodBlocking {
		t.Fatalf("cancel+timeout: %+v", r)
	}
}

func TestAnalyzeTransportError_OtherStaysMinorUnknown(t *testing.T) {
	other := domain.NewRelayError(domain.ErrProtocol, "failed to sign relay request", errors.New("boom"), false)
	r := AnalyzeTransportError(other, nil)
	if r.Attribution != AttrUnknown || r.PenaltySeverity != SeverityMinor || r.ShouldCircuitBreak || r.MethodBlocking {
		t.Fatalf("other: %+v", r)
	}
	if r.ShouldRetry {
		t.Fatal("ShouldRetry must follow domain.IsRetryable for the other bucket")
	}
}
