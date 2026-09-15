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

// The belt: a cancel that arrives as the ATTEMPT's error while the request
// context is still live. An endpoint cannot cancel our context, so this is
// never evidence about it — without the guard it falls to the catch-all and
// costs an innocent supplier a minor penalty. PATH reached this shape through
// a hedge fallthrough that reused a cancelled context (PR #529) and
// circuit-broke one operator across 12-18 pods for six hours.
func TestAnalyzeTransportError_CancelledAttemptWithLiveRequestContext(t *testing.T) {
	err := domain.NewRelayError(domain.ErrTransport, "relay failed", context.Canceled, true)
	r := AnalyzeTransportError(err, nil)
	if r.Attribution != AttrClient || r.ShouldPenalize || r.ShouldRetry || r.ShouldCircuitBreak || r.MethodBlocking {
		t.Fatalf("cancelled attempt, live request context: %+v", r)
	}
	if r.Reason != "client_cancelled" {
		t.Fatalf("reason = %q", r.Reason)
	}
}

// The guard must not swallow a deadline: a host that accepted the connection
// and then did not answer is a real signal about that host, and stays one.
func TestAnalyzeTransportError_DeadlineIsNotExcusedByTheCancelGuard(t *testing.T) {
	err := domain.NewRelayError(domain.ErrTransport, "relay failed", context.DeadlineExceeded, true)
	r := AnalyzeTransportError(err, nil)
	if r.Attribution != AttrSupplier || !r.ShouldPenalize || r.Reason != "transport_timeout" {
		t.Fatalf("deadline as the attempt error: %+v", r)
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

// A domain.ConnectError carries the fact the error shape cannot: the host was
// never reached. It must grade as a dead host whatever it wraps — here the
// exact http timeout a SYN-dropping host produces under Client.Timeout.
func TestAnalyzeTransportError_ConnectErrorIsDeadHost(t *testing.T) {
	inner := &url.Error{Op: "Post", URL: "http://10.255.255.1:1", Err: context.DeadlineExceeded}
	r := AnalyzeTransportError(relayerWrap(&domain.ConnectError{Cause: inner}), nil)
	if r.Reason != "transport_connect_failed" || !r.ShouldCircuitBreak || r.MethodBlocking {
		t.Fatalf("connect error graded as %+v", r)
	}
}

// A relay miner answering with a status instead of a relay is graded by that
// status: 5xx and 429 are the supplier's layer, retried and scored minor
// (the rate term accumulates a steady stream); 413 is the client's payload,
// neither retried nor scored. Before 2026-09-13 all of them were
// transport_error with attribution unknown.
func TestAnalyzeTransportError_UpstreamStatusIsGradedByStatus(t *testing.T) {
	cases := []struct {
		status      int
		wantReason  string
		wantAttr    ErrorAttribution
		wantRetry   bool
		wantPenalty bool
	}{
		{502, "upstream_5xx", AttrSupplier, true, true},
		{503, "upstream_5xx", AttrSupplier, true, true},
		{429, "upstream_429", AttrSupplier, true, true},
		{413, "upstream_413", AttrClient, false, false},
		{404, "upstream_4xx", AttrSupplier, true, true},
	}
	for _, tc := range cases {
		err := domain.NewRelayError(domain.ErrEndpoint, "upstream endpoint unavailable", &domain.UpstreamStatusError{Status: tc.status}, true)
		r := AnalyzeTransportError(err, nil)
		if r.Reason != tc.wantReason || r.Attribution != tc.wantAttr || r.ShouldRetry != tc.wantRetry || r.ShouldPenalize != tc.wantPenalty {
			t.Errorf("status %d: got reason %q attr %v retry %v penalize %v; want %q %v %v %v",
				tc.status, r.Reason, r.Attribution, r.ShouldRetry, r.ShouldPenalize, tc.wantReason, tc.wantAttr, tc.wantRetry, tc.wantPenalty)
		}
		if r.ShouldCircuitBreak {
			t.Errorf("status %d: a miner status must not open the breaker", tc.status)
		}
	}
}

// A response over the ceiling is the request's size: another supplier would
// send the same bytes, so it is neither retried nor scored.
func TestAnalyzeTransportError_ResponseTooLargeIsTheClients(t *testing.T) {
	err := domain.NewRelayError(domain.ErrEndpoint, "upstream response too large", domain.ErrResponseTooLarge, false)
	r := AnalyzeTransportError(err, nil)
	if r.Reason != "response_too_large" || r.Attribution != AttrClient || r.ShouldRetry || r.ShouldPenalize || r.ShouldCircuitBreak {
		t.Fatalf("got %+v", r)
	}
}
