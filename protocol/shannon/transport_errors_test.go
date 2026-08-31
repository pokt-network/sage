package shannon

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
)

// This is the seam. SendRelay wraps whatever sendHTTP returns in
// domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", err, true)
// (relayer.go), and the Heuristic middleware hands exactly that to
// heuristic.AnalyzeTransportError together with the request context's Err().
// Everything downstream — the circuit breaker, the method blocks, the
// reputation signal — keys on the Reason the classifier returns, so the
// classifier has to recognise the shapes net/http ACTUALLY produces, not
// shapes a test built by hand.
//
// The test drives the real (*Protocol).sendHTTP against real listeners.
// sendHTTP touches only p.httpClient, so a Protocol with that one field set
// is the whole dependency — the same hand-built-struct style as the other
// tests in this package. SendRelay itself cannot be driven here: it needs a
// session, a signer and a full node.
func TestSendHTTP_TransportErrorShapesClassify(t *testing.T) {
	// A listener that accepts and never answers: the hanging-supplier case
	// PATH measured, and the one that must grade as a timeout rather than a
	// connect failure.
	block := make(chan struct{})
	hanging := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-block
	}))
	t.Cleanup(func() {
		close(block)
		hanging.Close()
	})

	t.Run("nothing listening is a connect failure", func(t *testing.T) {
		p := &Protocol{httpClient: &http.Client{Timeout: 5 * time.Second}}

		_, err := p.sendHTTP(context.Background(), "http://"+deadAddr(t), []byte(`{}`), domain.RPCTypeJSONRPC)
		if err == nil {
			t.Fatal("want an error from a port nothing listens on")
		}
		assertReason(t, err, nil, "transport_connect_failed")
	})

	t.Run("accepted and never answered is a timeout", func(t *testing.T) {
		p := &Protocol{httpClient: &http.Client{Timeout: 50 * time.Millisecond}}

		_, err := p.sendHTTP(context.Background(), hanging.URL, []byte(`{}`), domain.RPCTypeJSONRPC)
		if err == nil {
			t.Fatal("want an error when the client timeout fires")
		}
		assertReason(t, err, nil, "transport_timeout")
	})

	t.Run("dial that never completes is a connect failure", func(t *testing.T) {
		// A host that drops SYNs. net/http runs the dial under the request
		// context, so when Client.Timeout fires the error is a url.Error
		// around an http timeout — no net.OpError{Op: "dial"} anywhere in
		// the chain, the same shape as a host that accepted and went quiet.
		// The only fact that separates the two is whether a connection was
		// ever obtained, which is what the httptrace hook records.
		p := &Protocol{httpClient: &http.Client{
			Timeout: 50 * time.Millisecond,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			},
		}}

		_, err := p.sendHTTP(context.Background(), "http://10.255.255.1:1", []byte(`{}`), domain.RPCTypeJSONRPC)
		if err == nil {
			t.Fatal("want an error when the dial never completes")
		}
		assertReason(t, err, nil, "transport_connect_failed")
	})

	t.Run("context cancelled mid-flight is a client hang-up", func(t *testing.T) {
		p := &Protocol{httpClient: &http.Client{Timeout: 5 * time.Second}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		timer := time.AfterFunc(20*time.Millisecond, cancel)
		defer timer.Stop()

		_, err := p.sendHTTP(ctx, hanging.URL, []byte(`{}`), domain.RPCTypeJSONRPC)
		if err == nil {
			t.Fatal("want an error when the caller goes away")
		}
		// The relayer cannot tell a client hang-up from anything else, which
		// is why the request context's Err() is passed alongside.
		assertReason(t, err, ctx.Err(), "client_cancelled")
	})
}

// assertReason grades err exactly as the Heuristic middleware does.
func assertReason(t *testing.T, err, requestCtxErr error, want string) {
	t.Helper()
	relayErr := domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", err, true)
	got := heuristic.AnalyzeTransportError(relayErr, requestCtxErr)
	if got.Reason != want {
		t.Fatalf("Reason = %q, want %q (underlying error: %v)", got.Reason, want, err)
	}
}

// deadAddr returns a host:port that was bound and released, so a connection
// to it is refused rather than hanging on a firewalled address.
func deadAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}
