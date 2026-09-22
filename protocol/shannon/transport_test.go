package shannon

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
)

// A burst wider than http.DefaultTransport's 2 idle connections per host must
// come back to the pool, so the next burst opens nothing. On the default
// transport the second burst opens n-2 new TLS connections, which is what put
// a third of mainnet CPU into handshakes.
func TestRelayTransportReusesABurstAndResumesTLS(t *testing.T) {
	const n = 20
	var gate atomic.Pointer[sync.WaitGroup]
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Hold every request until all n arrive, so the burst really needs n
		// connections at once.
		if wg := gate.Load(); wg != nil {
			wg.Done()
			wg.Wait()
		}
		_, _ = w.Write([]byte("ok"))
	}))
	var opened atomic.Int32
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			opened.Add(1)
		}
	}
	srv.StartTLS()
	defer srv.Close()

	tr := newRelayTransport()
	tr.TLSClientConfig.RootCAs = srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	client := &http.Client{Transport: tr}

	get := func() *http.Response {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Error(err) // not require: this runs on burst goroutines too
			return nil
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp
	}
	burst := func() {
		barrier := &sync.WaitGroup{}
		barrier.Add(n)
		gate.Store(barrier)
		var done sync.WaitGroup
		for range n {
			done.Go(func() { get() })
		}
		done.Wait()
	}

	burst()
	require.EqualValues(t, n, opened.Load())
	burst()
	require.EqualValues(t, n, opened.Load(), "second burst must reuse the first burst's connections")

	gate.Store(nil)
	tr.CloseIdleConnections()
	resp := get()
	require.NotNil(t, resp)
	require.EqualValues(t, n+1, opened.Load())
	require.True(t, resp.TLS.DidResume, "a new connection must resume the TLS session, not verify the chain again")
}

// A supplier's 307 must come back as the response, not be followed: following
// it re-sends the signed relay to wherever the supplier points, this pod's own
// loopback admin API included.
func TestRelayClientDoesNotFollowRedirects(t *testing.T) {
	var reached atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		reached.Add(1)
	}))
	defer elsewhere.Close()
	supplier := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+"/admin/reputation/reset/svc/ep", http.StatusTemporaryRedirect)
	}))
	defer supplier.Close()

	p := &Protocol{httpClient: newRelayClient(5 * time.Second)}
	resp, err := p.sendHTTP(context.Background(), supplier.URL, []byte("relay"), domain.RPCTypeJSONRPC)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode)
	require.Zero(t, reached.Load(), "the redirect target must never be contacted")
}
