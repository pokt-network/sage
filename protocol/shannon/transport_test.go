package shannon

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
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
