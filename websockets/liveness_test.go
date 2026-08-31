package websockets

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// newSilentServer upgrades and then never reads or writes. Because gorilla
// answers pings only from inside a read call, a peer that never reads never
// pongs either — the half-open / wedged supplier the liveness check exists
// to notice.
func newSilentServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		<-r.Context().Done()
	}))
}

// readUntilClosed keeps reading — which is what makes gorilla answer pings —
// and delivers the final read error once the peer closes.
func readUntilClosed(conn *websocket.Conn) <-chan error {
	done := make(chan error, 1)
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				done <- err
				return
			}
		}
	}()
	return done
}

func startBridgeServer(t *testing.T, endpointURL string, opts ...BridgeOption) (*httptest.Server, <-chan *Bridge) {
	t.Helper()
	bridges := make(chan *Bridge, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w,
			endpointURL, nil, &passthroughProcessor{}, opts...)
		if err != nil {
			return
		}
		bridges <- b
		<-b.Done()
	}))
	return srv, bridges
}

// TestBridge_SilentEndpointClosesWithinPongWait: an endpoint that stops
// answering pings is gone, whatever TCP says. The bridge must notice within
// one pong wait and send the client 1012 so it reconnects elsewhere, instead
// of holding a dead connection until the session boundary.
func TestBridge_SilentEndpointClosesWithinPongWait(t *testing.T) {
	silent := newSilentServer(t)
	defer silent.Close()
	srv, bridges := startBridgeServer(t, wsURL(silent), WithLiveness(20*time.Millisecond, 100*time.Millisecond))
	defer srv.Close()

	client := dialTestServer(t, srv)
	defer client.Close()
	clientErr := readUntilClosed(client) // a real client reads, and so pongs
	b := <-bridges

	select {
	case <-b.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("bridge kept a silent endpoint open past the pong wait")
	}

	var err error
	select {
	case err = <-clientErr:
	case <-time.After(time.Second):
		t.Fatal("client never saw the close")
	}
	var ce *websocket.CloseError
	require.ErrorAs(t, err, &ce)
	require.Equal(t, websocket.CloseServiceRestart, ce.Code, "client must be told to reconnect, got %d %q", ce.Code, ce.Text)
}

// TestBridge_SilentClientClosesWithinPongWait: the same in the other
// direction. A client that never reads never pongs; its connection is held
// for nothing.
func TestBridge_SilentClientClosesWithinPongWait(t *testing.T) {
	echo := newEchoServer(t)
	defer echo.Close()
	srv, bridges := startBridgeServer(t, wsURL(echo), WithLiveness(20*time.Millisecond, 100*time.Millisecond))
	defer srv.Close()

	client := dialTestServer(t, srv) // never reads
	defer client.Close()
	b := <-bridges

	select {
	case <-b.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("bridge kept a silent client open past the pong wait")
	}
}

// TestBridge_IdleButAliveStaysOpen is the control: an idle connection whose
// peers answer pings must NOT be closed by the read deadline — the pongs are
// what keep it alive. Without the ping side of this feature a deadline alone
// would cut every quiet subscription.
func TestBridge_IdleButAliveStaysOpen(t *testing.T) {
	echo := newEchoServer(t) // reads, so it pongs
	defer echo.Close()
	srv, bridges := startBridgeServer(t, wsURL(echo), WithLiveness(20*time.Millisecond, 100*time.Millisecond))
	defer srv.Close()

	client := dialTestServer(t, srv)
	defer client.Close()
	// A reading client pongs too.
	go func() {
		for {
			if _, _, err := client.ReadMessage(); err != nil {
				return
			}
		}
	}()
	b := <-bridges

	select {
	case <-b.Done():
		t.Fatal("an idle but responsive connection was closed by the liveness check")
	case <-time.After(500 * time.Millisecond): // 5× the pong wait, no traffic
	}
}

type recordingObserver struct {
	mu           sync.Mutex
	opened       int
	frames       map[MessageSource]int
	bytes        int
	unresponsive []MessageSource
	closed       []struct {
		initiator CloseInitiator
		code      int
	}
	rebinds []string
}

func (o *recordingObserver) Rebound(r RebindResult) {
	o.mu.Lock()
	o.rebinds = append(o.rebinds, string(r))
	o.mu.Unlock()
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{frames: map[MessageSource]int{}}
}
func (o *recordingObserver) Opened() { o.mu.Lock(); o.opened++; o.mu.Unlock() }
func (o *recordingObserver) Frame(s MessageSource, n int) {
	o.mu.Lock()
	o.frames[s]++
	o.bytes += n
	o.mu.Unlock()
}
func (o *recordingObserver) Unresponsive(s MessageSource) {
	o.mu.Lock()
	o.unresponsive = append(o.unresponsive, s)
	o.mu.Unlock()
}
func (o *recordingObserver) Closed(i CloseInitiator, code int) {
	o.mu.Lock()
	o.closed = append(o.closed, struct {
		initiator CloseInitiator
		code      int
	}{i, code})
	o.mu.Unlock()
}

// TestBridge_ObserverSeesLifecycle: open once, one frame each way for an
// echo, and exactly one close naming who ended it and the code the client
// was sent. This is the whole surface the WS metrics are built on.
func TestBridge_ObserverSeesLifecycle(t *testing.T) {
	echo := newEchoServer(t)
	defer echo.Close()
	obs := newRecordingObserver()
	srv, bridges := startBridgeServer(t, wsURL(echo), WithObserver(obs))
	defer srv.Close()

	client := dialTestServer(t, srv)
	b := <-bridges
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("hello")))
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, got, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))

	// Client hangs up politely.
	require.NoError(t, client.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"), time.Now().Add(time.Second)))
	client.Close()
	select {
	case <-b.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("bridge did not shut down after the client closed")
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	require.Equal(t, 1, obs.opened)
	require.Equal(t, 1, obs.frames[SourceClient], "one frame client→endpoint")
	require.Equal(t, 1, obs.frames[SourceEndpoint], "one frame endpoint→client")
	require.Equal(t, 10, obs.bytes)
	require.Len(t, obs.closed, 1)
	require.Equal(t, InitiatorClient, obs.closed[0].initiator)
	require.Equal(t, websocket.CloseNormalClosure, obs.closed[0].code)
}

// A silent endpoint is reported as such, and the close is the gateway's.
func TestBridge_ObserverSeesUnresponsiveEndpoint(t *testing.T) {
	silent := newSilentServer(t)
	defer silent.Close()
	obs := newRecordingObserver()
	srv, bridges := startBridgeServer(t, wsURL(silent), WithObserver(obs), WithLiveness(20*time.Millisecond, 100*time.Millisecond))
	defer srv.Close()

	client := dialTestServer(t, srv)
	defer client.Close()
	_ = readUntilClosed(client)
	b := <-bridges
	<-b.Done()

	obs.mu.Lock()
	defer obs.mu.Unlock()
	require.Equal(t, []MessageSource{SourceEndpoint}, obs.unresponsive)
	require.Len(t, obs.closed, 1)
	require.Equal(t, InitiatorGateway, obs.closed[0].initiator)
	require.Equal(t, websocket.CloseServiceRestart, obs.closed[0].code)
}
