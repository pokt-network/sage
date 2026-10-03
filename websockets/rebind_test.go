package websockets

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// newKillableEchoServer echoes until kill is closed, then drops every
// connection without a close frame — the supplier that vanished.
func newKillableEchoServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	var mu sync.Mutex
	var conns []*websocket.Conn
	// registered closes once the first connection is recorded. The dialer's
	// handshake completes before this handler runs its next line, so a kill
	// straight after the bridge starts could find no connection to close and
	// the bridge would never lose its endpoint: a CI-only flake under -race
	// (TestBridge_RebindHandlerErrorClosesClientWith1012, three runs to 2026-09-14).
	registered := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		conns = append(conns, conn)
		mu.Unlock()
		once.Do(func() { close(registered) })
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(mt, msg); err != nil {
				return
			}
		}
	}))
	kill := func() {
		select {
		case <-registered:
		case <-time.After(5 * time.Second):
			t.Error("killable server: no connection was ever registered")
		}
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	}
	return srv, kill
}

// recordingEchoServer echoes and records every message it received.
func newRecordingEchoServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			mu.Lock()
			got = append(got, string(msg))
			mu.Unlock()
			if err := conn.WriteMessage(mt, msg); err != nil {
				return
			}
		}
	}))
	return srv, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), got...) }
}

// TestBridge_RebindsToNewEndpointWithoutClosingClient: the first supplier
// vanishes; the handler dials a second; the client's socket never closes,
// frames keep flowing, and the replay reaches the second supplier.
func TestBridge_RebindsToNewEndpointWithoutClosingClient(t *testing.T) {
	first, kill := newKillableEchoServer(t)
	defer first.Close()
	second, received := newRecordingEchoServer(t)
	defer second.Close()

	var rebinds atomic.Int32
	handler := func(_ context.Context, cause error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
		rebinds.Add(1)
		conn, err := ConnectEndpoint(newTestLogger(), wsURL(second), nil)
		if err != nil {
			return nil, nil, nil, err
		}
		return conn, &prefixProcessor{clientPrefix: "", endpointPrefix: "2:"}, [][]byte{[]byte("replayed-subscribe")}, nil
	}
	obs := newRecordingObserver()
	srv, bridges := startBridgeServer(t, wsURL(first), WithEndpointLost(handler), WithObserver(obs))
	defer srv.Close()

	client := dialTestServer(t, srv)
	defer client.Close()
	b := <-bridges

	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("one")))
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, got, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "one", string(got))

	kill() // first supplier drops the socket

	// The replay must have reached the second supplier, and a fresh client
	// frame must be answered by it.
	require.Eventually(t, func() bool {
		for _, m := range received() {
			if m == "replayed-subscribe" {
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond, "replay never reached the new endpoint")

	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("two")))
	// The replayed subscribe is echoed too (prefixed "2:"); read until "two".
	for {
		_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, got, err = client.ReadMessage()
		require.NoError(t, err, "client must not see a close across a rebind")
		if string(got) == "2:two" {
			break
		}
	}
	require.Equal(t, int32(1), rebinds.Load())
	select {
	case <-b.Done():
		t.Fatal("bridge must stay up after a successful rebind")
	default:
	}
	obs.mu.Lock()
	require.Equal(t, []string{"ok"}, obs.rebinds)
	obs.mu.Unlock()
}

// TestBridge_RebindHandlerErrorClosesClientWith1012: nowhere to rebind to
// is today's behaviour — the client is told to reconnect.
func TestBridge_RebindHandlerErrorClosesClientWith1012(t *testing.T) {
	first, kill := newKillableEchoServer(t)
	defer first.Close()
	handler := func(context.Context, error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
		return nil, nil, nil, errors.New("no other supplier")
	}
	obs := newRecordingObserver()
	srv, bridges := startBridgeServer(t, wsURL(first), WithEndpointLost(handler), WithObserver(obs))
	defer srv.Close()

	client := dialTestServer(t, srv)
	defer client.Close()
	clientErr := readUntilClosed(client)
	b := <-bridges
	kill()

	select {
	case <-b.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("bridge must close when the rebind handler fails")
	}
	var ce *websocket.CloseError
	require.ErrorAs(t, <-clientErr, &ce)
	require.Equal(t, websocket.CloseServiceRestart, ce.Code)
	obs.mu.Lock()
	require.Equal(t, []string{"failed"}, obs.rebinds)
	obs.mu.Unlock()
}

// TestBridge_RebindLimitExhaustsWith1012: a supplier pool that keeps dying
// must not be rebound forever.
func TestBridge_RebindLimitExhaustsWith1012(t *testing.T) {
	first, killFirst := newKillableEchoServer(t)
	defer first.Close()
	// Every rebind lands on a server that will also be killed.
	var servers []*httptest.Server
	var kills []func()
	for i := 0; i < 3; i++ {
		s, k := newKillableEchoServer(t)
		servers, kills = append(servers, s), append(kills, k)
		defer s.Close()
	}
	var n atomic.Int32
	handler := func(context.Context, error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
		i := int(n.Add(1)) - 1
		if i >= len(servers) {
			return nil, nil, nil, errors.New("out of servers")
		}
		conn, err := ConnectEndpoint(newTestLogger(), wsURL(servers[i]), nil)
		return conn, &passthroughProcessor{}, nil, err
	}
	obs := newRecordingObserver()
	srv, bridges := startBridgeServer(t, wsURL(first), WithEndpointLost(handler), WithRebindLimit(2), WithObserver(obs))
	defer srv.Close()

	client := dialTestServer(t, srv)
	defer client.Close()
	clientErr := readUntilClosed(client)
	b := <-bridges

	killFirst()
	// Let each rebind land, then kill it.
	for i := 0; i < 2; i++ {
		require.Eventually(t, func() bool { return int(n.Load()) == i+1 }, 2*time.Second, 5*time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		kills[i]()
	}
	select {
	case <-b.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("bridge must close once the rebind limit is spent")
	}
	var ce *websocket.CloseError
	require.ErrorAs(t, <-clientErr, &ce)
	require.Equal(t, websocket.CloseServiceRestart, ce.Code)
	require.Equal(t, int32(2), n.Load(), "the handler must not be asked past the limit")
	obs.mu.Lock()
	require.Equal(t, []string{"ok", "ok", "exhausted"}, obs.rebinds)
	obs.mu.Unlock()
}

// A client-side loss is never a rebind: the client is gone.
func TestBridge_ClientLossDoesNotRebind(t *testing.T) {
	echo := newEchoServer(t)
	defer echo.Close()
	var called atomic.Int32
	handler := func(context.Context, error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
		called.Add(1)
		return nil, nil, nil, errors.New("must not be called")
	}
	srv, bridges := startBridgeServer(t, wsURL(echo), WithEndpointLost(handler))
	defer srv.Close()
	client := dialTestServer(t, srv)
	b := <-bridges
	client.Close()
	<-b.Done()
	require.Equal(t, int32(0), called.Load())
}

// TestBridge_ReplaceEndpointRebinds: an operator-requested replacement goes
// through the same rebind path as a loss, and the client sees nothing.
func TestBridge_ReplaceEndpointRebinds(t *testing.T) {
	first := newEchoServer(t)
	defer first.Close()
	second, received := newRecordingEchoServer(t)
	defer second.Close()
	handler := func(_ context.Context, cause error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
		require.ErrorIs(t, cause, ErrBridgeReplaceRequested)
		conn, err := ConnectEndpoint(newTestLogger(), wsURL(second), nil)
		return conn, &passthroughProcessor{}, [][]byte{[]byte("replay")}, err
	}
	srv, bridges := startBridgeServer(t, wsURL(first), WithEndpointLost(handler))
	defer srv.Close()
	client := dialTestServer(t, srv)
	defer client.Close()
	b := <-bridges

	b.ReplaceEndpoint(ErrBridgeReplaceRequested)
	require.Eventually(t, func() bool { return len(received()) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("after")))
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, got, err := client.ReadMessage()
		require.NoError(t, err)
		if string(got) == "after" {
			break
		}
	}
}

// A session rollover and an operator's rebind are planned replacements, not
// a dying pool: neither may eat the rebind budget that guards against the
// latter.
func TestBridge_PlannedRebindsDoNotCountTowardLimit(t *testing.T) {
	for _, cause := range []error{ErrBridgeSessionExpired, ErrBridgeReplaceRequested} {
		t.Run(cause.Error(), func(t *testing.T) { plannedRebindsKeepBudget(t, cause) })
	}
}

func plannedRebindsKeepBudget(t *testing.T, cause error) {
	first := newEchoServer(t)
	defer first.Close()
	var servers []*httptest.Server
	for i := 0; i < 3; i++ {
		s := newEchoServer(t)
		defer s.Close()
		servers = append(servers, s)
	}
	var n atomic.Int32
	handler := func(context.Context, error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
		i := int(n.Add(1)) - 1
		conn, err := ConnectEndpoint(newTestLogger(), wsURL(servers[i%len(servers)]), nil)
		return conn, &passthroughProcessor{}, nil, err
	}
	srv, bridges := startBridgeServer(t, wsURL(first), WithEndpointLost(handler), WithRebindLimit(1))
	defer srv.Close()
	client := dialTestServer(t, srv)
	defer client.Close()
	b := <-bridges

	for i := 0; i < 3; i++ {
		b.ReplaceEndpoint(cause)
		require.Eventually(t, func() bool { return int(n.Load()) == i+1 }, 2*time.Second, 5*time.Millisecond)
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-b.Done():
		t.Fatal("three planned rebinds must not exhaust a limit of one")
	default:
	}
}

// The limit counts losses inside rebindWindow only: a connection that lost
// three suppliers hours ago is not refused its next rollover, while one that
// lost three in the last few minutes still is.
func TestBridge_RebindLimitCountsRecentLossesOnly(t *testing.T) {
	b := &Bridge{
		endpointLost: func(context.Context, error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
			return nil, nil, nil, nil
		},
		rebindLimit: 3,
	}
	now := time.Now()
	old := now.Add(-2 * rebindWindow)
	b.losses = []time.Time{old, old, old}
	require.True(t, b.CanRebind(), "three losses outside the window must not spend the limit")
	b.losses = []time.Time{now, now, now}
	require.False(t, b.CanRebind(), "three losses inside the window spend it")
}

// TestBridge_EndpointProcessingErrorRebinds: a frame from the endpoint that
// fails processing (a relay miner's refusal, a response that fails
// verification) is that endpoint failing, and with a rebind handler it is
// met like any other endpoint loss: the client stays connected and the next
// supplier answers. It used to close the client with 1011.
func TestBridge_EndpointProcessingErrorRebinds(t *testing.T) {
	first, _ := newRecordingEchoServer(t)
	defer first.Close()
	second, _ := newRecordingEchoServer(t)
	defer second.Close()

	var causes []error
	var mu sync.Mutex
	handler := func(_ context.Context, cause error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
		mu.Lock()
		causes = append(causes, cause)
		mu.Unlock()
		conn, err := ConnectEndpoint(newTestLogger(), wsURL(second), nil)
		if err != nil {
			return nil, nil, nil, err
		}
		return conn, &prefixProcessor{endpointPrefix: "2:"}, nil, nil
	}
	bridges := make(chan *Bridge, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w,
			wsURL(first), nil, &failEndpointProcessor{}, WithEndpointLost(handler))
		if err != nil {
			return
		}
		bridges <- b
		<-b.Done()
	}))
	defer srv.Close()

	client := dialTestServer(t, srv)
	defer client.Close()
	b := <-bridges

	// The first endpoint's echo fails processing: a rebind, not a close.
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("one")))
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(causes) == 1 },
		3*time.Second, 10*time.Millisecond, "no rebind after the endpoint's frame failed processing")
	mu.Lock()
	require.ErrorIs(t, causes[0], ErrBridgeMessageProcessing)
	mu.Unlock()

	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("two")))
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, got, err := client.ReadMessage()
	require.NoError(t, err, "client must not see a close across the rebind")
	require.Equal(t, "2:two", string(got))
	select {
	case <-b.Done():
		t.Fatal("bridge must stay up after a successful rebind")
	default:
	}
}
