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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		conns = append(conns, conn)
		mu.Unlock()
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

// A session rollover is a planned replacement, not a dying pool: it must not
// eat the rebind budget that guards against the latter.
func TestBridge_SessionRolloverDoesNotCountTowardLimit(t *testing.T) {
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
		b.ReplaceEndpoint(ErrBridgeSessionExpired)
		require.Eventually(t, func() bool { return int(n.Load()) == i+1 }, 2*time.Second, 5*time.Millisecond)
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-b.Done():
		t.Fatal("three session rollovers must not exhaust a limit of one")
	default:
	}
}
