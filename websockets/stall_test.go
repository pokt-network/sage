package websockets

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// TestBridge_StallDetectorTriggersRebind: the detector says the connection
// has subscriptions and no data; the bridge must treat that as an endpoint
// loss and rebind, without closing the client.
func TestBridge_StallDetectorTriggersRebind(t *testing.T) {
	first := newEchoServer(t)
	defer first.Close()
	second, received := newRecordingEchoServer(t)
	defer second.Close()

	var stalled atomic.Bool
	var rebinds atomic.Int32
	handler := func(context.Context, error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
		rebinds.Add(1)
		// Clear the stall here, not after the assertion below. The detector
		// polls every 10ms and ReplaceEndpoint runs the rebind inline, so a
		// stall still set when the loop comes round again is a SECOND real
		// stall — correct behaviour, bounded by rebindLimit, but not what this
		// test is about. Clearing it as the rebind happens is what makes the
		// stall transient, which is what the exact counts below assert. Left
		// until after require.Eventually returned, this test failed on a
		// loaded machine (seen 2026-09-03 in a full -race run).
		stalled.Store(false)
		conn, err := ConnectEndpoint(newTestLogger(), wsURL(second), nil)
		return conn, &passthroughProcessor{}, [][]byte{[]byte("resubscribe")}, err
	}
	obs := newRecordingObserver()
	srv, bridges := startBridgeServer(t, wsURL(first),
		WithEndpointLost(handler),
		WithStallDetector(func() bool { return stalled.Load() }, 10*time.Millisecond),
		WithObserver(obs))
	defer srv.Close()

	client := dialTestServer(t, srv)
	defer client.Close()
	b := <-bridges

	time.Sleep(50 * time.Millisecond) // several ticks with no stall: nothing happens
	require.Equal(t, int32(0), rebinds.Load())

	stalled.Store(true)
	require.Eventually(t, func() bool {
		for _, m := range received() {
			if m == "resubscribe" {
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond, "a stall must rebind and replay")

	select {
	case <-b.Done():
		t.Fatal("bridge must stay up after a stall rebind")
	default:
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	require.Equal(t, 1, obs.stalls)
	require.Equal(t, []string{"ok"}, obs.rebinds)
}

// A stall without a rebind handler is a close with 1012: the client's
// reconnect draws a fresh supplier through selection.
func TestBridge_StallWithoutHandlerCloses1012(t *testing.T) {
	echo := newEchoServer(t)
	defer echo.Close()
	srv, bridges := startBridgeServer(t, wsURL(echo),
		WithStallDetector(func() bool { return true }, 10*time.Millisecond))
	defer srv.Close()

	client := dialTestServer(t, srv)
	defer client.Close()
	clientErr := readUntilClosed(client)
	b := <-bridges
	select {
	case <-b.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a stall must end the bridge when it cannot rebind")
	}
	var ce *websocket.CloseError
	require.ErrorAs(t, <-clientErr, &ce)
	require.Equal(t, websocket.CloseServiceRestart, ce.Code)
}
