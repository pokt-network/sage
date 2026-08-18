package websockets

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// ---------- Helpers ----------

// newEchoServer creates an httptest.Server that upgrades to WebSocket and echoes
// every message it receives.
func newEchoServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			if err := conn.WriteMessage(mt, msg); err != nil {
				return
			}
		}
	}))
}

// newPushServer creates an httptest.Server that upgrades to WebSocket and
// immediately pushes n messages, then stays open.
func newPushServer(t *testing.T, msgs []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for _, m := range msgs {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(m)); err != nil {
				return
			}
		}
		// Stay open until connection is closed by the other side.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
}

// wsURL converts an http:// test server URL to ws://.
func wsURL(s *httptest.Server) string {
	return "ws" + strings.TrimPrefix(s.URL, "http")
}

// dialTestServer connects to an httptest.Server that runs a bridge, returns the
// client WebSocket connection.
func dialTestServer(t *testing.T, bridgeServer *httptest.Server) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(bridgeServer), nil)
	require.NoError(t, err)
	return conn
}

// passthroughProcessor is a MessageProcessor that passes messages unchanged.
type passthroughProcessor struct{}

func (p *passthroughProcessor) ProcessClientMessage(data []byte) ([]byte, error) {
	return data, nil
}
func (p *passthroughProcessor) ProcessEndpointMessage(data []byte) ([]byte, error) {
	return data, nil
}

// prefixProcessor prepends a tag to each message so tests can verify routing.
type prefixProcessor struct {
	clientPrefix   string
	endpointPrefix string
}

func (p *prefixProcessor) ProcessClientMessage(data []byte) ([]byte, error) {
	return append([]byte(p.clientPrefix), data...), nil
}
func (p *prefixProcessor) ProcessEndpointMessage(data []byte) ([]byte, error) {
	return append([]byte(p.endpointPrefix), data...), nil
}

// failClientProcessor returns an error for every client message.
type failClientProcessor struct{}

func (p *failClientProcessor) ProcessClientMessage([]byte) ([]byte, error) {
	return nil, fmt.Errorf("intentional client processing error")
}
func (p *failClientProcessor) ProcessEndpointMessage(data []byte) ([]byte, error) {
	return data, nil
}

// failEndpointProcessor returns an error for every endpoint message.
type failEndpointProcessor struct{}

func (p *failEndpointProcessor) ProcessClientMessage(data []byte) ([]byte, error) {
	return data, nil
}
func (p *failEndpointProcessor) ProcessEndpointMessage([]byte) ([]byte, error) {
	return nil, fmt.Errorf("intentional endpoint processing error")
}

// ---------- Bridge tests ----------

func TestBridge_ClientToEndpoint(t *testing.T) {
	endpointSrv := newEchoServer(t)
	defer endpointSrv.Close()

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w,
			wsURL(endpointSrv), nil, &passthroughProcessor{})
		require.NoError(t, err)
		<-b.Done()
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	require.NoError(t, clientConn.WriteMessage(websocket.TextMessage, []byte("hello")))

	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, got, err := clientConn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
}

func TestBridge_EndpointToClient(t *testing.T) {
	pushSrv := newPushServer(t, []string{"msg1", "msg2", "msg3"})
	defer pushSrv.Close()

	received := make(chan string, 10)

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w,
			wsURL(pushSrv), nil, &passthroughProcessor{})
		require.NoError(t, err)
		<-b.Done()
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	go func() {
		for {
			_, data, err := clientConn.ReadMessage()
			if err != nil {
				return
			}
			received <- string(data)
		}
	}()

	timeout := time.After(2 * time.Second)
	got := make(map[string]bool)
	for len(got) < 3 {
		select {
		case m := <-received:
			got[m] = true
		case <-timeout:
			t.Fatalf("timed out; received %d/3 messages", len(got))
		}
	}
	require.True(t, got["msg1"])
	require.True(t, got["msg2"])
	require.True(t, got["msg3"])
}

func TestBridge_ProcessorTransformsMessages(t *testing.T) {
	endpointSrv := newEchoServer(t)
	defer endpointSrv.Close()

	proc := &prefixProcessor{clientPrefix: "C:", endpointPrefix: "E:"}

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w,
			wsURL(endpointSrv), nil, proc)
		require.NoError(t, err)
		<-b.Done()
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	// Client sends "hello" → ProcessClientMessage prepends "C:" → endpoint receives "C:hello"
	// Echo server bounces "C:hello" back → ProcessEndpointMessage prepends "E:" → client sees "E:C:hello"
	require.NoError(t, clientConn.WriteMessage(websocket.TextMessage, []byte("hello")))

	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, got, err := clientConn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "E:C:hello", string(got))
}

func TestBridge_ShutdownOnClientProcessingError(t *testing.T) {
	endpointSrv := newEchoServer(t)
	defer endpointSrv.Close()

	doneCh := make(chan struct{})

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w,
			wsURL(endpointSrv), nil, &failClientProcessor{})
		require.NoError(t, err)
		<-b.Done()
		close(doneCh)
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	require.NoError(t, clientConn.WriteMessage(websocket.TextMessage, []byte("trigger error")))

	select {
	case <-doneCh:
		// bridge shut down cleanly
	case <-time.After(3 * time.Second):
		t.Fatal("bridge did not shut down after client processing error")
	}
}

func TestBridge_ShutdownOnEndpointProcessingError_NoPanic(t *testing.T) {
	// Run multiple iterations to exercise the race between readLoop sending on
	// msgChan and Shutdown (which must NOT close msgChan).
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprintf("iter_%d", i), func(t *testing.T) {
			t.Parallel()

			// Push many messages so readLoop races with shutdown.
			var msgs []string
			for j := 0; j < 50; j++ {
				msgs = append(msgs, fmt.Sprintf("push-%d", j))
			}
			pushSrv := newPushServer(t, msgs)
			defer pushSrv.Close()

			doneCh := make(chan struct{})

			bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, err := StartBridge(context.Background(), newTestLogger(), r, w,
					wsURL(pushSrv), nil, &failEndpointProcessor{})
				if err != nil {
					return
				}
				<-b.Done()
				close(doneCh)
			}))
			defer bridgeSrv.Close()

			clientConn, _, err := websocket.DefaultDialer.Dial(wsURL(bridgeSrv), nil)
			require.NoError(t, err)
			defer clientConn.Close()

			_ = clientConn.WriteMessage(websocket.TextMessage, []byte("hello"))

			select {
			case <-doneCh:
				// bridge shut down without panicking
			case <-time.After(5 * time.Second):
				t.Fatal("bridge did not shut down within timeout")
			}
		})
	}
}

func TestBridge_EndpointUnavailable(t *testing.T) {
	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := StartBridge(context.Background(), newTestLogger(), r, w,
			"ws://127.0.0.1:1", nil, &passthroughProcessor{})
		require.Error(t, err)
	}))
	defer bridgeSrv.Close()

	// Initiate the upgrade so StartBridge has a real HTTP request to work with.
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL(bridgeSrv), nil)
	// The server wrote an error after upgrade, so the dial itself may succeed or
	// fail; we only care that the bridge returned an error (tested above via
	// require.Error inside the handler).
	if err == nil {
		clientConn.Close()
	}
}

func TestBridge_ContextCancel(t *testing.T) {
	endpointSrv := newEchoServer(t)
	defer endpointSrv.Close()

	ctx, cancel := context.WithCancel(context.Background())

	doneCh := make(chan struct{})

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(ctx, newTestLogger(), r, w,
			wsURL(endpointSrv), nil, &passthroughProcessor{})
		require.NoError(t, err)
		<-b.Done()
		close(doneCh)
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	cancel()

	select {
	case <-doneCh:
		// bridge shut down after context cancel
	case <-time.After(3 * time.Second):
		t.Fatal("bridge did not shut down after context cancel")
	}
}

// ---------- Connection tests ----------

func TestConnection_ReadWrite(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer conn.Close()

		// server echoes one message
		_, msg, err := conn.ReadMessage()
		require.NoError(t, err)
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, msg))
	}))
	defer srv.Close()

	raw, _, err := websocket.DefaultDialer.Dial(wsURL(srv), nil)
	require.NoError(t, err)

	c := NewConnection(raw, SourceClient, newTestLogger())
	require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte("ping")))

	_ = raw.SetReadDeadline(time.Now().Add(time.Second))
	mt, data, err := c.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.TextMessage, mt)
	require.Equal(t, "ping", string(data))
}

func TestConnection_CloseInfoThreadSafety(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, _ := upgrader.Upgrade(w, r, nil)
		defer conn.Close()
		// just hang open
		time.Sleep(time.Second)
	}))
	defer srv.Close()

	raw, _, err := websocket.DefaultDialer.Dial(wsURL(srv), nil)
	require.NoError(t, err)
	defer raw.Close()

	c := NewConnection(raw, SourceClient, newTestLogger())

	// Hammer Get/Set from multiple goroutines.
	const workers = 20
	done := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func(i int) {
			for j := 0; j < 100; j++ {
				c.SetCloseInfo(1000+i, fmt.Sprintf("text-%d", i))
				c.GetCloseInfo()
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < workers; i++ {
		<-done
	}
	// Reaching here without the race detector firing is the pass condition.
}

func TestConnection_Close(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, _ := upgrader.Upgrade(w, r, nil)
		defer conn.Close()
		time.Sleep(time.Second)
	}))
	defer srv.Close()

	raw, _, err := websocket.DefaultDialer.Dial(wsURL(srv), nil)
	require.NoError(t, err)

	c := NewConnection(raw, SourceClient, newTestLogger())
	require.NoError(t, c.Close())

	// A second close should return an error (already closed), not panic.
	_ = c.Close()
}

// ---------- Upgrade helpers tests ----------

func TestUpgradeClient_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := UpgradeClient(newTestLogger(), r, w)
		require.NoError(t, err)
		require.NotNil(t, conn)
		conn.Close()
	}))
	defer srv.Close()

	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL(srv), nil)
	require.NoError(t, err)
	clientConn.Close()
}

func TestConnectEndpoint_InvalidURL(t *testing.T) {
	conn, err := ConnectEndpoint(newTestLogger(), "not-a-url", nil)
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestConnectEndpoint_UnreachableHost(t *testing.T) {
	conn, err := ConnectEndpoint(newTestLogger(), "ws://127.0.0.1:1/ws", nil)
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestConnectEndpoint_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	conn, err := ConnectEndpoint(newTestLogger(), wsURL(srv), nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	conn.Close()
}

// ---------- Reserved close codes ----------

// newAbruptServer upgrades, then rips the TCP connection out from under the
// peer without a close handshake. gorilla surfaces that to the reader as
// *CloseError{Code: 1006} — the code the RFC reserves for "the connection
// dropped", which by definition no endpoint may ever put on the wire.
func newAbruptServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Close the underlying TCP connection directly — no close frame.
		_ = conn.UnderlyingConn().Close()
	}))
}

// The bug, end to end: a supplier drops its TCP connection, gorilla reports
// 1006, and the bridge propagates that code verbatim onto the wire to the
// client. 1006 is reserved — gorilla's own validReceivedCloseCodes marks it
// false — so the receiver rejects the frame as a protocol error rather than
// reading a close. The client learns nothing about why it was disconnected,
// and the relay miner on the other side rejects our frame the same way.
func TestBridge_NeverEmitsReservedCloseCode(t *testing.T) {
	endpointSrv := newAbruptServer(t)
	defer endpointSrv.Close()

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w,
			wsURL(endpointSrv), nil, &passthroughProcessor{})
		require.NoError(t, err)
		<-b.Done()
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	_ = clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := clientConn.ReadMessage()
	require.Error(t, err, "the bridge should close the client connection")

	// The client must receive a legible close frame, not a protocol error.
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr,
		"client got %v — a reserved close code on the wire is rejected as a protocol error, not read as a close", err)

	require.NotEqual(t, websocket.CloseAbnormalClosure, closeErr.Code,
		"1006 is reserved and must never be sent")
	require.True(t, isSendableCloseCode(closeErr.Code),
		"close code %d is not valid to send", closeErr.Code)
}

// sanitizeCloseCode is the single choke point, so cover the whole table here
// rather than spinning up a server per code.
func TestSanitizeCloseCode(t *testing.T) {
	cases := []struct {
		name string
		code int
		want int
	}{
		{"1006 abnormal closure is reserved", websocket.CloseAbnormalClosure, websocket.CloseInternalServerErr},
		{"1005 no status received is reserved", websocket.CloseNoStatusReceived, websocket.CloseInternalServerErr},
		{"1015 TLS handshake is reserved", websocket.CloseTLSHandshake, websocket.CloseInternalServerErr},
		{"normal closure passes through", websocket.CloseNormalClosure, websocket.CloseNormalClosure},
		{"service restart passes through", websocket.CloseServiceRestart, websocket.CloseServiceRestart},
		{"going away passes through", websocket.CloseGoingAway, websocket.CloseGoingAway},
		// The bridge propagates supplier-chosen codes; the application range is
		// explicitly valid to receive, so it must survive untouched.
		{"application code 4000 passes through", 4000, 4000},
		{"application code 3000 passes through", 3000, 3000},
		{"application code 4999 passes through", 4999, 4999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sanitizeCloseCode(tc.code))
		})
	}
}

// Whatever sanitizeCloseCode returns must itself be sendable, for every input
// a peer could hand us — including codes gorilla has no name for.
func TestSanitizeCloseCode_OutputAlwaysSendable(t *testing.T) {
	for code := 0; code <= 5100; code++ {
		if got := sanitizeCloseCode(code); !isSendableCloseCode(got) {
			t.Fatalf("sanitizeCloseCode(%d) = %d, which is not sendable", code, got)
		}
	}
}

// ---------- Endpoint dial failure ----------

// The client upgrade succeeds and the upstream dial then fails — the one window
// where the client is a live peer but no bridge exists yet, so Shutdown's
// close-frame path is unavailable. Closing bare here told the client 1006,
// blaming it for an upstream outage.
func TestBridge_DialFailureSendsLegibleCloseFrame(t *testing.T) {
	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Port 1 on loopback: nothing listens, so the dial fails fast.
		_, err := StartBridge(context.Background(), newTestLogger(), r, w,
			"ws://127.0.0.1:1", nil, &passthroughProcessor{})
		require.Error(t, err, "dialling an unreachable endpoint must fail")
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	_ = clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := clientConn.ReadMessage()
	require.Error(t, err)

	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr,
		"client got %v — it should receive a close frame, not an abrupt disconnect", err)
	require.Equal(t, websocket.CloseTryAgainLater, closeErr.Code,
		"an unavailable upstream should tell the client to retry, not blame the client")
}

// ---------- Read limit ----------

// gorilla buffers a whole frame before returning it, so an unbounded frame is
// an OOM. The limit has to hold on the CLIENT side too: a malicious client is
// just as able to send one as a supplier.
func TestBridge_OversizedClientFrameIsRejected(t *testing.T) {
	endpointSrv := newEchoServer(t)
	defer endpointSrv.Close()

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w,
			wsURL(endpointSrv), nil, &passthroughProcessor{})
		require.NoError(t, err)
		<-b.Done()
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	// The client's own write limit would stop us short of the bridge, so raise it.
	oversized := make([]byte, maxMessageBytes+1024)
	for i := range oversized {
		oversized[i] = 'a'
	}
	require.NoError(t, clientConn.WriteMessage(websocket.TextMessage, oversized))

	// The bridge must tear the connection down rather than buffer the frame.
	_ = clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := clientConn.ReadMessage()
	require.Error(t, err, "an oversized frame must not be echoed back")
}

// A frame at the limit is ordinary traffic and must survive: the cap exists to
// stop abuse, not to clip large-but-legal eth_getLogs responses.
func TestBridge_FrameUnderLimitPassesThrough(t *testing.T) {
	endpointSrv := newEchoServer(t)
	defer endpointSrv.Close()

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w,
			wsURL(endpointSrv), nil, &passthroughProcessor{})
		require.NoError(t, err)
		<-b.Done()
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	large := make([]byte, 1<<20) // 1 MiB — well under the 15MB cap
	for i := range large {
		large[i] = 'b'
	}
	require.NoError(t, clientConn.WriteMessage(websocket.TextMessage, large))

	_ = clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, got, err := clientConn.ReadMessage()
	require.NoError(t, err, "a 1 MiB frame is legitimate traffic")
	require.Len(t, got, len(large))
}

// The read limit is set by NewConnection, so it applies to whichever peer the
// wrapper is built for — not only the client-facing one.
func TestNewConnection_SetsReadLimitOnBothSides(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	raw, _, err := websocket.DefaultDialer.Dial(wsURL(srv), nil)
	require.NoError(t, err)
	defer raw.Close()

	_ = NewConnection(raw, SourceEndpoint, newTestLogger())

	// gorilla exposes no getter for the read limit, so assert it behaviourally:
	// a frame over the cap must produce a read error rather than be delivered.
	oversized := make([]byte, maxMessageBytes+1024)
	require.NoError(t, raw.WriteMessage(websocket.TextMessage, oversized))

	_ = raw.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err = raw.ReadMessage()
	require.Error(t, err, "an echoed oversized frame must trip the read limit")
}

// newCloseRecordingServer upgrades, then blocks on reads until the peer closes.
// The close code it received (or 1006 if the socket was merely dropped) is sent
// on the returned channel.
//
// This is what the relay miner sees. Asserting on it is the whole point: a test
// that inspected endpointCloseCode directly would still pass if Shutdown never
// called it, which is exactly the bug being fixed.
func newCloseRecordingServer(t *testing.T) (*httptest.Server, <-chan int) {
	t.Helper()
	codeCh := make(chan int, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				var closeErr *websocket.CloseError
				if errors.As(err, &closeErr) {
					select {
					case codeCh <- closeErr.Code:
					default:
					}
				}
				return
			}
		}
	}))
	return srv, codeCh
}

// The two peers hold opposite roles, so they must not get the same close code.
// SAGE is the server to the client and the client to the relay miner: 1012
// ("service restarting, reconnect") is a server's word, and sending it upstream
// asks the miner to reconnect to us — something it does not do.
func TestBridge_CloseCodeIsAdaptedPerDirection(t *testing.T) {
	endpointSrv, endpointCode := newCloseRecordingServer(t)
	defer endpointSrv.Close()

	ctx, cancel := context.WithCancel(context.Background())

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(ctx, newTestLogger(), r, w,
			wsURL(endpointSrv), nil, &passthroughProcessor{})
		require.NoError(t, err)
		<-b.Done()
	}))
	defer bridgeSrv.Close()

	clientConn := dialTestServer(t, bridgeSrv)
	defer clientConn.Close()

	// A gateway shutdown: ErrBridgeContextCanceled → 1012 facing the client.
	cancel()

	_ = clientConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := clientConn.ReadMessage()
	var clientErr *websocket.CloseError
	require.ErrorAs(t, err, &clientErr, "client should receive a close frame")
	require.Equal(t, websocket.CloseServiceRestart, clientErr.Code,
		"the client is the one that should reconnect, so it keeps 1012")

	select {
	case got := <-endpointCode:
		require.Equal(t, websocket.CloseGoingAway, got,
			"the relay miner must get 1001 Going Away, not a server-role code")
	case <-time.After(3 * time.Second):
		t.Fatal("endpoint never received a close frame")
	}
}

// The table is the contract: server-role codes are rewritten upstream, and
// nothing else is. Application codes carry the miner's own vocabulary (4000 at
// session expiry) and must survive being echoed back to it.
func TestEndpointCloseCode(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"1011 internal error is a server's word", websocket.CloseInternalServerErr, websocket.CloseGoingAway},
		{"1012 service restart is a server's word", websocket.CloseServiceRestart, websocket.CloseGoingAway},
		{"1013 try again later is a server's word", websocket.CloseTryAgainLater, websocket.CloseGoingAway},
		{"1000 normal means the same both ways", websocket.CloseNormalClosure, websocket.CloseNormalClosure},
		{"1001 going away is already correct", websocket.CloseGoingAway, websocket.CloseGoingAway},
		{"1008 policy violation is not role-bound", websocket.ClosePolicyViolation, websocket.ClosePolicyViolation},
		{"4000 session expiry is the miner's own code", 4000, 4000},
		{"3000 application range passes through", 3000, 3000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, endpointCloseCode(tc.in))
		})
	}
}
