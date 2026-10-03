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

// newReviewBurstServer sends n frames the moment a connection opens, then echoes.
func newReviewBurstServer(t *testing.T, n int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for range n {
			if conn.WriteMessage(websocket.TextMessage, []byte("bad")) != nil {
				return
			}
		}
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if conn.WriteMessage(mt, msg) != nil {
				return
			}
		}
	}))
}

// Evidence for a clean area, expected to PASS: a burst of endpoint frames
// that all fail processing spawns one endpointGone goroutine each, but only
// the first rebinds (the rest find the endpoint replaced), so the burst
// spends one loss, not one per frame.
func TestReview_BridgeProcessingBurstSpendsOneLoss(t *testing.T) {
	first := newReviewBurstServer(t, 20)
	defer first.Close()
	second, _ := newRecordingEchoServer(t)
	defer second.Close()

	var mu sync.Mutex
	var causes []error
	handler := func(_ context.Context, cause error) (*websocket.Conn, MessageProcessor, [][]byte, error) {
		mu.Lock()
		causes = append(causes, cause)
		mu.Unlock()
		time.Sleep(100 * time.Millisecond) // a slow dial widens the window
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

	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(causes) >= 1 },
		3*time.Second, 10*time.Millisecond)
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("x")))
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, got, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "2:x", string(got))

	mu.Lock()
	n := len(causes)
	mu.Unlock()
	require.Equal(t, 1, n, "one burst of failing frames must rebind once")
	b.endpointMu.Lock()
	losses := len(b.losses)
	b.endpointMu.Unlock()
	require.Equal(t, 1, losses, "one burst must spend one loss")

	// Shutdown with endpointGone goroutines possibly still parked: no hang.
	done := make(chan struct{})
	go func() { b.Shutdown(nil); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown hung")
	}
}
