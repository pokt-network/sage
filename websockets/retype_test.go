package websockets

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// countingRetype is passthroughProcessor that counts RetypedToText.
type countingRetype struct {
	passthroughProcessor
	n atomic.Int32
}

func (c *countingRetype) RetypedToText() { c.n.Add(1) }

// A JSON answer that arrives from the endpoint as a binary frame goes to the
// client as text, and is counted; text stays text, and a binary frame that is
// not JSON stays binary, uncounted.
func TestBridge_BinaryJSONGoesToTheClientAsText(t *testing.T) {
	endpoint := newEchoServer(t) // echoes each frame with the type it read
	defer endpoint.Close()
	proc := &countingRetype{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w, wsURL(endpoint), nil, proc)
		if err != nil {
			return
		}
		<-b.Done()
	}))
	defer srv.Close()
	client := dialTestServer(t, srv)
	defer client.Close()

	for _, tc := range []struct {
		name     string
		sent     int
		data     string
		wantType int
	}{
		{"binary JSON becomes text", websocket.BinaryMessage, " {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x4c4f117\"}\n", websocket.TextMessage},
		{"binary JSON array becomes text", websocket.BinaryMessage, `[{"jsonrpc":"2.0","id":2,"result":"0x1"}]`, websocket.TextMessage},
		{"text stays text", websocket.TextMessage, `{"jsonrpc":"2.0","id":3,"result":"0x1"}`, websocket.TextMessage},
		{"binary that is not JSON stays binary", websocket.BinaryMessage, "\x00\x01\x02", websocket.BinaryMessage},
		{"binary that is not UTF-8 stays binary", websocket.BinaryMessage, "{\xff\xfe}", websocket.BinaryMessage},
	} {
		require.NoError(t, client.WriteMessage(tc.sent, []byte(tc.data)), tc.name)
		_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
		gotType, got, err := client.ReadMessage()
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.wantType, gotType, tc.name)
		require.Equal(t, tc.data, string(got), "%s: the bytes are not touched", tc.name)
	}
	require.Equal(t, int32(2), proc.n.Load(), "only the two binary JSON answers are counted")
}
