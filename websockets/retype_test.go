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

// countingBinaryJSON is passthroughProcessor that counts
// EndpointSentBinaryJSON.
type countingBinaryJSON struct {
	passthroughProcessor
	n atomic.Int32
}

func (c *countingBinaryJSON) EndpointSentBinaryJSON() { c.n.Add(1) }

// flipServer answers every frame with its bytes in the other frame type: the
// supplier whose framing disagrees with the client's.
func flipServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			flipped := websocket.TextMessage
			if mt == websocket.TextMessage {
				flipped = websocket.BinaryMessage
			}
			if err := conn.WriteMessage(flipped, msg); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// The client is answered in the frame type it asks in, whatever type the
// supplier framed the answer in; only bytes that are not text stay binary.
// A JSON answer the supplier framed as binary is counted either way, which is
// what names the supplier.
func TestBridge_AnswersInTheClientsFrameType(t *testing.T) {
	proc := &countingBinaryJSON{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := StartBridge(context.Background(), newTestLogger(), r, w, wsURL(flipServer(t)), nil, proc)
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
		// A text client, a supplier that answers binary.
		{"binary JSON to a text client is text", websocket.TextMessage, "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x4c4f117\"}\n", websocket.TextMessage},
		{"binary JSON array to a text client is text", websocket.TextMessage, `[{"jsonrpc":"2.0","id":2,"result":"0x1"}]`, websocket.TextMessage},
		{"binary bytes that are not text stay binary", websocket.TextMessage, "{\xff\xfe}", websocket.BinaryMessage},
		// A binary client, a supplier that answers text.
		{"text JSON to a binary client is binary", websocket.BinaryMessage, `{"jsonrpc":"2.0","id":3,"result":"0x1"}`, websocket.BinaryMessage},
	} {
		require.NoError(t, client.WriteMessage(tc.sent, []byte(tc.data)), tc.name)
		_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
		gotType, got, err := client.ReadMessage()
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.wantType, gotType, tc.name)
		require.Equal(t, tc.data, string(got), "%s: the bytes are not touched", tc.name)
	}
	require.Equal(t, int32(2), proc.n.Load(), "the two JSON answers the supplier framed as binary are counted")
}

// Before the client has said anything, and for what the gateway itself owes
// it, the frame type is text; after, it is the client's.
func TestBridge_ClientFrameTypeDefaultsToText(t *testing.T) {
	var b Bridge
	require.Equal(t, websocket.TextMessage, b.clientFrameType())
	b.clientType.Store(websocket.BinaryMessage)
	require.Equal(t, websocket.BinaryMessage, b.clientFrameType())
	b.clientType.Store(websocket.TextMessage)
	require.Equal(t, websocket.TextMessage, b.clientFrameType())
}
