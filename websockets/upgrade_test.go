package websockets

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Suppliers stake WebSocket endpoints as https:// (the largest mainnet WS
// operator stakes every one that way); the dialer must treat the scheme as
// the TLS marker it is, not reject the URL unopened.
func TestConnectEndpoint_AcceptsHTTPScheme(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		// Echo one message so the client side can prove the socket works.
		mt, msg, err := c.ReadMessage()
		if err != nil {
			return
		}
		_ = c.WriteMessage(mt, msg)
	}))
	defer srv.Close()

	// srv.URL is http://…; before the scheme rewrite this failed with
	// gorilla's "malformed ws or wss URL" without sending a packet.
	conn, err := ConnectEndpoint(discardLogger(), srv.URL, nil)
	if err != nil {
		t.Fatalf("ConnectEndpoint(%q): %v", srv.URL, err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(msg) != "ping" {
		t.Fatalf("echo = %q, want %q", msg, "ping")
	}
}

// https must map to wss, never ws: the rewrite may not downgrade a
// TLS-staked endpoint to cleartext. A TLS handshake against a plain HTTP
// server fails — which is exactly what proves wss was attempted.
func TestConnectEndpoint_HTTPSDialsTLS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	httpsURL := "https" + strings.TrimPrefix(srv.URL, "http")
	_, err := ConnectEndpoint(discardLogger(), httpsURL, nil)
	if err == nil {
		t.Fatal("expected a TLS failure against a cleartext server")
	}
	if strings.Contains(err.Error(), "malformed ws or wss URL") {
		t.Fatalf("scheme was not rewritten: %v", err)
	}
}
