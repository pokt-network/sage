package router

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/websockets"
)

type audit2Passthrough struct{}

func (audit2Passthrough) ProcessClientMessage(b []byte) ([]byte, error)   { return b, nil }
func (audit2Passthrough) ProcessEndpointMessage(b []byte) ([]byte, error) { return b, nil }

// audit2BridgeOpener opens a bridge the way shannon.WSRelayer.Open does —
// StartBridge on the request's context, then block until it is done — minus
// session and endpoint resolution.
type audit2BridgeOpener struct{ endpointURL string }

func (o audit2BridgeOpener) Open(ctx context.Context, _ domain.ServiceID, req *http.Request, w http.ResponseWriter) error {
	b, err := websockets.StartBridge(ctx, discardLogger(), req, w, o.endpointURL, http.Header{}, audit2Passthrough{})
	if err != nil {
		return err
	}
	<-b.Done()
	return nil
}

// WS-6. docs/operations.md promises that on graceful shutdown "WebSocket
// clients get 1012 and reconnect". http.Server.Shutdown does not track
// hijacked connections and cancels nothing a bridge waits on, so a live bridge
// outlives Router.Shutdown and the client learns of the restart only when the
// process exits and the socket drops (1006).
func TestAudit2_ShutdownClosesWebSocketsWith1012(t *testing.T) {
	supplier := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
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
	defer supplier.Close()

	r := New(config.RouterConfig{}, relay.Noop, &mockSessions{ready: true},
		audit2BridgeOpener{endpointURL: "ws" + strings.TrimPrefix(supplier.URL, "http")}, discardLogger())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = r.server.Serve(ln)
	}()

	client, _, err := websocket.DefaultDialer.Dial("ws://"+ln.Addr().String()+"/v1",
		http.Header{"Target-Service-Id": []string{"eth"}})
	if err != nil {
		t.Fatalf("precondition: dial through the router: %v", err)
	}
	defer client.Close()
	// Prove the bridge is live end to end before shutting down.
	if err := client.WriteMessage(websocket.TextMessage, []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, msg, err := client.ReadMessage(); err != nil || string(msg) != `{"id":1}` {
		t.Fatalf("precondition: echo through the bridge = %q, %v", msg, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	<-served

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = client.ReadMessage()
	var ce *websocket.CloseError
	if !errors.As(err, &ce) || ce.Code != websocket.CloseServiceRestart {
		t.Fatalf("after Router.Shutdown the WebSocket client read %v; want a close frame with code 1012", err)
	}
}
