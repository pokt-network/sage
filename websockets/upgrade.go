package websockets

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/gorilla/websocket"
)

var clientUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// Allow all origins; callers are responsible for authentication.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// UpgradeClient upgrades an incoming HTTP request to a WebSocket connection.
// Returns ErrBridgeClientUpgradeFailed (wrapped) on failure.
func UpgradeClient(logger *slog.Logger, r *http.Request, w http.ResponseWriter) (*websocket.Conn, error) {
	conn, err := clientUpgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("websocket: client upgrade failed", "err", err)
		return nil, fmt.Errorf("%w: %w", ErrBridgeClientUpgradeFailed, err)
	}
	return conn, nil
}

// ConnectEndpoint dials a WebSocket endpoint and returns the connection.
// Returns ErrBridgeEndpointUnavailable (wrapped) on failure.
//
// An http or https scheme is dialed as ws or wss. Suppliers stake WebSocket
// endpoints with https:// URLs — on mainnet the largest WS operator stakes
// every one that way, and PATH accepts them — so the scheme on a staked URL
// is a security marker (TLS or not), not a transport. Handing https:// to the
// dialer as-is fails with gorilla's "malformed ws or wss URL" before a single
// packet is sent, which on 2026-09-01 was 78% of the canary's WS pool failing
// with close 1011. The rewrite never downgrades: https becomes wss, never ws.
func ConnectEndpoint(logger *slog.Logger, rawURL string, headers http.Header) (*websocket.Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		logger.Error("websocket: invalid endpoint URL", "url", rawURL, "err", err)
		return nil, fmt.Errorf("%w: invalid URL %q: %w", ErrBridgeEndpointUnavailable, rawURL, err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}

	dialer := websocket.Dialer{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
	}

	conn, _, err := dialer.Dial(u.String(), headers)
	if err != nil {
		logger.Error("websocket: endpoint connection failed", "url", u.String(), "err", err)
		return nil, fmt.Errorf("%w: dial %q: %w", ErrBridgeEndpointUnavailable, u.String(), err)
	}
	return conn, nil
}
