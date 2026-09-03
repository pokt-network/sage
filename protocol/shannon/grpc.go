package shannon

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pokt-network/sage/domain"
)

// relayServiceMethod is the relay miner's gRPC entry point. A gRPC relay is a
// gRPC call carrying the signed RelayRequest as its message, not an HTTP POST
// of that message — the miner routes the two down different paths, and only
// this one reaches the h2c client it uses for gRPC backends.
const relayServiceMethod = "/pocket.service.RelayService/SendRelay"

// gRPC transport modes, settable per deployment (config: protocol.grpc_mode).
const (
	// GRPCModeAuto tries native gRPC once per supplier host and remembers the
	// answer, falling back to gRPC-Web when the host cannot carry HTTP/2. It is
	// the zero value because it is the only setting that is correct in both
	// deployments SAGE actually runs in.
	GRPCModeAuto = ""
	// GRPCModeNative forces native gRPC (HTTP/2). Right when SAGE sits next to
	// the relay miners, where h2c reaches them directly.
	GRPCModeNative = "native"
	// GRPCModeWeb forces gRPC-Web over HTTP/1.1. Right when SAGE reaches
	// suppliers through an ingress that terminates HTTP/2 and forwards HTTP/1.1
	// — the miner answers "gRPC requires HTTP/2" to native calls in that case.
	GRPCModeWeb = "web"
)

// grpcWebContentType is the framing SAGE speaks to the relay miner in web mode.
// "+proto" because the message is a protobuf RelayRequest; the base64 "-text"
// variant exists for browsers that cannot handle binary bodies, which is not a
// problem a gateway has.
const grpcWebContentType = "application/grpc-web+proto"

// grpcFrameHeaderLen is the length-prefixed message header both gRPC and
// gRPC-Web use: one flag byte, then a big-endian uint32 length.
const grpcFrameHeaderLen = 5

// grpcTrailerFlag marks a gRPC-Web trailer frame. gRPC-Web has no HTTP
// trailers, so it appends them as a final frame in the body instead — which is
// exactly why it survives an HTTP/1.1 hop that would drop real trailers.
const grpcTrailerFlag = 0x80

// grpcRelayTransport sends signed relay requests to a supplier's relay miner
// over gRPC, in whichever framing that supplier's front door accepts.
type grpcRelayTransport struct {
	mode       string
	httpClient *http.Client
	logger     *slog.Logger

	// conns caches one *grpc.ClientConn per supplier host. A ClientConn is a
	// long-lived multiplexed connection; building one per relay would pay a TLS
	// handshake every time and defeat HTTP/2 entirely.
	//
	// Bounded by idle eviction, not by count. The set of hosts is small, but
	// each entry is a live connection and a supplier that leaves the network
	// never comes back — so without this the process holds an open socket per
	// gRPC host it has ever relayed to, for its whole life. Recorded as a
	// residual by the ever-seen-maps audit on 2026-09-01, after the reputation
	// timeline was OOMKilled for the same shape of mistake.
	conns sync.Map // host (string) → *grpcConn

	// lastSweep is when idle connections were last closed, so the sweep runs
	// at most once per interval however many relays arrive. Unix nanoseconds.
	lastSweep atomic.Int64

	// webOnly records hosts that answered a native attempt with "not HTTP/2".
	// Without it, auto mode would re-learn the same fact on every single relay.
	webOnly sync.Map // host (string) → struct{}
}

// grpcConn is a cached connection and when it was last used.
type grpcConn struct {
	conn *grpc.ClientConn
	// lastUsed is Unix nanoseconds, written on every send. Atomic because
	// relays to one host run concurrently.
	lastUsed atomic.Int64
}

func (c *grpcConn) touch(now time.Time) { c.lastUsed.Store(now.UnixNano()) }

const (
	// grpcConnIdleTTL is how long an unused connection is kept. Long enough
	// that a service probed once per health-check cycle keeps its connection
	// across cycles, short enough that a supplier which has left the session
	// stops costing a socket within the hour.
	grpcConnIdleTTL = 30 * time.Minute
	// grpcConnSweepInterval bounds how often the sweep walks the map. The walk
	// is O(hosts) and hosts are few, but it runs from the relay path and there
	// is no reason to pay it more than once a minute.
	grpcConnSweepInterval = time.Minute
)

func newGRPCRelayTransport(mode string, httpClient *http.Client, logger *slog.Logger) *grpcRelayTransport {
	return &grpcRelayTransport{mode: mode, httpClient: httpClient, logger: logger}
}

// sweepIdleConns closes connections unused for grpcConnIdleTTL.
//
// Lazy rather than a background goroutine on purpose: a goroutine would need a
// lifecycle — somewhere to be started, somewhere to be stopped — and the
// transport has neither, so it would either leak or need one invented for it.
// Sweeping from the dial path costs a map walk at most once a minute and only
// while relays are flowing, which is exactly when the map can grow.
//
// A connection is removed from the map before it is closed, so a concurrent
// caller either finds it and uses it (holding a reference the close cannot
// invalidate mid-call — gRPC drains in-flight RPCs) or misses it and dials a
// fresh one.
func (t *grpcRelayTransport) sweepIdleConns(now time.Time) {
	last := t.lastSweep.Load()
	if now.UnixNano()-last < int64(grpcConnSweepInterval) {
		return
	}
	if !t.lastSweep.CompareAndSwap(last, now.UnixNano()) {
		// Another relay is sweeping; one walk is enough.
		return
	}

	cutoff := now.Add(-grpcConnIdleTTL).UnixNano()
	t.conns.Range(func(key, value any) bool {
		c := value.(*grpcConn)
		if c.lastUsed.Load() > cutoff {
			return true
		}
		t.conns.Delete(key)
		if err := c.conn.Close(); err != nil && t.logger != nil {
			t.logger.Debug("gRPC: closing idle connection", "host", key, "error", err)
		}
		return true
	})
}

// send delivers relayReqBz to the supplier and returns the raw RelayResponse
// bytes, which the caller still has to signature-check. Returning bytes rather
// than a decoded message is deliberate: the supplier's signature covers what it
// sent, so re-encoding a decoded message before validating would be checking a
// signature against something the supplier never actually produced.
func (t *grpcRelayTransport) send(ctx context.Context, supplierURL string, relayReqBz []byte, rpcType domain.RPCType) ([]byte, error) {
	host, useTLS, err := grpcTarget(supplierURL)
	if err != nil {
		return nil, err
	}

	switch t.mode {
	case GRPCModeWeb:
		return t.sendWeb(ctx, supplierURL, relayReqBz, rpcType)
	case GRPCModeNative:
		return t.sendNative(ctx, host, useTLS, relayReqBz, rpcType)
	}

	// Auto: web straight away for a host already known not to speak HTTP/2.
	if _, known := t.webOnly.Load(host); known {
		return t.sendWeb(ctx, supplierURL, relayReqBz, rpcType)
	}

	resp, err := t.sendNative(ctx, host, useTLS, relayReqBz, rpcType)
	if err == nil || !isNotHTTP2(err) {
		return resp, err
	}
	// Only this one failure means "wrong framing for this front door". Anything
	// else is a real error and must not be retried as a different protocol.
	if _, alreadyKnown := t.webOnly.LoadOrStore(host, struct{}{}); !alreadyKnown && t.logger != nil {
		// Logged once per host: which framing a supplier resolved to is the
		// first thing to check when gRPC behaves differently between
		// deployments, and it is invisible otherwise.
		t.logger.Info("gRPC: supplier does not speak HTTP/2, using gRPC-Web for this host",
			"host", host, "error", err)
	}
	return t.sendWeb(ctx, supplierURL, relayReqBz, rpcType)
}

// sendNative performs the relay as a real gRPC call over HTTP/2.
func (t *grpcRelayTransport) sendNative(ctx context.Context, host string, useTLS bool, relayReqBz []byte, rpcType domain.RPCType) ([]byte, error) {
	conn, err := t.conn(host, useTLS)
	if err != nil {
		return nil, err
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "rpc-type", rpcTypeHeaderValue(rpcType))

	var respBz []byte
	// ForceCodec keeps the wire bytes untouched in both directions; the default
	// proto codec would decode the response and cost us the exact bytes the
	// supplier signed.
	if err := conn.Invoke(ctx, relayServiceMethod, &relayReqBz, &respBz, grpc.ForceCodec(rawCodec{})); err != nil {
		return nil, fmt.Errorf("grpc relay: %w", err)
	}
	return respBz, nil
}

// conn returns the cached ClientConn for host, dialing one if needed.
func (t *grpcRelayTransport) conn(host string, useTLS bool) (*grpc.ClientConn, error) {
	now := time.Now()
	if c, ok := t.conns.Load(host); ok {
		cached := c.(*grpcConn)
		cached.touch(now)
		return cached.conn, nil
	}
	t.sweepIdleConns(now)

	creds := insecure.NewCredentials()
	if useTLS {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("grpc relay: dial %s: %w", host, err)
	}

	entry := &grpcConn{conn: conn}
	entry.touch(now)

	// Another goroutine may have won the race; keep theirs and drop ours so the
	// cache never hands out a connection nobody will close.
	actual, loaded := t.conns.LoadOrStore(host, entry)
	if loaded {
		_ = conn.Close()
	}
	cached := actual.(*grpcConn)
	cached.touch(now)
	return cached.conn, nil
}

// sendWeb performs the relay as a gRPC-Web call over ordinary HTTP/1.1.
func (t *grpcRelayTransport) sendWeb(ctx context.Context, supplierURL string, relayReqBz []byte, rpcType domain.RPCType) ([]byte, error) {
	endpoint := strings.TrimSuffix(supplierURL, "/") + relayServiceMethod

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(encodeGRPCFrame(0, relayReqBz))))
	if err != nil {
		return nil, fmt.Errorf("grpc-web relay: build request: %w", err)
	}
	req.Header.Set("Content-Type", grpcWebContentType)
	req.Header.Set("rpc-type", rpcTypeHeaderValue(rpcType))

	resp, err := doTraced(t.httpClient, req)
	if err != nil {
		return nil, fmt.Errorf("grpc-web relay: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("grpc-web relay: read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("grpc-web relay: supplier returned HTTP %d: %s",
			resp.StatusCode, truncate(string(body), 200))
	}

	message, trailers, err := decodeGRPCWebResponse(body)
	if err != nil {
		return nil, fmt.Errorf("grpc-web relay: %w", err)
	}

	// The status may arrive as HTTP headers (trailers-only replies) or as the
	// in-body trailer frame. Both are legal; check the frame first because a
	// reply that carries one is the more specific answer.
	if st, msg := grpcStatusFrom(trailers, resp.Header); st != 0 {
		return nil, fmt.Errorf("grpc-web relay: supplier returned grpc-status %d: %s", st, msg)
	}
	return message, nil
}

// grpcResponseHeaders lifts the gRPC outcome out of a relayed response.
//
// The relay miner folds the backend's HTTP trailers into the headers precisely
// so this survives the relay — grpc-status is the only place a gRPC call says
// whether it worked, and it is not in the body. Returns nil for every other
// transport so the common path allocates nothing.
func grpcResponseHeaders(rpcType domain.RPCType, resp *sdktypes.POKTHTTPResponse) map[string]string {
	if rpcType != domain.RPCTypeGRPC || resp == nil || resp.Header == nil {
		return nil
	}
	var out map[string]string
	for _, key := range []string{"grpc-status", "grpc-message"} {
		for name, header := range resp.Header {
			if !strings.EqualFold(name, key) || len(header.Values) == 0 {
				continue
			}
			if out == nil {
				out = make(map[string]string, 2)
			}
			out[key] = header.Values[0]
		}
	}
	return out
}

// rawCodec hands gRPC the message bytes verbatim in both directions.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("rawCodec: expected *[]byte, got %T", v)
	}
	return *b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawCodec: expected *[]byte, got %T", v)
	}
	// Copy: the buffer belongs to the transport and is reused after this call.
	*b = append([]byte(nil), data...)
	return nil
}

func (rawCodec) Name() string { return "sage-raw-bytes" }

// encodeGRPCFrame wraps a message in the length-prefixed framing shared by
// gRPC and gRPC-Web.
func encodeGRPCFrame(flag byte, msg []byte) []byte {
	out := make([]byte, grpcFrameHeaderLen+len(msg))
	out[0] = flag
	binary.BigEndian.PutUint32(out[1:5], uint32(len(msg)))
	copy(out[grpcFrameHeaderLen:], msg)
	return out
}

// decodeGRPCWebResponse splits a gRPC-Web body into the response message and
// the trailer key/values. Unary calls carry one data frame; a trailers-only
// reply carries none, which is not an error — the status says what happened.
func decodeGRPCWebResponse(body []byte) (message []byte, trailers map[string]string, err error) {
	for off := 0; off < len(body); {
		if off+grpcFrameHeaderLen > len(body) {
			return nil, nil, fmt.Errorf("truncated frame header at byte %d", off)
		}
		flag := body[off]
		size := int(binary.BigEndian.Uint32(body[off+1 : off+grpcFrameHeaderLen]))
		start := off + grpcFrameHeaderLen
		if start+size > len(body) {
			return nil, nil, fmt.Errorf("frame at byte %d claims %d bytes, only %d remain", off, size, len(body)-start)
		}
		payload := body[start : start+size]

		if flag&grpcTrailerFlag != 0 {
			trailers = parseGRPCTrailers(payload)
		} else if message == nil {
			message = payload
		}
		off = start + size
	}
	return message, trailers, nil
}

// parseGRPCTrailers reads the HTTP-header-shaped trailer block gRPC-Web puts in
// its final frame ("grpc-status: 0\r\ngrpc-message: ...\r\n").
func parseGRPCTrailers(payload []byte) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(string(payload), "\r\n") {
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		out[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	return out
}

// grpcStatusFrom resolves the call outcome, preferring the in-body trailers and
// falling back to HTTP headers. A reply with neither is a success: gRPC treats
// an absent grpc-status as OK.
func grpcStatusFrom(trailers map[string]string, header http.Header) (int, string) {
	if raw, ok := trailers["grpc-status"]; ok {
		code, _ := strconv.Atoi(raw)
		return code, trailers["grpc-message"]
	}
	if raw := header.Get("Grpc-Status"); raw != "" {
		code, _ := strconv.Atoi(raw)
		return code, header.Get("Grpc-Message")
	}
	return 0, ""
}

// grpcTarget splits a staked supplier URL into a gRPC dial target and whether
// it is TLS. Suppliers stake ordinary URLs ("https://host"), while gRPC dials
// "host:port", so the scheme has to become a transport decision here.
func grpcTarget(supplierURL string) (host string, useTLS bool, err error) {
	if supplierURL == "" {
		// Otherwise this becomes ":80" and gets dialled, failing far from the
		// cause.
		return "", false, fmt.Errorf("grpc relay: empty supplier URL")
	}

	// A bare "host:port" — the conventional way a gRPC backend is written — has
	// no "//", and url.Parse would read "host" as the scheme and reject it as
	// undialable. Recognise that shape before parsing rather than after.
	if !strings.Contains(supplierURL, "://") {
		return withDefaultPort(supplierURL, false), false, nil
	}

	u, err := url.Parse(supplierURL)
	if err != nil {
		return "", false, fmt.Errorf("grpc relay: bad supplier URL %q: %w", supplierURL, err)
	}
	// Schemes are case-insensitive per RFC 3986, and grpc:// is accepted
	// because poktroll's own staking validator tells operators to use it —
	// "expected http, https, or grpc" — while not enforcing it. Refusing a
	// scheme the chain's own error message recommends would reject the first
	// operator who follows the instructions.
	switch strings.ToLower(u.Scheme) {
	case "https", "grpcs":
		useTLS = true
	case "http", "grpc":
		useTLS = false
	default:
		return "", false, fmt.Errorf("grpc relay: supplier URL %q has undialable scheme %q", supplierURL, u.Scheme)
	}
	if u.Host == "" {
		return "", false, fmt.Errorf("grpc relay: supplier URL %q has no host", supplierURL)
	}
	return withDefaultPort(u.Host, useTLS), useTLS, nil
}

// withDefaultPort supplies the port gRPC dialling requires when the URL omits
// one.
func withDefaultPort(host string, useTLS bool) string {
	if strings.Contains(host, ":") {
		return host
	}
	if useTLS {
		return host + ":443"
	}
	return host + ":80"
}

// isNotHTTP2 reports whether an error means "this front door does not speak
// HTTP/2", which is the one failure worth retrying in a different framing.
//
// The relay miner replies 505 with "gRPC requires HTTP/2" when a native gRPC
// request reaches it over HTTP/1.1 — which is what happens behind an ingress
// that terminates HTTP/2 and forwards HTTP/1.1.
func isNotHTTP2(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unimplemented {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "505") ||
		strings.Contains(msg, "HTTP Version Not Supported") ||
		strings.Contains(msg, "gRPC requires HTTP/2") ||
		// A plain HTTP/1.1 server fails before any status code — grpc-go cannot
		// read the HTTP/2 preface. Match the specific clause, not the bare
		// "error reading server preface" that wraps it: that prefix is also
		// produced by a transient reset or a connection dropped mid-handshake,
		// and this answer is memoized in webOnly for the process lifetime. One
		// unlucky observation would pin a healthy supplier to gRPC-Web forever,
		// which is exactly what keying on a single unambiguous marker prevents.
		strings.Contains(msg, "looked like an HTTP/1.1 header")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
