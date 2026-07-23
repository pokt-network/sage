package shannon

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pokt-network/sage/domain"
)

func TestEncodeDecodeGRPCFrame_RoundTrip(t *testing.T) {
	msg := []byte("a relay request, pretend it is protobuf")

	frame := encodeGRPCFrame(0, msg)
	if len(frame) != grpcFrameHeaderLen+len(msg) {
		t.Fatalf("frame length = %d, want %d", len(frame), grpcFrameHeaderLen+len(msg))
	}

	got, trailers, err := decodeGRPCWebResponse(frame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(got) != string(msg) {
		t.Errorf("message = %q, want %q", got, msg)
	}
	if trailers != nil {
		t.Errorf("unexpected trailers: %v", trailers)
	}
}

func TestDecodeGRPCWebResponse(t *testing.T) {
	message := []byte("payload")

	t.Run("message followed by trailers", func(t *testing.T) {
		body := append(encodeGRPCFrame(0, message),
			encodeGRPCFrame(grpcTrailerFlag, []byte("grpc-status:0\r\ngrpc-message:\r\n"))...)

		got, trailers, err := decodeGRPCWebResponse(body)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if string(got) != string(message) {
			t.Errorf("message = %q, want %q", got, message)
		}
		if trailers["grpc-status"] != "0" {
			t.Errorf("grpc-status = %q, want 0", trailers["grpc-status"])
		}
	})

	// A failed call often has no message at all — the status is the whole
	// answer. That must decode cleanly rather than read as a truncated body.
	t.Run("trailers only", func(t *testing.T) {
		body := encodeGRPCFrame(grpcTrailerFlag, []byte("grpc-status:5\r\ngrpc-message:not found\r\n"))

		got, trailers, err := decodeGRPCWebResponse(body)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != nil {
			t.Errorf("message = %q, want none", got)
		}
		if trailers["grpc-status"] != "5" || trailers["grpc-message"] != "not found" {
			t.Errorf("trailers = %v", trailers)
		}
	})

	t.Run("truncated frame is an error, not silent truncation", func(t *testing.T) {
		body := encodeGRPCFrame(0, message)[:len(message)] // claims more than it has
		if _, _, err := decodeGRPCWebResponse(body); err == nil {
			t.Error("expected an error for a frame shorter than its declared length")
		}
	})
}

func TestGRPCTarget(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		wantHost string
		wantTLS  bool
		wantErr  bool
	}{
		{"https adds 443", "https://rm.example", "rm.example:443", true, false},
		{"http adds 80", "http://rm.example", "rm.example:80", false, false},
		{"explicit port kept", "https://rm.example:8443", "rm.example:8443", true, false},
		{"bare host:port", "rm.example:9090", "rm.example:9090", false, false},
		{"undialable scheme", "ws://rm.example", "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, useTLS, err := grpcTarget(tt.url)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != tt.wantHost {
				t.Errorf("host = %q, want %q", host, tt.wantHost)
			}
			if useTLS != tt.wantTLS {
				t.Errorf("useTLS = %v, want %v", useTLS, tt.wantTLS)
			}
		})
	}
}

// Falling back to a different protocol is only correct for the one failure that
// means "wrong framing". Treating any error that way would silently reroute a
// genuinely broken supplier and hide the real fault.
func TestIsNotHTTP2(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"505 from the miner", errors.New("unexpected HTTP status code received from server: 505 (HTTP Version Not Supported)"), true},
		{"explicit message", errors.New("gRPC requires HTTP/2"), true},
		{"unimplemented", status.Error(codes.Unimplemented, "unknown service"), true},
		{"connection refused", errors.New("connection refused"), false},
		{"unavailable", status.Error(codes.Unavailable, "backend down"), false},
		{"nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNotHTTP2(tt.err); got != tt.want {
				t.Errorf("isNotHTTP2 = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSendWeb_RoundTrip(t *testing.T) {
	relayReq := []byte("signed-relay-request")
	relayResp := []byte("signed-relay-response")

	var gotPath, gotContentType, gotRPCType string
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		gotRPCType = r.Header.Get("rpc-type")
		gotBody, _ = io.ReadAll(r.Body)

		w.Header().Set("Content-Type", grpcWebContentType)
		_, _ = w.Write(append(encodeGRPCFrame(0, relayResp),
			encodeGRPCFrame(grpcTrailerFlag, []byte("grpc-status:0\r\n"))...))
	}))
	defer server.Close()

	tr := newGRPCRelayTransport(GRPCModeWeb, server.Client(), nil)
	got, err := tr.send(context.Background(), server.URL, relayReq, domain.RPCTypeGRPC)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if string(got) != string(relayResp) {
		t.Errorf("response = %q, want %q", got, relayResp)
	}
	if gotPath != relayServiceMethod {
		t.Errorf("path = %q, want %q", gotPath, relayServiceMethod)
	}
	if gotContentType != grpcWebContentType {
		t.Errorf("content-type = %q, want %q", gotContentType, grpcWebContentType)
	}
	// The miner picks the backend from this; without it the relay lands on the
	// service's default backend rather than its gRPC one.
	if gotRPCType == "" {
		t.Error("rpc-type header was not sent")
	}
	if _, _, err := decodeGRPCWebResponse(gotBody); err != nil {
		t.Errorf("request body was not a valid gRPC frame: %v", err)
	}
}

// A non-zero grpc-status is the call failing, even though the HTTP hop is 200.
// Returning the message unchecked would hand a caller an error page as if it
// were a signed relay response.
func TestSendWeb_NonZeroStatusIsAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", grpcWebContentType)
		_, _ = w.Write(encodeGRPCFrame(grpcTrailerFlag,
			[]byte("grpc-status:13\r\ngrpc-message:backend exploded\r\n")))
	}))
	defer server.Close()

	tr := newGRPCRelayTransport(GRPCModeWeb, server.Client(), nil)
	_, err := tr.send(context.Background(), server.URL, []byte("req"), domain.RPCTypeGRPC)
	if err == nil {
		t.Fatal("expected an error for grpc-status 13")
	}
	if !strings.Contains(err.Error(), "backend exploded") {
		t.Errorf("error should carry the supplier's message, got: %v", err)
	}
}

// Auto mode has to survive a front door that cannot carry HTTP/2, which is what
// an ingress terminating h2 and forwarding HTTP/1.1 looks like.
func TestSend_AutoFallsBackToWeb(t *testing.T) {
	relayResp := []byte("signed-relay-response")

	var webCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Count only real gRPC-Web relays. The native attempt also reaches this
		// handler once — an HTTP/1.1 server parses grpc-go's HTTP/2 preface as
		// a garbage request — and that is the failure being tested, not a relay.
		if r.Header.Get("Content-Type") != grpcWebContentType {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		webCalls++
		w.Header().Set("Content-Type", grpcWebContentType)
		_, _ = w.Write(append(encodeGRPCFrame(0, relayResp),
			encodeGRPCFrame(grpcTrailerFlag, []byte("grpc-status:0\r\n"))...))
	}))
	defer server.Close()

	tr := newGRPCRelayTransport(GRPCModeAuto, server.Client(), nil)

	for i := 0; i < 3; i++ {
		got, err := tr.send(context.Background(), server.URL, []byte("req"), domain.RPCTypeGRPC)
		if err != nil {
			t.Fatalf("relay %d: %v", i, err)
		}
		if string(got) != string(relayResp) {
			t.Errorf("relay %d: response = %q", i, got)
		}
	}

	if webCalls != 3 {
		t.Errorf("gRPC-Web served %d relays, want 3", webCalls)
	}

	// The point of caching: the host is only probed for HTTP/2 once.
	host, _, _ := grpcTarget(server.URL)
	if _, cached := tr.webOnly.Load(host); !cached {
		t.Error("the host was not remembered as gRPC-Web only, so every relay re-probes it")
	}
}

// poktroll's staking validator tells operators "expected http, https, or grpc"
// without enforcing it, so refusing grpc:// would reject the first operator who
// follows the chain's own instructions. Schemes are also case-insensitive.
func TestGRPCTarget_SchemesAndCasing(t *testing.T) {
	tests := []struct {
		url      string
		wantHost string
		wantTLS  bool
	}{
		{"grpc://rm.example:9090", "rm.example:9090", false},
		{"grpcs://rm.example", "rm.example:443", true},
		{"HTTPS://rm.example", "rm.example:443", true},
		{"HtTp://rm.example", "rm.example:80", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			host, useTLS, err := grpcTarget(tt.url)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != tt.wantHost || useTLS != tt.wantTLS {
				t.Errorf("= %q/%v, want %q/%v", host, useTLS, tt.wantHost, tt.wantTLS)
			}
		})
	}
}

// An empty URL would otherwise become ":80" and get dialled, failing far from
// the cause.
func TestGRPCTarget_EmptyURLIsRejected(t *testing.T) {
	if _, _, err := grpcTarget(""); err == nil {
		t.Error("expected an error for an empty supplier URL")
	}
}

// The fallback answer is memoized for the process lifetime, so the marker it
// keys on has to be unambiguous. A bare preface error is not: a transient reset
// or a connection dropped mid-handshake produces the same prefix, and reading
// that as a framing mismatch pins a healthy supplier to gRPC-Web forever.
func TestIsNotHTTP2_IgnoresAmbiguousPrefaceErrors(t *testing.T) {
	ambiguous := errors.New(`connection error: desc = "error reading server preface: EOF"`)
	if isNotHTTP2(ambiguous) {
		t.Error("a bare preface error is transient-ambiguous and must not pin the host to gRPC-Web")
	}

	// The real HTTP/1.1 front door appends the clause that is unambiguous, so
	// dropping the broad prefix costs no coverage.
	real := errors.New(`connection error: desc = "error reading server preface: http2: failed reading the frame payload: ` +
		`http2: frame too large, note that the frame header looked like an HTTP/1.1 header"`)
	if !isNotHTTP2(real) {
		t.Error("a genuine HTTP/1.1 front door must still be detected")
	}
}
