package middleware

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// bodyReader is an io.ReadCloser whose length need not match the
// Content-Length the request declares.
type bodyReader struct{ *bytes.Reader }

func (bodyReader) Close() error { return nil }

func newRequestWithBody(t *testing.T, body []byte, declaredLength int64) *http.Request {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/v1", bodyReader{bytes.NewReader(body)})
	req.ContentLength = declaredLength
	return req
}

func TestReadBody_ContentLengthCases(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)

	tests := []struct {
		name     string
		body     []byte
		declared int64
		want     []byte
	}{
		{"exact", body, int64(len(body)), body},
		{"absent (chunked)", body, -1, body},
		{"zero", body, 0, body},
		{"understated: body outran its header", body, 10, body},
		{"overstated: body shorter than declared", body, int64(len(body)) + 50, body},
		{"empty body, declared", nil, 0, nil},
		{"over the cap falls back to the buffer path", body, testBodyCap + 1, body},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := newRequestWithBody(t, tc.body, tc.declared)

			got, err := readBody(req, testBodyCap)
			if err != nil {
				t.Fatalf("readBody: %v", err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("body = %q, want %q", got, tc.want)
			}

			// The body must still be readable by everything downstream.
			again, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("re-read: %v", err)
			}
			if !bytes.Equal(again, tc.want) {
				t.Errorf("re-read body = %q, want %q", again, tc.want)
			}
		})
	}
}

// testBodyCap is a small cap so the over-the-cap cases stay cheap.
const testBodyCap = 1 << 12

func TestReadBody_RejectsOversizedBody(t *testing.T) {
	oversized := bytes.Repeat([]byte("x"), testBodyCap+1)

	// Declared truthfully (so the cap check rejects it before sizing) and
	// understated (so the read path discovers the overrun itself).
	for _, declared := range []int64{int64(len(oversized)), 1} {
		req := newRequestWithBody(t, oversized, declared)
		if _, err := readBody(req, testBodyCap); !errors.Is(err, errBodyTooLarge) {
			t.Errorf("declared=%d: err = %v, want errBodyTooLarge", declared, err)
		}
	}
}

func TestReadBody_AtExactlyTheCap(t *testing.T) {
	atCap := bytes.Repeat([]byte("x"), testBodyCap)

	req := newRequestWithBody(t, atCap, int64(len(atCap)))
	got, err := readBody(req, testBodyCap)
	if err != nil {
		t.Fatalf("a body exactly at the cap must be accepted: %v", err)
	}
	if len(got) != testBodyCap {
		t.Errorf("len = %d, want %d", len(got), testBodyCap)
	}
}

// BenchmarkReadBody measures the per-relay cost for a typical JSON-RPC request.
func BenchmarkReadBody(b *testing.B) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1", bodyReader{bytes.NewReader(body)})
		req.ContentLength = int64(len(body))
		if _, err := readBody(req, DefaultMaxBodyBytes); err != nil {
			b.Fatal(err)
		}
	}
}
