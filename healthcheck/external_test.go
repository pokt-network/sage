package healthcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// --- parseHeightFromBytes ---

func TestParseHeightFromBytes_HexString(t *testing.T) {
	resp := `{"jsonrpc":"2.0","id":1,"result":"0x1388"}`
	h, err := parseHeightFromBytes([]byte(resp))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != 0x1388 {
		t.Errorf("expected 0x1388=5000, got %d", h)
	}
}

func TestParseHeightFromBytes_DecimalResult(t *testing.T) {
	resp := `{"jsonrpc":"2.0","id":1,"result":12345}`
	h, err := parseHeightFromBytes([]byte(resp))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != 12345 {
		t.Errorf("expected 12345, got %d", h)
	}
}

func TestParseHeightFromBytes_CometBFTNested(t *testing.T) {
	resp := `{"jsonrpc":"2.0","result":{"sync_info":{"latest_block_height":"99999"}}}`
	h, err := parseHeightFromBytes([]byte(resp))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != 99999 {
		t.Errorf("expected 99999, got %d", h)
	}
}

func TestParseHeightFromBytes_CosmosRESTBlock(t *testing.T) {
	resp := `{"block":{"header":{"height":"888"}}}`
	h, err := parseHeightFromBytes([]byte(resp))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != 888 {
		t.Errorf("expected 888, got %d", h)
	}
}

func TestParseHeightFromBytes_EmptyError(t *testing.T) {
	_, err := parseHeightFromBytes(nil)
	if err == nil {
		t.Error("expected error for empty input")
	}
}

func TestParseHeightString_HexAndDecimal(t *testing.T) {
	cases := []struct {
		input string
		want  uint64
	}{
		{"0x1", 1},
		{"0xFF", 255},
		{"100", 100},
		{"0", 0},
	}
	for _, tc := range cases {
		h, err := parseHeightString(tc.input)
		if err != nil {
			t.Errorf("parseHeightString(%q): unexpected error %v", tc.input, err)
			continue
		}
		if h != tc.want {
			t.Errorf("parseHeightString(%q): got %d, want %d", tc.input, h, tc.want)
		}
	}
}

// --- ExternalBlockFetcher with mock HTTP ---

func TestExternalBlockFetcher_JSONRPC(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{"jsonrpc": "2.0", "id": 1, "result": "0x64"} // 100
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	sources := []config.ExternalBlockSource{
		{
			URL:      server.URL,
			Type:     "json_rpc",
			Interval: 100 * time.Millisecond,
		},
	}
	fetcher := NewExternalBlockFetcher(domain.ServiceID("eth"), sources, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := fetcher.Start(ctx)
	select {
	case ebh := <-ch:
		if ebh.Height != 100 {
			t.Errorf("expected height 100, got %d", ebh.Height)
		}
		if ebh.ServiceID != "eth" {
			t.Errorf("expected serviceID eth, got %q", ebh.ServiceID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for external block height")
	}
}

func TestExternalBlockFetcher_REST(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"block": map[string]any{
				"header": map[string]any{"height": "42"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	sources := []config.ExternalBlockSource{
		{
			URL:  server.URL,
			Type: "rest",
			Path: "/block",
		},
	}
	fetcher := NewExternalBlockFetcher(domain.ServiceID("cosmos"), sources, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := fetcher.Start(ctx)
	select {
	case ebh := <-ch:
		if ebh.Height != 42 {
			t.Errorf("expected height 42, got %d", ebh.Height)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for external block height")
	}
}

func TestExternalBlockFetcher_MaxSelection(t *testing.T) {
	// Two servers returning different heights; fetchMax should return the larger.
	makeServer := func(height string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": height})
		}))
	}
	s1 := makeServer("0x64")  // 100
	s2 := makeServer("0x12C") // 300
	defer s1.Close()
	defer s2.Close()

	sources := []config.ExternalBlockSource{
		{URL: s1.URL, Type: "json_rpc"},
		{URL: s2.URL, Type: "json_rpc"},
	}
	fetcher := NewExternalBlockFetcher(domain.ServiceID("eth"), sources, slog.Default())
	h, err := fetcher.fetchMax(context.Background())
	if err != nil {
		t.Fatalf("fetchMax error: %v", err)
	}
	if h != 300 {
		t.Errorf("expected max=300, got %d", h)
	}
}

func TestExternalBlockFetcher_NoSources(t *testing.T) {
	fetcher := NewExternalBlockFetcher(domain.ServiceID("eth"), nil, slog.Default())
	_, err := fetcher.fetchMax(context.Background())
	if err == nil {
		t.Error("expected error with no sources")
	}
}

func TestExternalBlockFetcher_ChannelClosedOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sources := []config.ExternalBlockSource{
		{URL: "http://127.0.0.1:1", Type: "json_rpc", Interval: 10 * time.Second},
	}
	fetcher := NewExternalBlockFetcher(domain.ServiceID("eth"), sources, slog.Default())
	ch := fetcher.Start(ctx)
	cancel()

	// Channel must close after cancel.
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case _, ok := <-ch:
		if ok {
			// Drain any values that arrived before cancel.
			for range ch {
			}
		}
	case <-timer.C:
		t.Error("channel not closed after context cancel")
	}
}

// Solana's getEpochInfo answers {"result":{"absoluteSlot":…,"blockHeight":…}};
// the plugin reads blockHeight and so must the fetcher. On the 2026-09-13
// canary a valid reply was logged as "cannot find block height in JSON".
func TestParseHeightFromBytes_SolanaEpochInfo(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","result":{"absoluteSlot":370000000,"blockHeight":358000000,"epoch":1034,"slotIndex":12,"slotsInEpoch":432000},"id":1}`)
	h, err := parseHeightFromBytes(body)
	if err != nil {
		t.Fatal(err)
	}
	if h != 358000000 {
		t.Fatalf("height = %d, want blockHeight 358000000", h)
	}
}

// A non-2xx answer is reported by its status, not by what the parser could
// not make of the body: trongrid's HTML "405 Not Allowed" page read as
// "cannot find block height in JSON: <html>…" until 2026-09-13.
func TestExternalBlockFetcher_NonSuccessStatusIsNamed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte("<html><body><h1>405 Not Allowed</h1></body></html>"))
	}))
	defer server.Close()

	for _, typ := range []string{"json_rpc", "rest"} {
		f := NewExternalBlockFetcher("tron", []config.ExternalBlockSource{{URL: server.URL, Type: typ}}, slog.Default())
		_, err := f.fetchOne(context.Background(), f.sources[0])
		if err == nil || !strings.Contains(err.Error(), "HTTP 405") {
			t.Fatalf("%s: err = %v, want the status named", typ, err)
		}
	}
}

type countingFailures struct{ n int }

func (c *countingFailures) RecordExternalSourceFailure(domain.ServiceID) { c.n++ }

// A failing source is counted on every poll and logged on the change of
// state only: once when the sources stop answering, once when they answer
// again. Between those, nothing at warn.
func TestExternalBlockFetcher_FailureCountedAndStateChangeLogged(t *testing.T) {
	var ok atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !ok.Load() {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("error code: 502"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x64"}`))
	}))
	defer server.Close()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	f := NewExternalBlockFetcher("bitway", []config.ExternalBlockSource{{URL: server.URL, Type: "json_rpc"}}, logger)
	counter := &countingFailures{}
	f.SetFailureRecorder(counter)
	ch := make(chan ExternalBlockHeight, 8)

	f.emit(context.Background(), ch)
	f.emit(context.Background(), ch)
	f.emit(context.Background(), ch)
	if counter.n != 3 {
		t.Fatalf("failures counted = %d, want 3, one per poll", counter.n)
	}
	if got := strings.Count(logs.String(), "external block sources failing"); got != 1 {
		t.Fatalf("failing warned %d times over three failed polls, want once:\n%s", got, logs.String())
	}
	if !f.failing {
		t.Fatal("fetcher should be in the failing state")
	}

	ok.Store(true)
	f.emit(context.Background(), ch)
	select {
	case h := <-ch:
		if h.Height != 100 {
			t.Fatalf("height = %d, want 100 after recovery", h.Height)
		}
	default:
		t.Fatal("no height emitted after the source recovered")
	}
	if got := strings.Count(logs.String(), "external block sources recovered"); got != 1 {
		t.Fatalf("recovered logged %d times, want once:\n%s", got, logs.String())
	}
	if f.failing {
		t.Fatal("fetcher should have left the failing state")
	}
	if counter.n != 3 {
		t.Fatalf("a successful poll must not count as a failure, got %d", counter.n)
	}
}
