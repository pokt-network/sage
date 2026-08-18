package healthcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"

	"github.com/pokt-network/sage/internal/safego"
)

// ExternalBlockHeight pairs a service with its latest known block height from
// an external (ground-truth) source.
type ExternalBlockHeight struct {
	ServiceID domain.ServiceID
	Height    uint64
}

// ExternalBlockFetcher periodically queries external block height sources and
// emits results on a channel.
type ExternalBlockFetcher struct {
	serviceID domain.ServiceID
	sources   []config.ExternalBlockSource
	logger    *slog.Logger
	client    *http.Client
}

// NewExternalBlockFetcher constructs a fetcher for the given service's sources.
func NewExternalBlockFetcher(
	serviceID domain.ServiceID,
	sources []config.ExternalBlockSource,
	logger *slog.Logger,
) *ExternalBlockFetcher {
	return &ExternalBlockFetcher{
		serviceID: serviceID,
		sources:   sources,
		logger:    logger,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Start launches a background poll loop and returns the output channel. The
// channel is closed when ctx is cancelled.
func (f *ExternalBlockFetcher) Start(ctx context.Context) <-chan ExternalBlockHeight {
	ch := make(chan ExternalBlockHeight, 8)

	// Determine the shortest non-zero interval across all sources; default 30s.
	interval := 30 * time.Second
	for _, s := range f.sources {
		if s.Interval > 0 && s.Interval < interval {
			interval = s.Interval
		}
	}

	go func() {
		defer safego.Recover(f.logger, "healthcheck.external.poll")
		defer close(ch)
		// First fetch immediately.
		f.emit(ctx, ch)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				f.emit(ctx, ch)
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

// emit runs fetchMax and sends the result on ch (non-blocking drop on full).
func (f *ExternalBlockFetcher) emit(ctx context.Context, ch chan<- ExternalBlockHeight) {
	height, err := f.fetchMax(ctx)
	if err != nil {
		f.logger.Warn("external block fetcher: fetchMax failed",
			"service_id", f.serviceID,
			"error", err,
		)
		return
	}
	ebh := ExternalBlockHeight{ServiceID: f.serviceID, Height: height}
	select {
	case ch <- ebh:
	default:
		f.logger.Warn("external block fetcher: channel full, dropping result",
			"service_id", f.serviceID,
		)
	}
}

// fetchMax queries all sources in parallel and returns the maximum height seen.
func (f *ExternalBlockFetcher) fetchMax(ctx context.Context) (uint64, error) {
	if len(f.sources) == 0 {
		return 0, fmt.Errorf("no external block sources configured for %s", f.serviceID)
	}

	type result struct {
		height uint64
		err    error
	}

	results := make([]result, len(f.sources))
	var wg sync.WaitGroup

	for i, src := range f.sources {
		i, src := i, src
		wg.Add(1)
		go func() {
			defer safego.Recover(f.logger, "healthcheck.external.fetch")
			defer wg.Done()
			h, err := f.fetchOne(ctx, src)
			results[i] = result{height: h, err: err}
		}()
	}
	wg.Wait()

	var maxHeight uint64
	var lastErr error
	successCount := 0
	for _, r := range results {
		if r.err != nil {
			lastErr = r.err
			continue
		}
		successCount++
		if r.height > maxHeight {
			maxHeight = r.height
		}
	}
	if successCount == 0 {
		return 0, fmt.Errorf("all external sources failed for %s: %w", f.serviceID, lastErr)
	}
	return maxHeight, nil
}

// fetchOne queries a single external source and returns the block height.
func (f *ExternalBlockFetcher) fetchOne(ctx context.Context, src config.ExternalBlockSource) (uint64, error) {
	timeout := src.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	switch strings.ToLower(src.Type) {
	case "json_rpc", "jsonrpc", "":
		return f.fetchJSONRPC(ctx, src)
	case "rest":
		return f.fetchREST(ctx, src)
	default:
		return 0, fmt.Errorf("unknown source type %q for %s", src.Type, f.serviceID)
	}
}

// fetchJSONRPC sends eth_blockNumber (or src.Method) to the URL and parses the result.
func (f *ExternalBlockFetcher) fetchJSONRPC(ctx context.Context, src config.ExternalBlockSource) (uint64, error) {
	method := src.Method
	if method == "" {
		method = "eth_blockNumber"
	}
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  []any{},
		"id":      1,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, src.URL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("fetchJSONRPC: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetchJSONRPC: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("fetchJSONRPC: read body: %w", err)
	}
	return parseHeightFromBytes(respBody)
}

// fetchREST sends a GET to URL+Path and parses the response.
func (f *ExternalBlockFetcher) fetchREST(ctx context.Context, src config.ExternalBlockSource) (uint64, error) {
	url := src.URL
	if src.Path != "" {
		url = strings.TrimRight(url, "/") + "/" + strings.TrimLeft(src.Path, "/")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("fetchREST: build request: %w", err)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetchREST: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("fetchREST: read body: %w", err)
	}
	return parseHeightFromBytes(respBody)
}

// parseHeightFromBytes tries several strategies to extract a block height:
//  1. JSON-RPC result field ({"result":"0x..."} or {"result":123})
//  2. Nested path via gjson (result.sync_info.latest_block_height, etc.)
//  3. Hex string (0x...)
//  4. Decimal string / integer
func parseHeightFromBytes(data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("empty response")
	}

	// If it's a JSON object, try common paths.
	if bytes.HasPrefix(data, []byte("{")) {
		// JSON-RPC style: {"result": "0x..."}
		result := gjson.GetBytes(data, "result")
		if result.Exists() {
			if h, err := parseHeightFromResult(result); err == nil {
				return h, nil
			}
		}
		// CometBFT style: result.sync_info.latest_block_height
		nested := gjson.GetBytes(data, "result.sync_info.latest_block_height")
		if nested.Exists() {
			if h, err := parseHeightValue(nested); err == nil {
				return h, nil
			}
		}
		// Try block.header.height (Cosmos REST)
		blockHeight := gjson.GetBytes(data, "block.header.height")
		if blockHeight.Exists() {
			if h, err := parseHeightValue(blockHeight); err == nil {
				return h, nil
			}
		}
		return 0, fmt.Errorf("cannot find block height in JSON: %s", truncate(data, 200))
	}

	// Raw value (hex or decimal string).
	s := strings.TrimSpace(string(data))
	s = strings.Trim(s, `"`)
	return parseHeightString(s)
}

// parseHeightFromResult handles the JSON-RPC "result" field which may be a
// string, number, or nested object.
func parseHeightFromResult(result gjson.Result) (uint64, error) {
	switch result.Type {
	case gjson.String:
		return parseHeightString(result.String())
	case gjson.Number:
		f := result.Float()
		if f < 0 {
			return 0, fmt.Errorf("negative block height: %v", f)
		}
		return uint64(f), nil
	case gjson.JSON:
		// Nested object — try common sub-paths.
		for _, path := range []string{"sync_info.latest_block_height", "height"} {
			v := gjson.Get(result.Raw, path)
			if v.Exists() {
				if h, err := parseHeightValue(v); err == nil {
					return h, nil
				}
			}
		}
	}
	return 0, fmt.Errorf("unrecognised result type %v", result.Type)
}

// parseHeightValue extracts a uint64 from a gjson.Result (string or number).
func parseHeightValue(v gjson.Result) (uint64, error) {
	switch v.Type {
	case gjson.String:
		return parseHeightString(v.String())
	case gjson.Number:
		f := v.Float()
		if f < 0 {
			return 0, fmt.Errorf("negative block height")
		}
		return uint64(f), nil
	}
	return 0, fmt.Errorf("unexpected gjson type %v", v.Type)
}

// parseHeightString parses either a hex (0x-prefixed) or decimal string.
func parseHeightString(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		h, err := strconv.ParseUint(s[2:], 16, 64)
		if err != nil {
			return 0, fmt.Errorf("parse hex height %q: %w", s, err)
		}
		return h, nil
	}
	h, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse decimal height %q: %w", s, err)
	}
	return h, nil
}

// truncate returns at most n bytes of b as a string.
func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
