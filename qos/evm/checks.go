package evm

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

// archivalBlockTags are block parameter values that refer to a non-archival (recent) state.
var archivalBlockTags = map[string]bool{
	"latest":    true,
	"pending":   true,
	"earliest":  true,
	"safe":      true,
	"finalized": true,
}

// methodsWithBlockParam lists eth_ methods whose last positional parameter is a block identifier.
// A non-tag block param (i.e. a hex number) indicates an archival request.
var methodsWithBlockParam = map[string]bool{
	"eth_getBalance":                          true,
	"eth_getCode":                             true,
	"eth_getTransactionCount":                 true,
	"eth_getStorageAt":                        true,
	"eth_call":                                true,
	"eth_estimateGas":                         true,
	"eth_getBlockByNumber":                    true,
	"eth_getBlockTransactionCountByNumber":    true,
	"eth_getUncleCountByBlockNumber":          true,
	"eth_getUncleByBlockNumberAndIndex":       true,
	"eth_getTransactionByBlockNumberAndIndex": true,
}

// parseHexUint64 parses an Ethereum hex-encoded integer (e.g. "0x1a2b") to uint64.
func parseHexUint64(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "0x") && !strings.HasPrefix(s, "0X") {
		return 0, fmt.Errorf("expected 0x prefix, got %q", s)
	}
	hex := s[2:]
	if hex == "" {
		return 0, fmt.Errorf("empty hex value")
	}
	v, err := strconv.ParseUint(hex, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("parse hex %q: %w", s, err)
	}
	return v, nil
}

// extractBlockNumber parses the block number from an eth_blockNumber JSON-RPC response body.
// The response body is the full JSON-RPC envelope; the result field is a hex string.
func extractBlockNumber(response []byte) (uint64, error) {
	result := gjson.GetBytes(response, "result")
	if !result.Exists() {
		return 0, fmt.Errorf("no result field in response")
	}
	if result.Type != gjson.String {
		return 0, fmt.Errorf("result is not a string (got %s)", result.Type)
	}
	return parseHexUint64(result.String())
}

// extractChainID parses the chain ID from an eth_chainId JSON-RPC response body.
func extractChainID(response []byte) (string, error) {
	result := gjson.GetBytes(response, "result")
	if !result.Exists() {
		return "", fmt.Errorf("no result field in response")
	}
	if result.Type != gjson.String {
		return "", fmt.Errorf("result is not a string (got %s)", result.Type)
	}
	s := result.String()
	if _, err := parseHexUint64(s); err != nil {
		return "", fmt.Errorf("chain ID is not valid hex: %w", err)
	}
	return s, nil
}

// isArchivalRequest returns true if the method + params indicate an archival data request.
// A request is archival when it targets a specific historical block (not a recent-state tag).
func isArchivalRequest(method string, params json.RawMessage) bool {
	if !methodsWithBlockParam[method] {
		return false
	}
	if len(params) == 0 {
		return false
	}

	// params is a JSON array. We need the last element that looks like a block identifier.
	result := gjson.ParseBytes(params)
	if !result.IsArray() {
		return false
	}

	arr := result.Array()
	if len(arr) == 0 {
		return false
	}

	// The block parameter is conventionally the last parameter for most methods.
	// For eth_getStorageAt it's the third parameter; for others it's the second.
	// Rather than hardcoding per-method positions, we inspect the last string element.
	blockParam := findLastStringParam(arr)
	if blockParam == "" {
		return false
	}

	// If it's a well-known tag, it's not archival.
	if archivalBlockTags[strings.ToLower(blockParam)] {
		return false
	}

	// If it's a hex number, it refers to a specific block — archival.
	_, err := parseHexUint64(blockParam)
	return err == nil
}

// findLastStringParam returns the last string element from a gjson array.
func findLastStringParam(arr []gjson.Result) string {
	for i := len(arr) - 1; i >= 0; i-- {
		if arr[i].Type == gjson.String {
			return arr[i].String()
		}
	}
	return ""
}
