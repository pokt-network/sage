package evm

import (
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// parseRequest validates the pre-read request body and returns one Payload per
// JSON-RPC request. Supports single requests and batch arrays.
func parseRequest(body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	if rpcType != domain.RPCTypeJSONRPC && rpcType != domain.RPCTypeWebSocket {
		return nil, &domain.RelayError{
			Kind:      domain.ErrValidation,
			Message:   fmt.Sprintf("EVM plugin: unsupported RPC type %q", rpcType),
			Retryable: false,
		}
	}

	if len(body) == 0 {
		return nil, &domain.RelayError{
			Kind:      domain.ErrValidation,
			Message:   "EVM plugin: empty request body",
			Retryable: false,
		}
	}

	if !gjson.ValidBytes(body) {
		return nil, &domain.RelayError{
			Kind:      domain.ErrValidation,
			Message:   "EVM plugin: request body is not valid JSON",
			Retryable: false,
		}
	}

	// Detect batch vs single.
	trimmed := trimLeftSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		return parseBatch(body, rpcType)
	}
	return parseSingle(body, rpcType)
}

// parseSingle parses one JSON-RPC request object.
func parseSingle(body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	method, err := extractMethod(body)
	if err != nil {
		return nil, &domain.RelayError{
			Kind:      domain.ErrValidation,
			Message:   fmt.Sprintf("EVM plugin: %v", err),
			Retryable: false,
		}
	}
	return []domain.Payload{domain.NewPayload(body, rpcType, method)}, nil
}

// parseBatch parses a JSON-RPC batch array.
// Each element becomes a separate Payload.
func parseBatch(body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	result := gjson.ParseBytes(body)
	if !result.IsArray() {
		return nil, &domain.RelayError{
			Kind:      domain.ErrValidation,
			Message:   "EVM plugin: batch must be a JSON array",
			Retryable: false,
		}
	}

	items := result.Array()
	if len(items) == 0 {
		return nil, &domain.RelayError{
			Kind:      domain.ErrValidation,
			Message:   "EVM plugin: batch array is empty",
			Retryable: false,
		}
	}

	payloads := make([]domain.Payload, 0, len(items))
	for i, item := range items {
		raw := []byte(item.Raw)
		method, err := extractMethod(raw)
		if err != nil {
			return nil, &domain.RelayError{
				Kind:      domain.ErrValidation,
				Message:   fmt.Sprintf("EVM plugin: batch item %d: %v", i, err),
				Retryable: false,
			}
		}
		payloads = append(payloads, domain.NewPayload(raw, rpcType, method))
	}
	return payloads, nil
}

// extractMethod extracts and validates the "method" field from a JSON-RPC request body.
func extractMethod(body []byte) (string, error) {
	method := gjson.GetBytes(body, "method")
	if !method.Exists() {
		return "", fmt.Errorf("missing \"method\" field")
	}
	if method.Type != gjson.String {
		return "", fmt.Errorf("\"method\" must be a string")
	}
	name := method.String()
	if name == "" {
		return "", fmt.Errorf("\"method\" is empty")
	}

	// Validate jsonrpc version field is present (optional but validates structure).
	ver := gjson.GetBytes(body, "jsonrpc")
	if !ver.Exists() {
		return "", fmt.Errorf("missing \"jsonrpc\" field")
	}
	if ver.String() != "2.0" {
		return "", fmt.Errorf("\"jsonrpc\" must be \"2.0\", got %q", ver.String())
	}

	return name, nil
}

// trimLeftSpace returns a slice of b with leading whitespace removed.
func trimLeftSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r') {
		b = b[1:]
	}
	return b
}
