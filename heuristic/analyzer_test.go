package heuristic

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

func TestAnalyze_Tier0_HTTPStatus(t *testing.T) {
	tests := []struct {
		name             string
		statusCode       int
		wantRetry        bool
		wantCircuitBreak bool
		wantPenalize     bool
		wantAttribution  ErrorAttribution
	}{
		{
			name:             "500 server error",
			statusCode:       500,
			wantRetry:        true,
			wantCircuitBreak: true,
			wantPenalize:     true,
			wantAttribution:  AttrSupplier,
		},
		{
			name:             "502 bad gateway",
			statusCode:       502,
			wantRetry:        true,
			wantCircuitBreak: true,
			wantPenalize:     true,
			wantAttribution:  AttrSupplier,
		},
		{
			name:             "503 service unavailable",
			statusCode:       503,
			wantRetry:        true,
			wantCircuitBreak: true,
			wantPenalize:     true,
			wantAttribution:  AttrSupplier,
		},
		{
			name:             "429 rate limited — retry but no circuit break",
			statusCode:       429,
			wantRetry:        true,
			wantCircuitBreak: false,
			wantPenalize:     true,
			wantAttribution:  AttrSupplier,
		},
		{
			name:             "408 request timeout — supplier fault, not the client's",
			statusCode:       408,
			wantRetry:        true,
			wantCircuitBreak: false,
			wantPenalize:     true,
			wantAttribution:  AttrSupplier,
		},
		{
			name:             "400 bad request — client fault",
			statusCode:       400,
			wantRetry:        false,
			wantCircuitBreak: false,
			wantPenalize:     false,
			wantAttribution:  AttrClient,
		},
		{
			name:             "404 not found — client fault",
			statusCode:       404,
			wantRetry:        false,
			wantCircuitBreak: false,
			wantPenalize:     false,
			wantAttribution:  AttrClient,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Analyze([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`), tt.statusCode, domain.RPCTypeJSONRPC)
			if result.ShouldRetry != tt.wantRetry {
				t.Errorf("ShouldRetry = %v, want %v", result.ShouldRetry, tt.wantRetry)
			}
			if result.ShouldCircuitBreak != tt.wantCircuitBreak {
				t.Errorf("ShouldCircuitBreak = %v, want %v", result.ShouldCircuitBreak, tt.wantCircuitBreak)
			}
			if result.ShouldPenalize != tt.wantPenalize {
				t.Errorf("ShouldPenalize = %v, want %v", result.ShouldPenalize, tt.wantPenalize)
			}
			if result.Attribution != tt.wantAttribution {
				t.Errorf("Attribution = %v, want %v", result.Attribution, tt.wantAttribution)
			}
		})
	}
}

func TestAnalyze_Tier1_Structural(t *testing.T) {
	tests := []struct {
		name             string
		body             []byte
		wantRetry        bool
		wantCircuitBreak bool
		wantReason       string
	}{
		{
			name:             "empty body",
			body:             []byte(""),
			wantRetry:        true,
			wantCircuitBreak: true,
			wantReason:       "empty_response",
		},
		{
			name:             "whitespace only",
			body:             []byte("   \n\t  "),
			wantRetry:        true,
			wantCircuitBreak: true,
			wantReason:       "empty_response",
		},
		{
			name:             "HTML error page",
			body:             []byte("<!DOCTYPE html><html><body>502 Bad Gateway</body></html>"),
			wantRetry:        true,
			wantCircuitBreak: true,
			wantReason:       "html_response",
		},
		{
			name:             "plain text error",
			body:             []byte("Service Temporarily Unavailable"),
			wantRetry:        true,
			wantCircuitBreak: false,
			wantReason:       "plain_text_response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Analyze(tt.body, 200, domain.RPCTypeJSONRPC)
			if result.ShouldRetry != tt.wantRetry {
				t.Errorf("ShouldRetry = %v, want %v", result.ShouldRetry, tt.wantRetry)
			}
			if result.ShouldCircuitBreak != tt.wantCircuitBreak {
				t.Errorf("ShouldCircuitBreak = %v, want %v", result.ShouldCircuitBreak, tt.wantCircuitBreak)
			}
			if result.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", result.Reason, tt.wantReason)
			}
		})
	}
}

func TestAnalyze_Tier2_JSONRPC_Success(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","result":"0x1234","id":1}`)
	result := Analyze(body, 200, domain.RPCTypeJSONRPC)
	if result.ShouldRetry {
		t.Error("ShouldRetry should be false for valid result")
	}
	if result.ShouldCircuitBreak {
		t.Error("ShouldCircuitBreak should be false for valid result")
	}
	if result.ShouldPenalize {
		t.Error("ShouldPenalize should be false for valid result")
	}
}

func TestAnalyze_Tier2_JSONRPC_NullResultWithError(t *testing.T) {
	// result:null + error is a valid error-only pattern — should classify the error, not penalize for null.
	body := []byte(`{"jsonrpc":"2.0","result":null,"error":{"code":-32601,"message":"method not found"},"id":1}`)
	result := Analyze(body, 200, domain.RPCTypeJSONRPC)
	if result.ShouldRetry {
		t.Error("ShouldRetry should be false for method not found")
	}
	if result.Attribution != AttrClient {
		t.Errorf("Attribution = %v, want AttrClient", result.Attribution)
	}
	if result.Reason != "method_not_found" {
		t.Errorf("Reason = %q, want method_not_found", result.Reason)
	}
}

func TestAnalyze_Tier2_FabricatedResponse(t *testing.T) {
	// Both non-null result AND error — fabricated.
	body := []byte(`{"jsonrpc":"2.0","result":"0x1234","error":{"code":-32000,"message":"some error"},"id":1}`)
	result := Analyze(body, 200, domain.RPCTypeJSONRPC)
	if !result.ShouldRetry {
		t.Error("ShouldRetry should be true for fabricated response")
	}
	if !result.ShouldCircuitBreak {
		t.Error("ShouldCircuitBreak should be true for fabricated response")
	}
	if result.PenaltySeverity != SeverityFatal {
		t.Errorf("PenaltySeverity = %q, want %q", result.PenaltySeverity, SeverityFatal)
	}
	if result.Reason != "fabricated_response" {
		t.Errorf("Reason = %q, want fabricated_response", result.Reason)
	}
}

func TestAnalyze_Tier2_ErrorCodeClassification(t *testing.T) {
	tests := []struct {
		name            string
		errorJSON       string
		wantRetry       bool
		wantAttribution ErrorAttribution
		wantReason      string
	}{
		{
			name:            "execution reverted — client fault",
			errorJSON:       `{"code":3,"message":"execution reverted"}`,
			wantRetry:       false,
			wantAttribution: AttrClient,
			wantReason:      "execution_reverted",
		},
		{
			name:            "method not found — client fault",
			errorJSON:       `{"code":-32601,"message":"Method not found"}`,
			wantRetry:       false,
			wantAttribution: AttrClient,
			wantReason:      "method_not_found",
		},
		{
			name:            "invalid params — client fault",
			errorJSON:       `{"code":-32602,"message":"Invalid params"}`,
			wantRetry:       false,
			wantAttribution: AttrClient,
			wantReason:      "invalid_params",
		},
		{
			name:            "block not found — blockchain fault",
			errorJSON:       `{"code":-32000,"message":"block not found"}`,
			wantRetry:       true,
			wantAttribution: AttrBlockchain,
			wantReason:      "blockchain_error",
		},
		{
			name:            "missing trie node — blockchain fault",
			errorJSON:       `{"code":-32000,"message":"missing trie node abc123"}`,
			wantRetry:       true,
			wantAttribution: AttrBlockchain,
			wantReason:      "blockchain_error",
		},
		{
			name:            "service unavailable at -32603 — supplier fault",
			errorJSON:       `{"code":-32603,"message":"Service unavailable"}`,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
			wantReason:      "supplier_internal_error",
		},
		{
			name:            "bad gateway at -32603 — supplier fault",
			errorJSON:       `{"code":-32603,"message":"Bad Gateway"}`,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
			wantReason:      "supplier_internal_error",
		},
		{
			name:            "parse error — supplier fault",
			errorJSON:       `{"code":-32700,"message":"Parse error"}`,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
			wantReason:      "parse_error",
		},
		{
			name:            "service unavailable at -32000 — supplier fault",
			errorJSON:       `{"code":-32000,"message":"service unavailable"}`,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
			wantReason:      "supplier_server_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(`{"jsonrpc":"2.0","error":` + tt.errorJSON + `,"id":1}`)
			result := Analyze(body, 200, domain.RPCTypeJSONRPC)
			if result.ShouldRetry != tt.wantRetry {
				t.Errorf("ShouldRetry = %v, want %v", result.ShouldRetry, tt.wantRetry)
			}
			if result.Attribution != tt.wantAttribution {
				t.Errorf("Attribution = %v, want %v", result.Attribution, tt.wantAttribution)
			}
			if result.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", result.Reason, tt.wantReason)
			}
		})
	}
}

func TestAnalyze_Tier2_CometBFT_EmptyResultIsValid(t *testing.T) {
	// CometBFT: {"result":{}} is a valid health check response.
	body := []byte(`{"jsonrpc":"2.0","result":{},"id":1}`)
	result := Analyze(body, 200, domain.RPCTypeCometBFT)
	if result.ShouldRetry {
		t.Error("ShouldRetry should be false for valid CometBFT result")
	}
	if result.ShouldCircuitBreak {
		t.Error("ShouldCircuitBreak should be false for valid CometBFT result")
	}
	if result.Reason != "success" {
		t.Errorf("Reason = %q, want success", result.Reason)
	}
}

func TestAnalyze_Tier2_CometBFT_NullResultIsValid(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","result":null,"id":1}`)
	result := Analyze(body, 200, domain.RPCTypeCometBFT)
	if result.ShouldRetry {
		t.Error("ShouldRetry should be false for CometBFT null result")
	}
	if result.Reason != "success" {
		t.Errorf("Reason = %q, want success", result.Reason)
	}
}

func TestAnalyze_Tier3_Indicators(t *testing.T) {
	tests := []struct {
		name            string
		body            string
		wantRetry       bool
		wantAttribution ErrorAttribution
	}{
		{
			name:            "connection refused in non-JSON body",
			body:            `upstream connect error: connection refused`,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
		},
		{
			name:            "timeout in non-JSON body",
			body:            `request timeout exceeded`,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Analyze([]byte(tt.body), 200, domain.RPCTypeJSONRPC)
			if result.ShouldRetry != tt.wantRetry {
				t.Errorf("ShouldRetry = %v, want %v", result.ShouldRetry, tt.wantRetry)
			}
			if result.Attribution != tt.wantAttribution {
				t.Errorf("Attribution = %v, want %v", result.Attribution, tt.wantAttribution)
			}
		})
	}
}

func TestAnalyze_RetryAndCircuitBreakAreIndependent(t *testing.T) {
	// 429: retry=true, circuit_break=false
	result429 := Analyze([]byte(`{}`), 429, domain.RPCTypeJSONRPC)
	if !result429.ShouldRetry {
		t.Error("429: ShouldRetry should be true")
	}
	if result429.ShouldCircuitBreak {
		t.Error("429: ShouldCircuitBreak should be false")
	}

	// 500: retry=true, circuit_break=true
	result500 := Analyze([]byte(`{}`), 500, domain.RPCTypeJSONRPC)
	if !result500.ShouldRetry {
		t.Error("500: ShouldRetry should be true")
	}
	if !result500.ShouldCircuitBreak {
		t.Error("500: ShouldCircuitBreak should be true")
	}

	// 400: retry=false, circuit_break=false
	result400 := Analyze([]byte(`{}`), 400, domain.RPCTypeJSONRPC)
	if result400.ShouldRetry {
		t.Error("400: ShouldRetry should be false")
	}
	if result400.ShouldCircuitBreak {
		t.Error("400: ShouldCircuitBreak should be false")
	}

	// Blockchain error: retry=true, circuit_break=false
	bodyBlockchain := []byte(`{"jsonrpc":"2.0","error":{"code":-32000,"message":"block not found"},"id":1}`)
	resultBlockchain := Analyze(bodyBlockchain, 200, domain.RPCTypeJSONRPC)
	if !resultBlockchain.ShouldRetry {
		t.Error("blockchain error: ShouldRetry should be true")
	}
	if resultBlockchain.ShouldCircuitBreak {
		t.Error("blockchain error: ShouldCircuitBreak should be false")
	}
}

func TestAnalyze_ValidResponse_NoActionNeeded(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","result":"0xdeadbeef","id":1}`)
	result := Analyze(body, 200, domain.RPCTypeJSONRPC)
	if result.ShouldRetry {
		t.Error("valid response should not retry")
	}
	if result.ShouldCircuitBreak {
		t.Error("valid response should not circuit break")
	}
	if result.ShouldPenalize {
		t.Error("valid response should not penalize")
	}
}

func TestAnalyze_RESTResponse(t *testing.T) {
	// A REST response that is valid JSON but not JSON-RPC should be treated as success.
	body := []byte(`{"block":{"header":{"height":"12345"}}}`)
	result := Analyze(body, 200, domain.RPCTypeREST)
	if result.ShouldRetry {
		t.Error("valid REST response should not retry")
	}
	if result.ShouldCircuitBreak {
		t.Error("valid REST response should not circuit break")
	}
}

func TestErrorAttribution_String(t *testing.T) {
	tests := []struct {
		attr ErrorAttribution
		want string
	}{
		{AttrSupplier, "supplier"},
		{AttrBlockchain, "blockchain"},
		{AttrClient, "client"},
		{AttrUnknown, "unknown"},
	}
	for _, tt := range tests {
		if got := tt.attr.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.attr, got, tt.want)
		}
	}
}

// A JSON-RPC server reports a client's mistake inside a JSON-RPC envelope. A
// 4xx with something else in the body — an HTML 404 page, nothing at all — is
// the supplier's HTTP layer answering for a backend that never saw the
// request. One mainnet supplier answered 74% of its solana JSON-RPC posts with
// a 404 page; graded as the client's fault it was passed through unretried.
func TestAnalyze_4xxWithoutEnvelopeOnJSONRPC_IsSupplierFault(t *testing.T) {
	page := []byte(`<!DOCTYPE html><html><head><title>404 Not Found</title></head><body>nginx</body></html>`)

	got := Analyze(page, 404, domain.RPCTypeJSONRPC)
	if !got.ShouldRetry || !got.ShouldPenalize || got.Attribution != AttrSupplier || !got.MethodBlocking {
		t.Errorf("HTML 404 on JSON-RPC: retry=%v penalize=%v attribution=%v methodBlocking=%v reason=%s; want retry, penalize, supplier, method-blocking",
			got.ShouldRetry, got.ShouldPenalize, got.Attribution, got.MethodBlocking, got.Reason)
	}
	if got.ShouldCircuitBreak {
		t.Error("a 4xx page must not circuit-break on its own")
	}

	got = Analyze(nil, 404, domain.RPCTypeJSONRPC)
	if !got.ShouldRetry || got.Attribution != AttrSupplier {
		t.Errorf("empty 404 on JSON-RPC: retry=%v attribution=%v; want retry, supplier", got.ShouldRetry, got.Attribution)
	}

	// The same page on REST is the client asking for a path that does not
	// exist — that is what a REST 404 means.
	got = Analyze(page, 404, domain.RPCTypeREST)
	if got.ShouldRetry || got.Attribution != AttrClient {
		t.Errorf("HTML 404 on REST: retry=%v attribution=%v; want no retry, client", got.ShouldRetry, got.Attribution)
	}

	// A JSON-RPC error envelope with a 4xx status stays the client's.
	got = Analyze([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"invalid request"}}`), 400, domain.RPCTypeJSONRPC)
	if got.ShouldRetry || got.Attribution != AttrClient {
		t.Errorf("JSON-RPC error with 400: retry=%v attribution=%v; want no retry, client", got.ShouldRetry, got.Attribution)
	}
}

// A supplier 408 carries a valid JSON-RPC envelope often enough that the
// 4xx-without-envelope branch does not catch it, and it arrives on rpc types
// that branch does not apply to at all. Both paths must reach the same
// verdict: the supplier timed out, so rotate and score it, but do not block
// the method — a timeout is not a statement about which methods the host
// serves.
func TestAnalyze_408IsSupplierTimeoutOnEveryRPCType(t *testing.T) {
	cases := []struct {
		name    string
		body    []byte
		rpcType domain.RPCType
	}{
		{"json-rpc envelope", []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`), domain.RPCTypeJSONRPC},
		{"json-rpc empty body", []byte(``), domain.RPCTypeJSONRPC},
		{"rest body", []byte(`{"height":"1"}`), domain.RPCTypeREST},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := Analyze(tc.body, 408, tc.rpcType)

			if !result.ShouldRetry {
				t.Error("ShouldRetry = false, want true (another supplier will answer)")
			}
			if result.ShouldCircuitBreak {
				t.Error("ShouldCircuitBreak = true, want false (slow is not broken)")
			}
			if !result.ShouldPenalize {
				t.Error("ShouldPenalize = false, want true")
			}
			if result.Attribution != AttrSupplier {
				t.Errorf("Attribution = %v, want %v", result.Attribution, AttrSupplier)
			}
			if result.PenaltySeverity != SeverityMinor {
				t.Errorf("PenaltySeverity = %v, want %v", result.PenaltySeverity, SeverityMinor)
			}
			if result.MethodBlocking {
				t.Error("MethodBlocking = true, want false — a timeout is not method-specific")
			}
			if result.Reason != "http_408" {
				t.Errorf("Reason = %q, want %q", result.Reason, "http_408")
			}
		})
	}
}
