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
			name:            "connection refused in -32603 data — supplier fault, wording in data not message",
			errorJSON:       `{"code":-32603,"message":"Internal error","data":"Post \"http://10.0.0.5:26657\": connection refused"}`,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
			wantReason:      "supplier_internal_error",
		},
		{
			name:            "CometBFT tx not found at -32603 — the node's answer, passed through",
			errorJSON:       `{"code":-32603,"message":"Internal error","data":"tx (3A1F) not found"}`,
			wantRetry:       false,
			wantAttribution: AttrBlockchain,
			wantReason:      "internal_error",
		},
		{
			name:            "CometBFT pruned height at -32603 — blockchain, retried without penalty",
			errorJSON:       `{"code":-32603,"message":"Internal error","data":"height 27782 is not available, lowest height is 25052001"}`,
			wantRetry:       true,
			wantAttribution: AttrBlockchain,
			wantReason:      "height_not_available",
		},
		{
			name:            "bare -32603 with no data — passed through",
			errorJSON:       `{"code":-32603,"message":"Internal error"}`,
			wantRetry:       false,
			wantAttribution: AttrBlockchain,
			wantReason:      "internal_error",
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

	// A plain-text 403 on REST is not: the gateway signs every relay, so no
	// client can be refused access. That is the supplier's front door. On
	// the 2026-09-04 canary two hosts answered "Access Denied" to 40% of
	// eth-beacon relays and client traffic never moved their score.
	got = Analyze([]byte("Access Denied"), 403, domain.RPCTypeREST)
	if !got.ShouldRetry || !got.ShouldPenalize || got.Attribution != AttrSupplier || got.Reason != "http_4xx_page" {
		t.Errorf("plain 403 on REST: retry=%v penalize=%v attribution=%v reason=%s; want retry, penalize, supplier", got.ShouldRetry, got.ShouldPenalize, got.Attribution, got.Reason)
	}
	// A JSON 403 is the backend speaking, and stays the client's.
	got = Analyze([]byte(`{"error":"forbidden"}`), 403, domain.RPCTypeREST)
	if got.Attribution != AttrClient {
		t.Errorf("JSON 403 on REST: attribution=%v, want client", got.Attribution)
	}

	// A JSON-RPC error envelope with a 4xx status stays the client's.
	got = Analyze([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"invalid request"}}`), 400, domain.RPCTypeJSONRPC)
	if got.ShouldRetry || got.Attribution != AttrClient {
		t.Errorf("JSON-RPC error with 400: retry=%v attribution=%v; want no retry, client", got.ShouldRetry, got.Attribution)
	}
}

// A supplier 408 gets no special case, and that is a decision the mainnet
// canary made for us on 2026-09-02.
//
// Treating 408 as a supplier timeout — retryable, AttrSupplier, minor penalty
// — is defensible on its face: the supplier's own server gave up waiting, so
// another supplier should answer. Shipped to the canary it quadrupled the
// client-facing 408 rate, 0.674% -> 2.597% of all client requests measured on
// sage_client_requests_total over equal 30-minute windows, with roughly 1,600
// requests per 100k that had been answered 200 now answered 408. Attempts per
// client request did not move (1.555 -> 1.524), so it was not retry
// amplification; SAGE emits no 408 of its own anywhere, so it was not the
// gateway manufacturing them. What is left is the penalty: scoring every
// timing-out supplier down concentrates traffic onto a smaller tier-1 set,
// which then sheds under the load it inherits, which scores it down in turn.
//
// So 408 falls through to the generic 4xx branches, and what happens to it
// depends on the body, exactly as any other 4xx does. Re-landing the special
// case needs a canary experiment that separates the retry half from the
// penalty half, not a rerun of the same reasoning.
func TestAnalyze_408IsRetriedAndNotPenalized(t *testing.T) {
	cases := []struct {
		name            string
		body            []byte
		rpcType         domain.RPCType
		status          int // defaults to 408
		wantRetry       bool
		wantPenalize    bool
		wantAttribution ErrorAttribution
		wantReason      string
	}{
		{
			name:            "json-rpc envelope: rotate, score nothing",
			body:            []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`),
			rpcType:         domain.RPCTypeJSONRPC,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
			wantReason:      "http_408",
		},
		{
			// The shentu shape: on REST and CometBFT frontDoorRefusal claims
			// only 401 and 403, so before this every 408 was the client's and
			// the caller got the supplier's timeout with rotation untried.
			name:            "rest: rotate, score nothing",
			body:            []byte(`{"height":"1"}`),
			rpcType:         domain.RPCTypeREST,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
			wantReason:      "http_408",
		},
		{
			// The body decides nothing here, and that is the point. On
			// JSON-RPC frontDoorRefusal claims every 4xx, so while this case
			// sat below it a 408 with a non-JSON body was graded
			// http_4xx_page — penalised and method-blocked — which was 448 of
			// 455 of the canary's 408s. One verdict for 408, whatever the
			// body: a timeout is not a statement about the request.
			name:            "json-rpc with no envelope: still just a timeout",
			body:            []byte(``),
			rpcType:         domain.RPCTypeJSONRPC,
			wantRetry:       true,
			wantAttribution: AttrSupplier,
			wantReason:      "http_408",
		},
		{
			// A non-JSON 4xx that is NOT a 408 keeps the front-door verdict,
			// so moving the 408 case above it took nothing else with it.
			name:            "403 with no envelope is still the front door",
			body:            []byte(``),
			rpcType:         domain.RPCTypeJSONRPC,
			status:          403,
			wantRetry:       true,
			wantPenalize:    true,
			wantAttribution: AttrSupplier,
			wantReason:      "http_4xx_page",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status := tc.status
			if status == 0 {
				status = 408
			}
			result := Analyze(tc.body, status, tc.rpcType)

			if result.ShouldRetry != tc.wantRetry {
				t.Errorf("ShouldRetry = %v, want %v", result.ShouldRetry, tc.wantRetry)
			}
			if result.ShouldPenalize != tc.wantPenalize {
				t.Errorf("ShouldPenalize = %v, want %v", result.ShouldPenalize, tc.wantPenalize)
			}
			if result.Attribution != tc.wantAttribution {
				t.Errorf("Attribution = %v, want %v", result.Attribution, tc.wantAttribution)
			}
			if result.Reason != tc.wantReason {
				t.Errorf("Reason = %q, want %q", result.Reason, tc.wantReason)
			}
			if result.ShouldCircuitBreak {
				t.Error("ShouldCircuitBreak = true, want false: no 4xx breaks a circuit")
			}
		})
	}
}

// A -32603 that names nothing on the supplier's side is the node's own answer
// to the request. CometBFT wraps every handler error this way with the reason
// in `data`, so on a CometBFT service the code is the normal shape of "could
// not serve this request". Until 2026-09-13 it retried with a major penalty,
// scoring the operator that answered a client's miss correctly. PATH passes it
// through; so does this.
func TestAnalyze_Tier2_InternalError_PassesThroughWithoutPenalty(t *testing.T) {
	for _, rpcType := range []domain.RPCType{domain.RPCTypeCometBFT, domain.RPCTypeJSONRPC} {
		t.Run(string(rpcType), func(t *testing.T) {
			body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"height 999999999 must be less than or equal to the current blockchain height 12345"}}`)
			result := Analyze(body, 200, rpcType)
			if result.ShouldRetry {
				t.Error("ShouldRetry = true, want false: the node answered; another supplier gets the same request and gives the same answer")
			}
			if result.ShouldPenalize {
				t.Error("ShouldPenalize = true, want false: a client's miss is not the supplier's fault")
			}
			if result.ShouldCircuitBreak {
				t.Error("ShouldCircuitBreak = true, want false")
			}
			if result.PenaltySeverity != SeverityNone {
				t.Errorf("PenaltySeverity = %q, want none", result.PenaltySeverity)
			}
			if result.Attribution != AttrBlockchain {
				t.Errorf("Attribution = %v, want %v", result.Attribution, AttrBlockchain)
			}
			if result.Reason != "internal_error" {
				t.Errorf("Reason = %q, want internal_error", result.Reason)
			}
		})
	}
}

// The wording that marks a -32603 as supplier infrastructure may sit in
// `data` rather than `message`: a proxy that wraps its upstream failure in a
// CometBFT-shaped envelope keeps `message` at "Internal error". Both fields
// are matched.
func TestAnalyze_Tier2_InternalError_SupplierWordingInData(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":{"reason":"upstream: bad gateway"}}}`)
	result := Analyze(body, 200, domain.RPCTypeCometBFT)
	if !result.ShouldRetry || !result.ShouldPenalize {
		t.Errorf("ShouldRetry = %v, ShouldPenalize = %v, want both true for a wrapped bad gateway", result.ShouldRetry, result.ShouldPenalize)
	}
	if result.Reason != "supplier_internal_error" {
		t.Errorf("Reason = %q, want supplier_internal_error", result.Reason)
	}
	if result.Attribution != AttrSupplier {
		t.Errorf("Attribution = %v, want %v", result.Attribution, AttrSupplier)
	}
}

// A pruned CometBFT node's answer keeps the indicator table's verdict: retry
// elsewhere, no penalty. The persistence canary capture of 2026-09-13 was
// six of these in nine relays, all from one pruned host.
func TestAnalyze_Tier2_InternalError_PrunedHeightRetriesWithoutPenalty(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"height 84213 is not available, lowest height is 25052001"}}`)
	result := Analyze(body, 200, domain.RPCTypeCometBFT)
	if !result.ShouldRetry {
		t.Error("ShouldRetry = false, want true: another operator may hold the height")
	}
	if result.ShouldPenalize || result.PenaltySeverity != SeverityNone {
		t.Errorf("ShouldPenalize = %v severity %q, want no penalty for a pruned node", result.ShouldPenalize, result.PenaltySeverity)
	}
	if result.Attribution != AttrBlockchain {
		t.Errorf("Attribution = %v, want blockchain", result.Attribution)
	}
	if result.Reason != "height_not_available" {
		t.Errorf("Reason = %q, want height_not_available", result.Reason)
	}
}
