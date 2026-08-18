package heuristic

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

func TestAnalyzeFrame_SuccessNotification(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xcd0c3e8af590364c09d0fa6a1210faf5","result":{"number":"0x1b4"}}}`)
	res := AnalyzeFrame(body, domain.RPCTypeJSONRPC)
	if res.ShouldPenalize {
		t.Errorf("valid subscription notification should not penalize: %+v", res)
	}
}

func TestAnalyzeFrame_SubscriptionAck(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":"0xcd0c3e8af590364c09d0fa6a1210faf5"}`)
	res := AnalyzeFrame(body, domain.RPCTypeJSONRPC)
	if res.ShouldPenalize {
		t.Errorf("valid subscribe ack should not penalize: %+v", res)
	}
}

func TestAnalyzeFrame_EmptyBody(t *testing.T) {
	res := AnalyzeFrame(nil, domain.RPCTypeJSONRPC)
	if !res.ShouldPenalize {
		t.Errorf("empty frame must penalize, got %+v", res)
	}
	if res.Reason != "empty_response" {
		t.Errorf("expected empty_response reason, got %q", res.Reason)
	}
}

func TestAnalyzeFrame_HTMLErrorPage(t *testing.T) {
	body := []byte(`<!DOCTYPE html><html><body>502 Bad Gateway</body></html>`)
	res := AnalyzeFrame(body, domain.RPCTypeJSONRPC)
	if !res.ShouldPenalize || res.PenaltySeverity != SeverityCritical {
		t.Errorf("HTML frame should be critical, got %+v", res)
	}
}

func TestAnalyzeFrame_FabricatedResponse(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x123","error":{"code":-32000,"message":"oops"}}`)
	res := AnalyzeFrame(body, domain.RPCTypeJSONRPC)
	if !res.ShouldPenalize || res.Reason != "fabricated_response" {
		t.Errorf("fabricated frame should be flagged, got %+v", res)
	}
	if res.PenaltySeverity != SeverityFatal {
		t.Errorf("fabricated frame should be fatal severity, got %v", res.PenaltySeverity)
	}
}

func TestAnalyzeFrame_JSONRPCError(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"rate limit exceeded"}}`)
	res := AnalyzeFrame(body, domain.RPCTypeJSONRPC)
	if !res.ShouldPenalize {
		t.Errorf("JSON-RPC error should penalize, got %+v", res)
	}
}

func TestAnalyzeFrame_NoTier0Dependency(t *testing.T) {
	// The same body analysed with Analyze(status=200) vs AnalyzeFrame should
	// produce equivalent results on the body side, since AnalyzeFrame skips
	// Tier 0 and Analyze's Tier 0 doesn't trigger on 2xx.
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	frameRes := AnalyzeFrame(body, domain.RPCTypeJSONRPC)
	httpRes := Analyze(body, 200, domain.RPCTypeJSONRPC)
	if frameRes.ShouldPenalize != httpRes.ShouldPenalize || frameRes.Reason != httpRes.Reason {
		t.Errorf("frame vs 200-status analysis diverged: frame=%+v http=%+v", frameRes, httpRes)
	}
}
