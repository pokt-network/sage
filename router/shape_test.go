package router

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
)

// The error line names what the request asked for and the last verdict,
// with the path cut at its query and bounded.
func TestRequestShape(t *testing.T) {
	ctx := &relay.Context{RPCType: domain.RPCTypeJSONRPC, RPCTypeDetected: domain.RPCTypeJSONRPC}
	ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{"method":"status"}`), domain.RPCTypeJSONRPC, "status")}
	ctx.HeuristicResult = &heuristic.AnalysisResult{Reason: "http_4xx_page"}
	got := fmt.Sprint(requestShape(ctx))
	for _, want := range []string{"rpc_type json_rpc", "method status", "last_verdict http_4xx_page", "payloads 1"} {
		if !strings.Contains(got, want) {
			t.Errorf("shape %q lacks %q", got, want)
		}
	}
	if fmt.Sprint(requestShape(&relay.Context{})) == "" {
		t.Error("an empty context still logs its rpc_type fields")
	}
}
