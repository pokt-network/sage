package shannon

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	sdk "github.com/pokt-network/shannon-sdk"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/reputation"
)

// Regressions for the 2026-10-03 correctness audit. Each test asserts the
// correct behaviour; on the code the audit read they fail.

// newStakeLimitSupplier upgrades, reads the probe frame, and closes with the
// HA relay miner's 4002: the application's allocation for this supplier and
// session is spent.
func newStakeLimitSupplier(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(heuristic.CloseMinerStakeLimit, "stake limit reached"), time.Now().Add(time.Second))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A WebSocket recovery probe the relay miner refuses for over-servicing,
// either as its unsigned report (relayer_proxy 7) or by closing with 4002, is
// protocol-correct. Relays score it as nothing (over_serviced,
// lossIsSuppliers); a probe must not record it as a supplier major error.
func TestAudit_WSProbeChargesOverServicing(t *testing.T) {
	enabled := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: true}
	answer := `{"jsonrpc":"2.0","id":1,"result":"0x10"}`

	for _, tc := range []struct {
		name     string
		supplier *httptest.Server
		refusal  bool
	}{
		{"unsigned relayer_proxy 7 frame", newEchoSupplier(t), true},
		{"close 4002", newStakeLimitSupplier(t), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, rep, m := probeFixture(t, wsURL(tc.supplier), 40, answer, enabled)
			fn := r.deps.Protocol.fullNode.(*mockRelayFullNode)
			// An operator staking one host per RPC type: the address names the
			// JSON-RPC host, the probe dials the WebSocket one.
			fn.session = buildMultiServiceSession("eth", "pokt1supplier", map[sharedtypes.RPCType]string{
				sharedtypes.RPCType_JSON_RPC:  "https://rm.example.com",
				sharedtypes.RPCType_WEBSOCKET: wsURL(tc.supplier),
			})
			if tc.refusal {
				fn.validateResponse = &servicetypes.RelayResponse{RelayMinerError: &servicetypes.RelayMinerError{
					Codespace: "relayer_proxy", Code: 7, Message: "offchain rate limit hit by relayer proxy",
				}}
				fn.validateErr = fmt.Errorf("%w: missing supplier operator signature", sdk.ErrRelayResponseValidationBasicValidation)
			}

			r.probeCycle(context.Background(), []domain.ServiceID{"eth"})
			if len(m.probes) != 1 {
				t.Fatalf("probes = %v, want one", m.probes)
			}
			for _, c := range rep.calls {
				if c.signal.Type == reputation.SignalMajorError {
					t.Fatalf("probe %s recorded %s %q against %s; over-servicing is not the supplier's failure",
						m.probes[0], c.signal.Type, c.signal.Reason, c.endpoint)
				}
			}
		})
	}
}
