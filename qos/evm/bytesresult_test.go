package evm

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

// A DATA method's result must be hex bytes; "0x0" is a QUANTITY's zero and
// no node encodes bytes that way. QUANTITY methods are not judged.
func TestInvalidResult(t *testing.T) {
	p := &Plugin{}
	cases := []struct {
		method, body string
		invalid      bool
	}{
		{"eth_call", `{"result":"0x0"}`, true},
		{"eth_call", `{"result":"0xabc"}`, true},
		{"eth_call", `{"result":"0xzz"}`, true},
		{"eth_call", `{"result":"deadbeef"}`, true},
		{"eth_call", `{"result":"0x"}`, false},
		{"eth_call", `{"result":"0x00ff"}`, false},
		{"eth_getCode", `{"result":"0x0"}`, true},
		{"eth_getStorageAt", `{"result":"0x0000000000000000000000000000000000000000000000000000000000000001"}`, false},
		{"eth_call", `{"error":{"code":3,"message":"execution reverted"}}`, false},
		{"eth_call", `{"result":null}`, false},
		{"eth_getBalance", `{"result":"0x0"}`, false},
		{"eth_estimateGas", `{"result":"0x0"}`, false},
	}
	for _, c := range cases {
		payload := domain.NewPayload([]byte(`{"method":"`+c.method+`"}`), domain.RPCTypeJSONRPC, c.method)
		if _, got := p.InvalidResult(payload, []byte(c.body)); got != c.invalid {
			t.Errorf("%s %s: invalid %v, want %v", c.method, c.body, got, c.invalid)
		}
	}
}
