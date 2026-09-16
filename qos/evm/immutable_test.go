package evm

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

func TestIsImmutable(t *testing.T) {
	p := &Plugin{}
	for _, tc := range []struct {
		body string
		want bool
	}{
		{`{"jsonrpc":"2.0","method":"eth_chainId","params":[]}`, true},
		{`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["0xabc"]}`, true},
		{`{"jsonrpc":"2.0","method":"eth_getTransactionByHash","params":["0xabc"]}`, true},
		{`{"jsonrpc":"2.0","method":"eth_getBlockByHash","params":["0xabc",false]}`, true},
		{`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x10",false]}`, true},
		{`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false]}`, false},
		{`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["finalized",false]}`, false},
		{`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"blockHash":"0xabc"}]}`, true},
		{`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x1","toBlock":"latest"}]}`, false},
		{`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[]}`, false},
		{`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","latest"]}`, false},
		{`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xabc"]}`, false},
	} {
		method, err := extractMethod([]byte(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		got := p.IsImmutable(domain.NewPayload([]byte(tc.body), domain.RPCTypeJSONRPC, method))
		if got != tc.want {
			t.Errorf("IsImmutable(%s) = %v, want %v", tc.body, got, tc.want)
		}
	}
}
