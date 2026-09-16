package cosmos

import (
	"context"
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

var _ qos.ImmutableClassifier = (*Plugin)(nil)

func TestIsImmutable(t *testing.T) {
	p := newPlugin(10)
	for _, tc := range []struct {
		verb, path, body string
		rpcType          domain.RPCType
		want             bool
	}{
		{http.MethodPost, "/", `{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"100"}}`, domain.RPCTypeCometBFT, true},
		{http.MethodPost, "/", `{"jsonrpc":"2.0","id":1,"method":"block","params":["100"]}`, domain.RPCTypeJSONRPC, true},
		{http.MethodPost, "/", `{"jsonrpc":"2.0","id":1,"method":"block","params":{}}`, domain.RPCTypeCometBFT, false},
		{http.MethodPost, "/", `{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"0"}}`, domain.RPCTypeCometBFT, false},
		{http.MethodPost, "/", `{"jsonrpc":"2.0","id":1,"method":"tx","params":{"hash":"ABCD"}}`, domain.RPCTypeCometBFT, true},
		{http.MethodPost, "/", `{"jsonrpc":"2.0","id":1,"method":"status","params":{}}`, domain.RPCTypeCometBFT, false},
		{http.MethodPost, "/", `{"jsonrpc":"2.0","id":1,"method":"abci_query","params":{"height":"100"}}`, domain.RPCTypeCometBFT, false},
		{http.MethodGet, "/block?height=100", "", domain.RPCTypeCometBFT, true},
		{http.MethodGet, "/block_results?height=100", "", domain.RPCTypeCometBFT, true},
		{http.MethodGet, "/block", "", domain.RPCTypeCometBFT, false},
		{http.MethodGet, "/block_by_hash?hash=0xABCD", "", domain.RPCTypeCometBFT, true},
		{http.MethodGet, "/cosmos/base/tendermint/v1beta1/blocks/100", "", domain.RPCTypeREST, true},
		{http.MethodGet, "/cosmos/base/tendermint/v1beta1/blocks/latest", "", domain.RPCTypeREST, false},
		{http.MethodGet, "/cosmos/base/tendermint/v1beta1/validatorsets/100", "", domain.RPCTypeREST, true},
		{http.MethodGet, "/cosmos/base/tendermint/v1beta1/validatorsets/latest", "", domain.RPCTypeREST, false},
		{http.MethodGet, "/cosmos/tx/v1beta1/txs/ABCDEF01", "", domain.RPCTypeREST, true},
		{http.MethodGet, "/cosmos/tx/v1beta1/txs/block/100", "", domain.RPCTypeREST, true},
		{http.MethodGet, "/cosmos/bank/v1beta1/balances/pokt1abc", "", domain.RPCTypeREST, false},
	} {
		payloads, err := p.ParseRequest(context.Background(), makeRequest(tc.verb, tc.path, tc.body), []byte(tc.body), tc.rpcType)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.verb, tc.path, err)
		}
		if got := p.IsImmutable(payloads[0]); got != tc.want {
			t.Errorf("IsImmutable(%s %s %s) = %v, want %v", tc.verb, tc.path, tc.body, got, tc.want)
		}
	}
}
