package solana

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// HeadLag reads the three head methods on the block-height scale and nothing
// else: getSlot is a slot and getBlock names a historical block. With no block
// rate yet nothing is stale; the lag is still read (AnswerLag's own test
// covers the judgement).
func TestHeadLag_ReadsHeadMethodsOnly(t *testing.T) {
	p := NewPlugin(nil, 0)
	p.UpdateBlockHeight("a1-https://x.a.net", 429_672_477)
	pay := func(m string) domain.Payload {
		return domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"`+m+`"}`), domain.RPCTypeJSONRPC, m)
	}
	for _, tc := range []struct {
		method, body string
		ok           bool
		lag          uint64
	}{
		{"getEpochInfo", `{"result":{"blockHeight":429672244,"absoluteSlot":451632688}}`, true, 233},
		{"getBlockHeight", `{"result":429672470}`, true, 7},
		{"getLatestBlockhash", `{"result":{"context":{"slot":451632700},"value":{"lastValidBlockHeight":429672627}}}`, true, 0},
		{"getSlot", `{"result":451632688}`, false, 0},
		{"getBlock", `{"result":{"blockHeight":100}}`, false, 0},
	} {
		lag, stale, ok := p.HeadLag(pay(tc.method), []byte(tc.body), time.Now())
		if ok != tc.ok || lag != tc.lag || stale {
			t.Errorf("%s: ok %v lag %d stale %v, want ok %v lag %d stale false", tc.method, ok, lag, stale, tc.ok, tc.lag)
		}
	}
}

// An answer at a commitment below finalized is on another scale: not read.
func TestHeadLag_SkipsUnfinalizedCommitment(t *testing.T) {
	p := NewPlugin(nil, 100)
	req := domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"getBlockHeight","params":[{"commitment":"processed"}]}`), domain.RPCTypeJSONRPC, "getBlockHeight")
	if _, _, ok := p.HeadLag(req, []byte(`{"result":100}`), time.Now()); ok {
		t.Fatal("processed answer must not be read")
	}
	if d, _ := p.ExtractData("a1-https://x.a.net", req.Bytes(), []byte(`{"result":100}`)); d.BlockHeight != nil {
		t.Fatal("processed answer must not feed consensus")
	}
}
