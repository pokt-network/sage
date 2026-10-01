package jsonheight

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func nearPayload(body string) domain.Payload {
	return domain.NewPayload([]byte(body), domain.RPCTypeJSONRPC, "")
}

// A NEAR block for a finality is the head and is read; a block by id is
// history and is not. The canary's state height is read the same way.
func TestNEARHeadLag(t *testing.T) {
	p := NewPlugin(nil, NEAR, 10)
	p.UpdateBlockHeight("a1-https://x.a.net", 1000)
	time.Sleep(20 * time.Millisecond) // a block rate needs two readings apart in time
	p.UpdateBlockHeight("a1-https://x.a.net", 1010)
	now := time.Now()

	head := nearPayload(`{"jsonrpc":"2.0","id":1,"method":"block","params":{"finality":"final"}}`)
	if lag, _, ok := p.HeadLag(head, []byte(`{"result":{"header":{"height":1005}}}`), now); !ok || lag < 5 {
		t.Fatalf("block at final 1005: lag %d ok %v", lag, ok)
	}
	history := nearPayload(`{"jsonrpc":"2.0","id":1,"method":"block","params":{"block_id":17}}`)
	if _, _, ok := p.HeadLag(history, []byte(`{"result":{"header":{"height":17}}}`), now); ok {
		t.Fatal("a block by id is history, not the head")
	}
	canary := domain.NewPayload(NEAR.Canaries[0], domain.RPCTypeJSONRPC, "query")
	// The verdict is AnswerLag's (tested in qos); here, that the canary's
	// state height is what is measured.
	if lag, _, ok := p.HeadLag(canary, []byte(`{"result":{"block_height":500}}`), now); !ok || lag < 500 {
		t.Fatalf("canary state at 500: lag %d ok %v", lag, ok)
	}
	other := nearPayload(`{"jsonrpc":"2.0","id":1,"method":"query","params":{"request_type":"view_account","finality":"final","account_id":"someone"}}`)
	if _, _, ok := p.HeadLag(other, []byte(`{"result":{"block_height":500}}`), now); ok {
		t.Fatal("a client's own query is not the canary")
	}
}

func TestNEARCanaryFollowsTheFlag(t *testing.T) {
	p := NewPlugin(nil, NEAR, 10)
	has := func() bool {
		for _, c := range p.HealthChecks() {
			if c.Name == "near_canary" {
				return c.GradesHead && p.isCanary(c.Payload.Bytes()) && c.Payload.Method() == "query"
			}
		}
		return false
	}
	if has() {
		t.Fatal("no gate: no canary")
	}
	on := true
	p.SetStateCanary(func() bool { return on })
	if !has() {
		t.Fatal("flag on: canary missing")
	}
	on = false
	if has() {
		t.Fatal("flag off: canary sent")
	}
	sui := NewPlugin(nil, Sui, 10)
	sui.SetStateCanary(func() bool { return true })
	if len(sui.HealthChecks()) != 1 {
		t.Fatal("a chain with no canary declares none")
	}
}
