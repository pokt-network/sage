package evm

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// aggregate3Result encodes an aggregate3 answer: one (success, returnData)
// tuple per call, each returnData one 32-byte word.
func aggregate3Result(results ...struct {
	ok    bool
	value uint64
}) string {
	word := func(n uint64) string { return fmt.Sprintf("%064x", n) }
	var heads, tuples string
	at := uint64(32 * len(results))
	for _, r := range results {
		ok := uint64(0)
		if r.ok {
			ok = 1
		}
		t := word(ok) + word(0x40) + word(32) + word(r.value)
		heads += word(at)
		tuples += t
		at += uint64(len(t) / 2)
	}
	return `{"jsonrpc":"2.0","id":1,"result":"0x` + word(0x20) + word(uint64(len(results))) + heads + tuples + `"}`
}

type res = struct {
	ok    bool
	value uint64
}

// The bodies are ABI-correct (aggregate3 checked against an encoding verified
// on mainnet 2026-10-01), every one targets Multicall3, and each makes a
// single getCurrentBlockTimestamp call.
func TestCanaryBodies(t *testing.T) {
	want := "82ad56cb000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000020000000000000000000000000ca11bde05977b3631167028862be2a173976ca110000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000040f28c97d00000000000000000000000000000000000000000000000000000000"
	if !strings.Contains(string(canaryBodies[1]), want) {
		t.Fatalf("aggregate3(getCurrentBlockTimestamp) encodes as\n%s", canaryBodies[1])
	}
	for i, b := range canaryBodies {
		if !strings.Contains(string(b), `"to":"0x`+multicall3) || strings.Count(string(b), selGetCurrentBlockTimestamp) != 1 {
			t.Errorf("body %d: %s", i, b)
		}
	}
}

func TestCanaryTimestamp(t *testing.T) {
	ts := uint64(1_790_000_000)
	for _, tc := range []struct {
		name string
		body string
		ok   bool
	}{
		{"direct", fmt.Sprintf(`{"result":"0x%064x"}`, ts), true},
		{"aggregate3 one call", aggregate3Result(res{true, ts}), true},
		// Arbitrum's getBlockNumber inside eth_call is Ethereum's number; only
		// the last call, the timestamp, is read.
		{"aggregate3 block number first", aggregate3Result(res{true, 21_000_000}, res{true, ts}), true},
		{"last call failed", aggregate3Result(res{true, 5}, res{false, ts}), false},
		{"no Multicall3 on the chain", `{"result":"0x"}`, false},
		{"revert", `{"error":{"code":3,"message":"execution reverted"}}`, false},
	} {
		got, ok := canaryTimestamp([]byte(tc.body))
		if ok != tc.ok || (ok && got.Unix() != int64(ts)) {
			t.Errorf("%s: %v %v, want ok=%v", tc.name, got.Unix(), ok, tc.ok)
		}
	}
}

// A fresh answer is not stale; one a cache has held past two blocks plus the
// slack is. The plugin has a fast block rate here, so two blocks are ~0.
func TestCanaryHeadLag(t *testing.T) {
	p := newTestPlugin(5)
	p.UpdateBlockHeight("a1-https://x.a.net", 1000)
	p.UpdateBlockHeight("a1-https://x.a.net", 1010)
	canary := domain.NewPayload(canaryBodies[3], domain.RPCTypeJSONRPC, "eth_call")
	now := time.Now()
	answer := func(age time.Duration) []byte {
		return []byte(aggregate3Result(res{true, 21_000_000}, res{true, uint64(now.Add(-age).Unix())}))
	}
	if _, stale, ok := p.HeadLag(canary, answer(2*time.Second), now); !ok || stale {
		t.Fatalf("fresh answer: stale=%v ok=%v", stale, ok)
	}
	if _, stale, ok := p.HeadLag(canary, answer(2*time.Minute), now); !ok || !stale {
		t.Fatalf("answer held two minutes: stale=%v ok=%v", stale, ok)
	}
	other := domain.NewPayload([]byte(`{"method":"eth_call","params":[{"to":"0x1"},"latest"]}`), domain.RPCTypeJSONRPC, "eth_call")
	if _, _, ok := p.HeadLag(other, answer(time.Hour), now); ok {
		t.Fatal("a client eth_call is not the canary")
	}
}

func TestCanaryCheckFollowsTheFlag(t *testing.T) {
	has := func(p *Plugin) bool {
		for _, c := range p.HealthChecks() {
			if c.Name == CanaryName {
				return c.GradesHead
			}
		}
		return false
	}
	if has(newTestPlugin(5)) {
		t.Fatal("no flag: no canary")
	}
	on := true
	p := NewPlugin(nil, Config{StateCanary: func() bool { return on }})
	if !has(p) {
		t.Fatal("flag on: canary missing")
	}
	on = false
	if has(p) {
		t.Fatal("flag off: canary still sent")
	}
	seen := map[string]bool{}
	for i := 0; i < len(canaryBodies); i++ {
		seen[string(CanaryCheck(time.Unix(int64(i)*int64(canaryRotation/time.Second), 0)).Payload.Bytes())] = true
	}
	if len(seen) != len(canaryBodies) {
		t.Fatalf("rotation used %d of %d bodies", len(seen), len(canaryBodies))
	}
}
