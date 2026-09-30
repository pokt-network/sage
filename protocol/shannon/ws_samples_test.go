package shannon

import (
	"fmt"
	"testing"
)

func TestNotificationHash_NamesWhatANotificationIsAbout(t *testing.T) {
	for _, tc := range []struct{ name, payload, kind, hash string }{
		{"pending tx hash", `{"params":{"subscription":"0x1","result":"0xabc"}}`, "tx", "0xabc"},
		{"full pending tx", `{"params":{"subscription":"0x1","result":{"hash":"0xdef","nonce":"0x1"}}}`, "tx", "0xdef"},
		{"log", `{"params":{"subscription":"0x1","result":{"transactionHash":"0x123","logIndex":"0x0"}}}`, "tx", "0x123"},
		{"new head", `{"params":{"subscription":"0x1","result":{"hash":"0x456","parentHash":"0x455","number":"0x10"}}}`, "block", "0x456"},
		{"nothing to name", `{"params":{"subscription":"0x1","result":{"slot":7}}}`, "", ""},
	} {
		if kind, hash := notificationHash([]byte(tc.payload)); kind != tc.kind || hash != tc.hash {
			t.Errorf("%s: got (%q, %q), want (%q, %q)", tc.name, kind, hash, tc.kind, tc.hash)
		}
	}
}

// One notification in wsSampleEvery is kept per key, the last wsSampleKeep of
// them, oldest first.
func TestNotificationSamples_KeepsOneInEveryAndTheLastFew(t *testing.T) {
	s := newWSNotificationSamples()
	total := wsSampleEvery * (wsSampleKeep + 5)
	for i := 0; i < total; i++ {
		s.observe("eth", "op.example", "pokt1owner", "newPendingTransactions",
			[]byte(fmt.Sprintf(`{"params":{"result":"0x%x"}}`, i)))
	}
	got := s.snapshot("eth")
	if len(got) != wsSampleKeep {
		t.Fatalf("samples = %d, want %d", len(got), wsSampleKeep)
	}
	// The first sample is notification 0, then every wsSampleEvery-th; after
	// wrapping, the oldest kept is the 6th sample taken.
	if want := fmt.Sprintf("0x%x", 5*wsSampleEvery); got[0].Hash != want {
		t.Errorf("oldest kept = %s, want %s", got[0].Hash, want)
	}
	if got[0].Kind != "tx" || got[0].Operator != "op.example" || got[0].Owner != "pokt1owner" {
		t.Errorf("sample = %+v", got[0])
	}
	if other := s.snapshot("base"); len(other) != 0 {
		t.Errorf("another service's samples = %d, want 0", len(other))
	}
}

func TestNotificationSamples_KeyTableIsBounded(t *testing.T) {
	s := newWSNotificationSamples()
	for i := 0; i < wsSampleMaxKeys+10; i++ {
		s.observe("eth", fmt.Sprintf("op%d.example", i), "", "logs", []byte(`{"params":{"result":{"transactionHash":"0x1"}}}`))
	}
	if n := len(s.rings); n > wsSampleMaxKeys {
		t.Fatalf("keys = %d, want at most the cap %d", n, wsSampleMaxKeys)
	}
	// A key first seen after the table filled is still sampled.
	late := fmt.Sprintf("op%d.example", wsSampleMaxKeys+9)
	if s.rings[wsSampleKey{"eth", late, "", "logs"}] == nil {
		t.Fatal("a key seen after the table filled must be sampled")
	}
}
