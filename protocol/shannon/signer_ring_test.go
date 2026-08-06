package shannon

import (
	"encoding/hex"
	"testing"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	sdk "github.com/pokt-network/shannon-sdk"
)

// newRingSigner returns a signer whose public keys come from fetcher. The SDK
// signer is real because rollover eviction clears its context cache.
func newRingSigner(t *testing.T, fetcher *fakePubKeyFetcher) *relaySigner {
	t.Helper()

	sdkSigner, err := sdk.NewSignerFromHex(hex.EncodeToString(secp256k1.GenPrivKey().Bytes()))
	if err != nil {
		t.Fatalf("sdk signer: %v", err)
	}
	return &relaySigner{
		sdkSigner: sdkSigner,
		pubKeys:   newPubKeyCache(fetcher, newTestLogger()),
		logger:    newTestLogger(),
	}
}

// The point of sharing the cache with the response path: a ring is rebuilt at
// every session rollover because its composition may have changed, but the keys
// of the addresses in it cannot change, so the rebuild must not re-query.
func TestGetOrCreateRing_RolloverDoesNotRefetchKeys(t *testing.T) {
	app := newSupplierKey() // an app account is an account like any other
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{app.address: app.pub}}
	rs := newRingSigner(t, fetcher)

	appRecord := &apptypes.Application{Address: app.address}

	// Three consecutive sessions, each a fresh ring cache key.
	for i, sessionEnd := range []uint64{100, 200, 300} {
		if _, err := rs.getOrCreateRing(t.Context(), appRecord, sessionEnd); err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
	}

	if got := fetcher.callCount(); got != 1 {
		t.Errorf("full node queried %d times across 3 rollovers, want 1", got)
	}
}

func TestGetOrCreateRing_CachedRingIsReused(t *testing.T) {
	app := newSupplierKey()
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{app.address: app.pub}}
	rs := newRingSigner(t, fetcher)
	appRecord := &apptypes.Application{Address: app.address}

	first, err := rs.getOrCreateRing(t.Context(), appRecord, 100)
	if err != nil {
		t.Fatalf("first build: %v", err)
	}
	second, err := rs.getOrCreateRing(t.Context(), appRecord, 100)
	if err != nil {
		t.Fatalf("second build: %v", err)
	}
	if first != second {
		t.Error("same session should reuse the cached ring")
	}
}

// The guard: a "no key onchain" answer is cached, and a ring member without a
// key blocks signing entirely. Serving that answer for its full TTL would break
// the app for 15 minutes after the key appears, so the ring path drops it.
func TestGetOrCreateRing_NilPubKeyIsNotServedFromCache(t *testing.T) {
	app := newSupplierKey()
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{}}
	rs := newRingSigner(t, fetcher)
	appRecord := &apptypes.Application{Address: app.address}

	if _, err := rs.getOrCreateRing(t.Context(), appRecord, 100); err == nil {
		t.Fatal("a ring member with no onchain key should fail the build")
	}

	// The account signs its first transaction. The next attempt must see it
	// rather than the cached nil.
	fetcher.setAnswer(app.address, app.pub)

	if _, err := rs.getOrCreateRing(t.Context(), appRecord, 100); err != nil {
		t.Fatalf("build should recover once the key exists: %v", err)
	}
}

// ...but recovery must not cost a full node query per relay while the address
// stays keyless, which is what the refetch gate bounds.
func TestGetOrCreateRing_NilPubKeyRetriesAreRateLimited(t *testing.T) {
	app := newSupplierKey()
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{}}
	rs := newRingSigner(t, fetcher)
	appRecord := &apptypes.Application{Address: app.address}

	for i := 0; i < 50; i++ {
		if _, err := rs.getOrCreateRing(t.Context(), appRecord, uint64(100+i)); err == nil {
			t.Fatalf("attempt %d: want failure", i)
		}
	}

	if got := fetcher.callCount(); got > 2 {
		t.Errorf("full node queried %d times across 50 failed builds, want at most 2", got)
	}
}
