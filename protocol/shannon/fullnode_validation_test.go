package shannon

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sdk "github.com/pokt-network/shannon-sdk"
)

// fakePubKeyFetcher stands in for the account query against the full node.
// It counts calls, which is the whole point of the cache under test.
type fakePubKeyFetcher struct {
	mu sync.Mutex
	// answers maps address → public key. A missing address answers (nil, nil),
	// which is how the SDK reports an account that has never signed onchain.
	answers map[string]cryptotypes.PubKey
	// err, when set, fails every lookup — a full node that is down.
	err error
	// calls counts lookups that reached this fetcher rather than the cache.
	calls int
}

func (f *fakePubKeyFetcher) GetPubKeyFromAddress(_ context.Context, address string) (cryptotypes.PubKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.answers[address], nil
}

func (f *fakePubKeyFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakePubKeyFetcher) setAnswer(address string, pubKey cryptotypes.PubKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.answers[address] = pubKey
}

// supplierKey is a supplier identity: a key pair plus the address it publishes.
type supplierKey struct {
	priv    cryptotypes.PrivKey
	pub     cryptotypes.PubKey
	address string
}

func newSupplierKey() supplierKey {
	priv := secp256k1.GenPrivKey()
	pub := priv.PubKey()
	// Derive the address from the key rather than hardcoding a bech32 string,
	// so the test does not depend on which HRP the SDK config carries.
	return supplierKey{priv: priv, pub: pub, address: sdktypes.AccAddress(pub.Address()).String()}
}

// signedRelayResponse builds the wire bytes of a RelayResponse signed by key.
func signedRelayResponse(t *testing.T, key supplierKey, payload []byte) []byte {
	t.Helper()

	appAddr := sdktypes.AccAddress(secp256k1.GenPrivKey().PubKey().Address()).String()
	resp := &servicetypes.RelayResponse{
		Meta: servicetypes.RelayResponseMetadata{
			SessionHeader: &sessiontypes.SessionHeader{
				ApplicationAddress:      appAddr,
				ServiceId:               "eth",
				SessionId:               "session-1",
				SessionStartBlockHeight: 1,
				SessionEndBlockHeight:   10,
			},
		},
		Payload: payload,
	}

	hash, err := resp.GetSignableBytesHash()
	if err != nil {
		t.Fatalf("signable hash: %v", err)
	}
	sig, err := key.priv.Sign(hash[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	resp.Meta.SupplierOperatorSignature = sig

	bz, err := resp.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return bz
}

// newValidatingFullNode returns a FullNode wired to fetcher. Only the pubkey
// cache and logger are used by ValidateRelayResponse.
func newValidatingFullNode(fetcher *fakePubKeyFetcher) *FullNode {
	logger := newTestLogger()
	return &FullNode{
		pubKeys: newPubKeyCache(fetcher, logger),
		logger:  logger,
	}
}

// The property the signature exists for: a response verifies only against the
// key of the supplier we selected. The response carries no address of its own
// to trust, so substituting any other signer fails.
func TestValidateRelayResponse_SignerMustBeTheSelectedSupplier(t *testing.T) {
	selected := newSupplierKey()
	impostor := newSupplierKey()

	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{
		selected.address: selected.pub,
		impostor.address: impostor.pub,
	}}
	fn := newValidatingFullNode(fetcher)

	responseBz := signedRelayResponse(t, selected, []byte(`{"result":"0x1"}`))

	resp, err := fn.ValidateRelayResponse(selected.address, responseBz)
	if err != nil {
		t.Fatalf("response from the selected supplier should verify: %v", err)
	}
	if string(resp.Payload) != `{"result":"0x1"}` {
		t.Errorf("payload = %q", resp.Payload)
	}

	// Same bytes, but attributed to a supplier that did not sign them.
	if _, err = fn.ValidateRelayResponse(impostor.address, responseBz); err == nil {
		t.Fatal("a response signed by another key must not verify")
	}
	// Asserted through isSignatureError, not errors.Is: the SDK puts this
	// sentinel in the message with %s rather than in the chain with %w, so
	// errors.Is is false here even though the error is exactly that one. If this
	// assertion ever starts holding for errors.Is too, the SDK has been fixed
	// and the fallback in isSignatureError can go.
	if errors.Is(err, sdk.ErrRelayResponseValidationSignatureError) {
		t.Log("shannon-sdk now wraps the signature sentinel; isSignatureError can be simplified")
	}
	if !isSignatureError(err) {
		t.Errorf("want signature error, got %v", err)
	}
}

// The hot-path win: public keys are immutable, so one account query serves
// every subsequent relay from that supplier.
func TestValidateRelayResponse_CachesPublicKey(t *testing.T) {
	key := newSupplierKey()
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{key.address: key.pub}}
	fn := newValidatingFullNode(fetcher)

	responseBz := signedRelayResponse(t, key, []byte(`ok`))

	for i := 0; i < 50; i++ {
		if _, err := fn.ValidateRelayResponse(key.address, responseBz); err != nil {
			t.Fatalf("relay %d: %v", i, err)
		}
	}

	if got := fetcher.callCount(); got != 1 {
		t.Errorf("full node queried %d times for 50 relays, want 1", got)
	}
}

// A supplier that had no public key when we first asked — it had never signed a
// transaction — recovers within a single relay once it does.
func TestValidateRelayResponse_RefetchesAfterNilPublicKey(t *testing.T) {
	key := newSupplierKey()
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{}}
	fn := newValidatingFullNode(fetcher)

	responseBz := signedRelayResponse(t, key, []byte(`ok`))

	// First lookup answers "no key onchain"; the refetch finds one.
	fn.pubKeys.fetcher = &sequencedFetcher{first: fetcher, onSecondCall: func() {
		fetcher.setAnswer(key.address, key.pub)
	}}

	if _, err := fn.ValidateRelayResponse(key.address, responseBz); err != nil {
		t.Fatalf("validation should recover after the refetch: %v", err)
	}
	if got := fetcher.callCount(); got != 2 {
		t.Errorf("full node queried %d times, want 2 (initial + refetch)", got)
	}
}

// A supplier whose signatures never verify must not cost a query per relay.
func TestValidateRelayResponse_RefetchIsRateLimited(t *testing.T) {
	signing := newSupplierKey()
	claimed := newSupplierKey()

	// The address we selected publishes a key that did not sign the response.
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{claimed.address: claimed.pub}}
	fn := newValidatingFullNode(fetcher)

	responseBz := signedRelayResponse(t, signing, []byte(`ok`))

	for i := 0; i < 20; i++ {
		if _, err := fn.ValidateRelayResponse(claimed.address, responseBz); err == nil {
			t.Fatalf("relay %d: bad signature should not verify", i)
		}
	}

	// One initial fetch plus one refetch, then the per-supplier interval holds.
	if got := fetcher.callCount(); got != 2 {
		t.Errorf("full node queried %d times for 20 bad relays, want 2", got)
	}
}

// A full node that cannot answer is our outage, not the supplier's fault: the
// error must stay attributable to us (see isSupplierValidationError).
func TestValidateRelayResponse_PubKeyFetchErrorIsNotTheSuppliersFault(t *testing.T) {
	key := newSupplierKey()
	fetcher := &fakePubKeyFetcher{
		answers: map[string]cryptotypes.PubKey{},
		err:     errors.New("connection refused"),
	}
	fn := newValidatingFullNode(fetcher)

	_, err := fn.ValidateRelayResponse(key.address, signedRelayResponse(t, key, []byte(`ok`)))
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !errors.Is(err, sdk.ErrRelayResponseValidationGetPubKey) {
		t.Fatalf("want pubkey fetch error, got %v", err)
	}
	if isSupplierValidationError(err) {
		t.Error("a full node outage must not blacklist the supplier")
	}
}

// sequencedFetcher runs a side effect before the second and later lookups,
// so a test can change what the full node would answer mid-validation.
type sequencedFetcher struct {
	first        sdk.PublicKeyFetcher
	onSecondCall func()
	calls        int
	mu           sync.Mutex
}

func (s *sequencedFetcher) GetPubKeyFromAddress(ctx context.Context, address string) (cryptotypes.PubKey, error) {
	s.mu.Lock()
	s.calls++
	if s.calls > 1 && s.onSecondCall != nil {
		s.onSecondCall()
	}
	s.mu.Unlock()

	return s.first.GetPubKeyFromAddress(ctx, address)
}
