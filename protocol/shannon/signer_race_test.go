package shannon

import (
	"encoding/hex"
	"sync"
	"testing"

	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sdk "github.com/pokt-network/shannon-sdk"
)

// Concurrent signing on one shared ring while sessions roll over. This is the
// production shape by design: ringCache hands every signer of an app the same
// *ring.Ring so the SDK's per-ring SignerContext cache hits, and hedging doubles
// the goroutines signing against it.
//
// Three defects lived on that shape before shannon-sdk b1ba68f, and this test
// under -race reports all of them on the older pin (9bf0b02):
//   - ClearSignerContextCache assigned a fresh sync.Map over one concurrent
//     signers were reading (a torn map; PATH crashed a pod on it),
//   - concurrent cache misses built the SignerContext for one ring at the same
//     time, normalizing shared curve points in place,
//   - go-dleq's Encode/Equals normalized their receiver in place on every
//     signature, so Serialize raced signInternal's reads of the ring's keys.
//
// Run under -race. It passes on the fixed pin and fails on the old one.
func TestSignRelayRequest_SharedRingAcrossRolloverIsRaceFree(t *testing.T) {
	const (
		signers = 8
		signs   = 60
	)

	app := newSupplierKey()
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{app.address: app.pub}}
	// The gateway signs with the app's own key here: a self-delegated app's
	// ring is [app, app], and a private key outside the ring cannot sign.
	sdkSigner, err := sdk.NewSignerFromHex(hex.EncodeToString(app.priv.Bytes()))
	if err != nil {
		t.Fatalf("sdk signer: %v", err)
	}
	rs := &relaySigner{
		sdkSigner: sdkSigner,
		pubKeys:   newPubKeyCache(fetcher, newTestLogger()),
		logger:    newTestLogger(),
	}
	appRecord := &apptypes.Application{Address: app.address}

	newReq := func() *servicetypes.RelayRequest {
		return &servicetypes.RelayRequest{
			Meta: servicetypes.RelayRequestMetadata{
				SessionHeader: &sessiontypes.SessionHeader{
					ApplicationAddress:      app.address,
					ServiceId:               "svc",
					SessionStartBlockHeight: 1,
					SessionEndBlockHeight:   100,
				},
			},
			Payload: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`),
		}
	}

	var signersWG, rolloverWG sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < signers; i++ {
		signersWG.Add(1)
		go func() {
			defer signersWG.Done()
			for n := 0; n < signs; n++ {
				if _, err := rs.signRelayRequest(t.Context(), newReq(), appRecord); err != nil {
					t.Errorf("sign %d: %v", n, err)
					return
				}
			}
		}()
	}

	// Rollovers evict the ring and clear the SDK cache while the signers run,
	// so every signer after an eviction misses the cache at once.
	rolloverWG.Add(1)
	go func() {
		defer rolloverWG.Done()
		for h := uint64(1000); ; h++ {
			select {
			case <-stop:
				return
			default:
				rs.evictStaleRingsOnRollover(h)
			}
		}
	}()

	signersWG.Wait()
	close(stop)
	rolloverWG.Wait()
}
