package shannon

import (
	"context"
	"encoding/hex"
	"testing"

	"cosmossdk.io/depinject"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/pokt-network/poktroll/pkg/client"
	"github.com/pokt-network/poktroll/pkg/crypto/rings"
	"github.com/pokt-network/poktroll/pkg/polylog/polyzero"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
)

// The onchain lookups poktroll's ring client makes to verify a relay request.
// The embedded interfaces are nil, so a call to any other method panics: that
// is how this test learns the verifier started depending on something new.
type verifierAccounts struct {
	client.AccountQueryClient
	keys map[string]cryptotypes.PubKey
}

func (a verifierAccounts) GetPubKeyFromAddress(_ context.Context, address string) (cryptotypes.PubKey, error) {
	return a.keys[address], nil
}

type verifierApps struct {
	client.ApplicationQueryClient
	app apptypes.Application
}

func (a verifierApps) GetApplication(context.Context, string) (apptypes.Application, error) {
	return a.app, nil
}

type verifierShared struct{ client.SharedQueryClient }

func (verifierShared) GetParams(context.Context) (*sharedtypes.Params, error) {
	params := sharedtypes.DefaultParams()
	return &params, nil
}

// A relay SAGE signs must pass the relay miner's signature check, run here
// through poktroll's own ring client rather than a copy of its rules, so a
// poktroll upgrade that changes what a valid ring signature is fails here
// instead of at the relay miner.
func TestSignedRelayRequestPassesRelayMinerVerification(t *testing.T) {
	appKey := secp256k1.GenPrivKey()
	gatewayKey := secp256k1.GenPrivKey()
	appAddr := cosmostypes.AccAddress(appKey.PubKey().Address()).String()
	gatewayAddr := cosmostypes.AccAddress(gatewayKey.PubKey().Address()).String()
	keys := map[string]cryptotypes.PubKey{appAddr: appKey.PubKey(), gatewayAddr: gatewayKey.PubKey()}

	tests := []struct {
		name     string
		gateways []string
		signer   *secp256k1.PrivKey
	}{
		// No gateway: the ring is the app's key twice.
		{name: "app signs for itself", signer: appKey},
		{name: "delegated gateway signs", gateways: []string{gatewayAddr}, signer: gatewayKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := apptypes.Application{Address: appAddr, DelegateeGatewayAddresses: tt.gateways}

			verifier, err := rings.NewRingClient(depinject.Supply(
				polyzero.NewLogger(),
				verifierAccounts{keys: keys},
				verifierApps{app: app},
				verifierShared{},
			))
			if err != nil {
				t.Fatalf("ring client: %v", err)
			}
			signer, err := newRelaySigner(
				newPubKeyCache(&fakePubKeyFetcher{answers: keys}, newTestLogger()),
				hex.EncodeToString(tt.signer.Bytes()),
				newTestLogger(),
			)
			if err != nil {
				t.Fatalf("signer: %v", err)
			}

			params := sharedtypes.DefaultParams()
			req := &servicetypes.RelayRequest{
				Meta: servicetypes.RelayRequestMetadata{
					SessionHeader: &sessiontypes.SessionHeader{
						ApplicationAddress:      appAddr,
						ServiceId:               "svc",
						SessionId:               "session",
						SessionStartBlockHeight: 1,
						SessionEndBlockHeight:   sharedtypes.GetSessionEndHeight(&params, 1),
					},
					SupplierOperatorAddress: gatewayAddr,
				},
				Payload: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`),
			}
			signed, err := signer.signRelayRequest(context.Background(), req, &app)
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			if err := verifier.VerifyRelayRequestSignature(context.Background(), signed); err != nil {
				t.Fatalf("relay miner rejected the signature: %v", err)
			}
		})
	}
}
