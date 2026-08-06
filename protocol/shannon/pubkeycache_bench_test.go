package shannon

import (
	"context"
	"log/slog"
	"testing"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	accounttypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	sdk "github.com/pokt-network/shannon-sdk"
	grpcoptions "google.golang.org/grpc"
)

// staticAccountFetcher answers every account query from a pre-built response,
// with no network involved. It isolates what the SDK's account client costs per
// call once the round trip is taken out: the codec still unpacks the account
// Any and re-derives the public key every single time.
type staticAccountFetcher struct {
	resp *accounttypes.QueryAccountResponse
}

func (s *staticAccountFetcher) Account(
	_ context.Context,
	_ *accounttypes.QueryAccountRequest,
	_ ...grpcoptions.CallOption,
) (*accounttypes.QueryAccountResponse, error) {
	return s.resp, nil
}

// newBenchLogger discards output so logging never lands in the measurement.
func newBenchLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func newStaticAccountClient(b *testing.B, key supplierKey) *sdk.AccountClient {
	b.Helper()

	acct := &accounttypes.BaseAccount{Address: key.address}
	if err := acct.SetPubKey(key.pub); err != nil {
		b.Fatalf("set pubkey: %v", err)
	}
	any, err := codectypes.NewAnyWithValue(acct)
	if err != nil {
		b.Fatalf("pack account: %v", err)
	}
	return &sdk.AccountClient{
		PoktNodeAccountFetcher: &staticAccountFetcher{
			resp: &accounttypes.QueryAccountResponse{Account: any},
		},
	}
}

// BenchmarkValidateRelayResponse measures one full response verification. The
// two arms differ only in where the public key comes from; neither touches the
// network, so the gap is the codec work the cache removes and excludes the full
// node round trip it also removes.
func BenchmarkValidateRelayResponse(b *testing.B) {
	key := newSupplierKey()
	responseBz := signedRelayResponse(b, key, []byte(`{"result":"0x1"}`))

	b.Run("cached", func(b *testing.B) {
		fn := &FullNode{pubKeys: newPubKeyCache(newStaticAccountClient(b, key), newBenchLogger()), logger: newBenchLogger()}
		// Warm the cache so the loop measures steady state.
		if _, err := fn.ValidateRelayResponse(key.address, responseBz); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := fn.ValidateRelayResponse(key.address, responseBz); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("uncached", func(b *testing.B) {
		client := newStaticAccountClient(b, key)
		fn := &FullNode{pubKeys: newPubKeyCache(client, newBenchLogger()), logger: newBenchLogger()}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Drop the cached key so every iteration pays the account path,
			// as the pre-cache code did on every relay.
			fn.pubKeys.invalidate(key.address)
			if _, err := fn.ValidateRelayResponse(key.address, responseBz); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkAccountPubKeyLookup isolates the account client call itself.
func BenchmarkAccountPubKeyLookup(b *testing.B) {
	key := newSupplierKey()
	client := newStaticAccountClient(b, key)
	ctx := context.Background()

	b.Run("account_client", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := client.GetPubKeyFromAddress(ctx, key.address); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("cache_hit", func(b *testing.B) {
		cache := newPubKeyCache(client, newBenchLogger())
		if _, err := cache.GetPubKeyFromAddress(ctx, key.address); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := cache.GetPubKeyFromAddress(ctx, key.address); err != nil {
				b.Fatal(err)
			}
		}
	})
}
