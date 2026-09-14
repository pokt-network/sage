package shannon

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	apptypes "github.com/pokt-network/poktroll/x/application/types"

	"github.com/pokt-network/sage/domain"
)

// getApp returns a cached Application, fetching from the full node only on first call.
//
// Delegation lifecycle: The Application object contains the app's delegatee gateway
// addresses. These are used by the signer to build the ring signature (see signer.go).
// The ring cache is keyed by (appAddress, sessionEndHeight), so delegation changes
// that take effect at session boundaries will naturally produce new rings.
//
// However, the Application object itself is cached here indefinitely. If a delegation
// is added or removed on-chain, GetRingAddressesAtSessionEndHeight reads from the
// *cached* Application, which may be stale. In practice this is safe because:
//   - Delegation changes are rare (operational, not per-request)
//   - The gateway restarts on config changes anyway (new keys = new deploy)
//   - The ring addresses at sessionEndHeight are computed from the app's DelegateeGatewayAddresses
//     field, which only changes with an on-chain MsgDelegateToGateway/MsgUndelegateFromGateway
//
// TODO: Refresh the app cache at session boundaries (when the session cache refreshes)
//
//	to pick up any mid-flight delegation changes without requiring a restart.
func (p *Protocol) getApp(ctx context.Context, appAddr string) (*apptypes.Application, error) {
	if cached, ok := p.appCache.Load(appAddr); ok {
		return cached.(*apptypes.Application), nil
	}
	app, err := p.fullNode.GetApp(ctx, appAddr)
	if err != nil {
		return nil, err
	}
	p.appCache.Store(appAddr, app)
	return app, nil
}

// pickApp returns the first available app address for a service.
// For MVP, we use the first app in the list. Round-robin can be added later.
func (p *Protocol) pickApp(serviceID domain.ServiceID) (string, error) {
	apps, ok := p.ownedApps[serviceID]
	if !ok || len(apps) == 0 {
		return "", fmt.Errorf("no owned apps configured for service %s", serviceID)
	}
	return apps[0], nil
}

// secp256k1KeyLen is the exact byte length of a secp256k1 private key.
const secp256k1KeyLen = 32

// buildOwnedApps resolves the configured apps to addresses and fetches each
// one's staked service ID from the full node to populate the owned apps map.
func buildOwnedApps(fn *FullNode, privateKeysHex, addresses []string, logger *slog.Logger) (map[domain.ServiceID][]string, error) {
	appAddrs, err := ownedAppAddresses(privateKeysHex, addresses)
	if err != nil {
		return nil, err
	}
	// With no app there is nothing to relay for, and every service would
	// answer "no owned apps" per request instead of once, here.
	if len(appAddrs) == 0 {
		return nil, fmt.Errorf("buildOwnedApps: no owned apps: set gateway_config.owned_apps_addresses (or PATH's owned_apps_private_keys_hex)")
	}
	result := make(map[domain.ServiceID][]string)
	for _, appAddr := range appAddrs {
		app, err := fn.GetApp(context.Background(), appAddr)
		if err != nil {
			logger.Warn("buildOwnedApps: failed to get app", "app", appAddr, "error", err)
			return nil, fmt.Errorf("buildOwnedApps: failed to get app %s: %w", appAddr, err)
		}

		svcConfigs := app.GetServiceConfigs()
		if len(svcConfigs) != 1 {
			return nil, fmt.Errorf("buildOwnedApps: app %s must be staked for exactly one service, got %d", appAddr, len(svcConfigs))
		}

		svcID := domain.ServiceID(svcConfigs[0].GetServiceId())
		if svcID == "" {
			return nil, fmt.Errorf("buildOwnedApps: app %s has empty service ID", appAddr)
		}

		result[svcID] = append(result[svcID], appAddr)
	}
	return result, nil
}

// ownedAppAddresses is every configured app as an address, in config order:
// those derived from owned_apps_private_keys_hex first, then
// owned_apps_addresses, each app once. The private keys are only ever used
// here, to derive an address.
func ownedAppAddresses(privateKeysHex, addresses []string) ([]string, error) {
	var out []string
	seen := make(map[string]bool, len(privateKeysHex)+len(addresses))
	add := func(addr string) {
		if !seen[addr] {
			seen[addr] = true
			out = append(out, addr)
		}
	}
	for i, privKeyHex := range privateKeysHex {
		privKeyBz, err := hex.DecodeString(privKeyHex)
		if err != nil {
			return nil, fmt.Errorf("buildOwnedApps: private key at index %d is not hex (key redacted)", i)
		}
		// secp256k1 keys are exactly 32 bytes, and nothing downstream checks.
		// A wrong-length key still derives a valid-looking pokt1… address, so
		// the failure surfaces as "app not found" pointing at an address that
		// was never staked — sending you to look at staking, the full node and
		// the network rather than at a stray character in a hex string. Worst
		// case is a 33-byte key: the extra byte is ignored and it derives the
		// *correct* address, so nothing looks wrong at all.
		//
		// The length is the whole diagnostic; the key itself is never logged.
		if len(privKeyBz) != secp256k1KeyLen {
			return nil, fmt.Errorf(
				"buildOwnedApps: private key at index %d is %d bytes, want %d (key redacted)",
				i, len(privKeyBz), secp256k1KeyLen)
		}
		privKey := &secp256k1.PrivKey{Key: privKeyBz}
		addrBz := privKey.PubKey().Address()
		appAddr, err := bech32.ConvertAndEncode("pokt", addrBz)
		if err != nil {
			return nil, fmt.Errorf("buildOwnedApps: failed to encode address: %w", err)
		}
		add(appAddr)
	}
	for i, addr := range addresses {
		// Checked here rather than left to the full node: a typo would
		// otherwise surface as "app not found", which reads as a staking
		// problem rather than a config one.
		if prefix, _, err := bech32.DecodeAndConvert(addr); err != nil || prefix != "pokt" {
			return nil, fmt.Errorf("buildOwnedApps: owned_apps_addresses[%d] %q is not a pokt1… address", i, addr)
		}
		add(addr)
	}
	return out, nil
}
