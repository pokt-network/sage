package shannon

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/url"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	sdk "github.com/pokt-network/shannon-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pokt-network/sage/config"
)

// FullNode wraps the Shannon SDK clients needed to send relays.
// It provides session fetching, block height queries, app lookups, and relay response validation.
type FullNode struct {
	sessionClient *sdk.SessionClient
	blockClient   *sdk.BlockClient
	accountClient *sdk.AccountClient
	appClient     *sdk.ApplicationClient
	sharedClient  *sdk.SharedClient
	logger        *slog.Logger
}

// NewFullNode constructs a FullNode from the provided configuration.
// It establishes gRPC connections to the full node for all required clients.
func NewFullNode(cfg config.FullNodeConfig, logger *slog.Logger) (*FullNode, error) {
	logger = logger.With("component", "fullnode")

	blockClient, err := newBlockClient(cfg.RPCURL)
	if err != nil {
		return nil, fmt.Errorf("NewFullNode: failed to create block client at %s: %w", cfg.RPCURL, err)
	}

	sessionClient, err := newSessionClient(cfg.GRPCConfig)
	if err != nil {
		return nil, fmt.Errorf("NewFullNode: failed to create session client at %s: %w", cfg.GRPCConfig.HostPort, err)
	}

	appClient, err := newAppClient(cfg.GRPCConfig)
	if err != nil {
		return nil, fmt.Errorf("NewFullNode: failed to create app client at %s: %w", cfg.GRPCConfig.HostPort, err)
	}

	accountClient, err := newAccountClient(cfg.GRPCConfig)
	if err != nil {
		return nil, fmt.Errorf("NewFullNode: failed to create account client at %s: %w", cfg.GRPCConfig.HostPort, err)
	}

	sharedClient, err := newSharedClient(cfg.GRPCConfig)
	if err != nil {
		return nil, fmt.Errorf("NewFullNode: failed to create shared client at %s: %w", cfg.GRPCConfig.HostPort, err)
	}

	return &FullNode{
		sessionClient: sessionClient,
		blockClient:   blockClient,
		accountClient: accountClient,
		appClient:     appClient,
		sharedClient:  sharedClient,
		logger:        logger,
	}, nil
}

// GetSession fetches the latest session for the given (serviceID, appAddr) pair.
func (fn *FullNode) GetSession(ctx context.Context, serviceID string, appAddr string) (*sessiontypes.Session, error) {
	session, err := fn.sessionClient.GetSession(ctx, appAddr, serviceID, 0)
	if err != nil {
		return nil, fmt.Errorf("GetSession: failed for service %s app %s: %w", serviceID, appAddr, err)
	}
	if session == nil {
		return nil, fmt.Errorf("GetSession: got nil session for service %s app %s", serviceID, appAddr)
	}
	return session, nil
}

// GetApp fetches the onchain application for the given address.
func (fn *FullNode) GetApp(ctx context.Context, appAddr string) (*apptypes.Application, error) {
	app, err := fn.appClient.GetApplication(ctx, appAddr)
	if err != nil {
		return nil, fmt.Errorf("GetApp: failed to get app %s: %w", appAddr, err)
	}
	return &app, nil
}

// GetCurrentBlockHeight returns the current block height from the full node.
func (fn *FullNode) GetCurrentBlockHeight(ctx context.Context) (int64, error) {
	height, err := fn.blockClient.LatestBlockHeight(ctx)
	if err != nil {
		return 0, fmt.Errorf("GetCurrentBlockHeight: failed: %w", err)
	}
	return height, nil
}

// GetSharedParams returns the shared module parameters from the blockchain.
func (fn *FullNode) GetSharedParams(ctx context.Context) (*sharedtypes.Params, error) {
	params, err := fn.sharedClient.GetParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetSharedParams: failed: %w", err)
	}
	return &params, nil
}

// ValidateRelayResponse validates the raw relay response bytes and verifies the supplier's signature.
func (fn *FullNode) ValidateRelayResponse(supplierAddr string, responseBz []byte) (*servicetypes.RelayResponse, error) {
	return sdk.ValidateRelayResponse(
		context.Background(),
		sdk.SupplierAddress(supplierAddr),
		responseBz,
		fn.accountClient,
	)
}

// AccountClient returns the account client, used for relay request signing.
func (fn *FullNode) AccountClient() *sdk.AccountClient {
	return fn.accountClient
}

// connectGRPC establishes a gRPC connection based on the provided config.
func connectGRPC(cfg config.GRPCConfig) (*grpc.ClientConn, error) {
	if cfg.Insecure {
		return grpc.NewClient(cfg.HostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return grpc.Dial( //nolint:staticcheck
		cfg.HostPort,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
	)
}

func newSessionClient(cfg config.GRPCConfig) (*sdk.SessionClient, error) {
	conn, err := connectGRPC(cfg)
	if err != nil {
		return nil, fmt.Errorf("newSessionClient: failed to connect gRPC at %s: %w", cfg.HostPort, err)
	}
	return &sdk.SessionClient{PoktNodeSessionFetcher: sdk.NewPoktNodeSessionFetcher(conn)}, nil
}

func newBlockClient(rpcURL string) (*sdk.BlockClient, error) {
	if _, err := url.Parse(rpcURL); err != nil {
		return nil, fmt.Errorf("newBlockClient: invalid URL %s: %w", rpcURL, err)
	}
	fetcher, err := sdk.NewPoktNodeStatusFetcher(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("newBlockClient: failed to create status fetcher for %s: %w", rpcURL, err)
	}
	return &sdk.BlockClient{PoktNodeStatusFetcher: fetcher}, nil
}

func newAppClient(cfg config.GRPCConfig) (*sdk.ApplicationClient, error) {
	conn, err := connectGRPC(cfg)
	if err != nil {
		return nil, fmt.Errorf("newAppClient: failed to connect gRPC at %s: %w", cfg.HostPort, err)
	}
	return &sdk.ApplicationClient{QueryClient: apptypes.NewQueryClient(conn)}, nil
}

func newAccountClient(cfg config.GRPCConfig) (*sdk.AccountClient, error) {
	conn, err := connectGRPC(cfg)
	if err != nil {
		return nil, fmt.Errorf("newAccountClient: failed to connect gRPC at %s: %w", cfg.HostPort, err)
	}
	return &sdk.AccountClient{PoktNodeAccountFetcher: sdk.NewPoktNodeAccountFetcher(conn)}, nil
}

func newSharedClient(cfg config.GRPCConfig) (*sdk.SharedClient, error) {
	conn, err := connectGRPC(cfg)
	if err != nil {
		return nil, fmt.Errorf("newSharedClient: failed to connect gRPC at %s: %w", cfg.HostPort, err)
	}
	return &sdk.SharedClient{QueryClient: sharedtypes.NewQueryClient(conn)}, nil
}
