package shannon

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sdk "github.com/pokt-network/shannon-sdk"

	"github.com/pokt-network/sage/domain"
)

// recordingMetrics captures what the protocol reports, so tests can assert on
// policy rather than on log output.
type recordingMetrics struct {
	mu         sync.Mutex
	blacklists []string
	minerErrs  []string
}

func (r *recordingMetrics) RecordSupplierBlacklist(_ domain.ServiceID, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blacklists = append(r.blacklists, reason)
}

func (r *recordingMetrics) RecordRelayMinerError(_ domain.ServiceID, codespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.minerErrs = append(r.minerErrs, codespace)
}

func (r *recordingMetrics) snapshot() ([]string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.blacklists...), append([]string(nil), r.minerErrs...)
}

func TestValidationErrorClassification(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		blacklists  bool
		pubKeyRetry bool
		reason      string
		kind        domain.ErrorKind
	}{
		{
			name:        "unmarshal: the miner did not return a RelayResponse at all",
			err:         fmt.Errorf("%w: bad bytes", sdk.ErrRelayResponseValidationUnmarshal),
			blacklists:  true,
			pubKeyRetry: false,
			reason:      blacklistReasonUnmarshal,
			kind:        domain.ErrProtocol,
		},
		{
			name:        "basic validation: response is missing its session header",
			err:         fmt.Errorf("%w: missing meta", sdk.ErrRelayResponseValidationBasicValidation),
			blacklists:  true,
			pubKeyRetry: false,
			reason:      blacklistReasonBasicValidation,
			kind:        domain.ErrProtocol,
		},
		{
			name:        "nil pubkey: the supplier has never signed onchain",
			err:         fmt.Errorf("%w: pokt1x", sdk.ErrRelayResponseValidationNilSupplierPubKey),
			blacklists:  true,
			pubKeyRetry: true,
			reason:      blacklistReasonNilPubKey,
			kind:        domain.ErrProtocol,
		},
		{
			name:        "pubkey fetch: our own full node did not answer",
			err:         fmt.Errorf("%w: connection refused", sdk.ErrRelayResponseValidationGetPubKey),
			blacklists:  false,
			pubKeyRetry: true,
			reason:      blacklistReasonPubKeyFetch,
			kind:        domain.ErrTransport,
		},
		{
			name:        "signature, as the SDK actually formats it",
			err:         fmt.Errorf("%s: relay response failed signature verification: %w", sdk.ErrRelayResponseValidationSignatureError, servicetypes.ErrServiceInvalidRelayResponse.Wrap("invalid signature")),
			blacklists:  true,
			pubKeyRetry: true,
			reason:      blacklistReasonSignature,
			kind:        domain.ErrProtocol,
		},
		{
			name:        "signature, if the SDK ever switches to %w",
			err:         fmt.Errorf("%w: invalid signature", sdk.ErrRelayResponseValidationSignatureError),
			blacklists:  true,
			pubKeyRetry: true,
			reason:      blacklistReasonSignature,
			kind:        domain.ErrProtocol,
		},
		{
			name:        "unrecognized error: attribute to nobody rather than guess",
			err:         errors.New("something else entirely"),
			blacklists:  false,
			pubKeyRetry: false,
			reason:      blacklistReasonUnknown,
			kind:        domain.ErrProtocol,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSupplierValidationError(tc.err); got != tc.blacklists {
				t.Errorf("isSupplierValidationError = %v, want %v", got, tc.blacklists)
			}
			if got := isPubKeyRelatedError(tc.err); got != tc.pubKeyRetry {
				t.Errorf("isPubKeyRelatedError = %v, want %v", got, tc.pubKeyRetry)
			}
			if got := blacklistReason(tc.err); got != tc.reason {
				t.Errorf("blacklistReason = %q, want %q", got, tc.reason)
			}
			if got := validationErrorKind(tc.err); got != tc.kind {
				t.Errorf("validationErrorKind = %v, want %v", got, tc.kind)
			}
		})
	}
}

func TestHandleValidationFailure_BlacklistsOnlySupplierFaults(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		blocked bool
		reason  string
	}{
		{
			name:    "signature failure blacklists",
			err:     fmt.Errorf("%w: invalid signature", sdk.ErrRelayResponseValidationSignatureError),
			blocked: true,
			reason:  blacklistReasonSignature,
		},
		{
			// The whole point of the split: if our full node is down, every
			// relay fails validation. Blacklisting on that empties the pool.
			name:    "our full node being down blacklists nobody",
			err:     fmt.Errorf("%w: connection refused", sdk.ErrRelayResponseValidationGetPubKey),
			blocked: false,
			reason:  blacklistReasonPubKeyFetch,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			p := &Protocol{bl: newBlacklist(), metrics: rec, logger: newTestLogger()}

			relayErr := p.handleValidationFailure("eth", "endpoint-1", "pokt1supplier", tc.err)

			if got := p.bl.IsBlacklisted("eth", "pokt1supplier"); got != tc.blocked {
				t.Errorf("blacklisted = %v, want %v", got, tc.blocked)
			}
			blacklists, _ := rec.snapshot()
			if tc.blocked {
				if len(blacklists) != 1 || blacklists[0] != tc.reason {
					t.Errorf("recorded blacklists = %v, want [%s]", blacklists, tc.reason)
				}
			} else if len(blacklists) != 0 {
				t.Errorf("recorded blacklists = %v, want none", blacklists)
			}
			if !errors.Is(relayErr, tc.err) {
				t.Errorf("returned error should wrap the cause, got %v", relayErr)
			}
			if !domain.IsRetryable(relayErr) {
				t.Error("validation failures should stay retryable on another endpoint")
			}
		})
	}
}

// The miner reports its own failures in the response rather than as a transport
// error, and fills the field in on responses that then fail validation — so the
// report must survive the failure.
func TestTrackRelayMinerError(t *testing.T) {
	rec := &recordingMetrics{}
	p := &Protocol{bl: newBlacklist(), metrics: rec, logger: newTestLogger()}

	p.trackRelayMinerError("eth", "endpoint-1", "pokt1supplier", &servicetypes.RelayResponse{
		RelayMinerError: &servicetypes.RelayMinerError{
			Codespace:   "relayer_proxy",
			Code:        1,
			Description: "invalid session in relayer request",
			Message:     "application has no service config",
		},
	})
	// A response with nothing to report, and a nil response (validation failed
	// before the bytes parsed), must both be silent.
	p.trackRelayMinerError("eth", "endpoint-1", "pokt1supplier", &servicetypes.RelayResponse{})
	p.trackRelayMinerError("eth", "endpoint-1", "pokt1supplier", nil)

	_, minerErrs := rec.snapshot()
	if len(minerErrs) != 1 || minerErrs[0] != "relayer_proxy" {
		t.Errorf("recorded miner errors = %v, want [relayer_proxy]", minerErrs)
	}
}

func TestWSProcessor_RelayMinerErrorSurvivesValidationFailure(t *testing.T) {
	rec := &recordingMetrics{}
	fn := &mockRelayFullNode{
		// The SDK returns the parsed response alongside a basic-validation
		// error, which is exactly when the miner's own report matters most.
		validateResponse: &servicetypes.RelayResponse{
			RelayMinerError: &servicetypes.RelayMinerError{Codespace: "relayer_proxy", Code: 3},
		},
		validateErr: fmt.Errorf("%w: missing meta", sdk.ErrRelayResponseValidationBasicValidation),
	}
	p := &Protocol{
		fullNode: fn, signer: &countingSigner{}, bl: newBlacklist(),
		metrics: rec, logger: newTestLogger(),
	}
	proc := newWSMessageProcessor(
		t.Context(), p,
		&sessiontypes.SessionHeader{ServiceId: "eth", SessionEndBlockHeight: 200},
		"pokt1supplier", "endpoint-1", &apptypes.Application{Address: "pokt1app"}, nil,
	)

	if _, err := proc.ProcessEndpointMessage([]byte(`garbage`)); err == nil {
		t.Fatal("want validation error, got nil")
	}

	blacklists, minerErrs := rec.snapshot()
	if len(minerErrs) != 1 || minerErrs[0] != "relayer_proxy" {
		t.Errorf("recorded miner errors = %v, want [relayer_proxy]", minerErrs)
	}
	if len(blacklists) != 1 || blacklists[0] != blacklistReasonBasicValidation {
		t.Errorf("recorded blacklists = %v, want [%s]", blacklists, blacklistReasonBasicValidation)
	}
}
