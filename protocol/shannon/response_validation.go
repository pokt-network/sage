package shannon

import (
	"errors"
	"strings"

	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sdk "github.com/pokt-network/shannon-sdk"

	"github.com/pokt-network/sage/domain"
)

// Blacklist reasons. These are Prometheus label values, so the set is closed
// on purpose — see metrics.Recorder.RecordSupplierBlacklist.
const (
	blacklistReasonSignature       = "signature_error"
	blacklistReasonUnmarshal       = "unmarshal_error"
	blacklistReasonBasicValidation = "basic_validation_error"
	blacklistReasonNilPubKey       = "nil_pubkey"
	blacklistReasonPubKeyFetch     = "pubkey_fetch_error"
	blacklistReasonUnknown         = "unknown"
)

// isSignatureError reports whether validation failed because the signature does
// not verify against the supplier's public key.
//
// It cannot be a plain errors.Is. The SDK formats this one sentinel with %s
// where every other validation error uses %w (shannon-sdk relay.go, in
// ValidateRelayResponse), so the sentinel is in the message text but not in the
// error chain, and errors.Is never matches it. PATH classifies with errors.Is
// alone and therefore does not recognise bad signatures at all.
//
// The errors.Is call is kept first so this starts working the moment the SDK
// fixes the verb. Below it: rule out the sentinels that do wrap correctly, then
// match what is actually in the chain — VerifySupplierOperatorSignature returns
// a poktroll ErrServiceInvalidRelayResponse, which the SDK does wrap with %w.
func isSignatureError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sdk.ErrRelayResponseValidationSignatureError) {
		return true
	}
	if errors.Is(err, sdk.ErrRelayResponseValidationUnmarshal) ||
		errors.Is(err, sdk.ErrRelayResponseValidationBasicValidation) ||
		errors.Is(err, sdk.ErrRelayResponseValidationGetPubKey) ||
		errors.Is(err, sdk.ErrRelayResponseValidationNilSupplierPubKey) {
		return false
	}
	return errors.Is(err, servicetypes.ErrServiceInvalidRelayResponse) ||
		strings.Contains(err.Error(), sdk.ErrRelayResponseValidationSignatureError.Error())
}

// isSupplierValidationError reports whether a relay response validation failure
// is attributable to the supplier, and so should blacklist it.
//
// It covers every failure mode of sdk.ValidateRelayResponse except one:
// ErrRelayResponseValidationGetPubKey means *our* full node did not answer the
// account query. Blacklisting for that turns a local outage into a service-wide
// supplier purge — every relay would fail validation, blacklist its supplier,
// and after one pass there is nobody left to route to. (PATH blacklists on it;
// this is a deliberate divergence.)
//
// A nil public key is kept on the supplier's side of the line: it means the
// supplier's operator account has never signed a transaction, so nothing it
// returns can be verified, which is its problem to fix and ours to route around.
func isSupplierValidationError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, sdk.ErrRelayResponseValidationUnmarshal) ||
		errors.Is(err, sdk.ErrRelayResponseValidationBasicValidation) ||
		errors.Is(err, sdk.ErrRelayResponseValidationNilSupplierPubKey) ||
		isSignatureError(err)
}

// isPubKeyRelatedError reports whether a failure could be explained by the
// public key we verified against rather than by the response, and is therefore
// worth one retry against a freshly fetched key.
//
// A signature mismatch qualifies: the cached answer may be the "no key onchain"
// one taken before the supplier's first transaction.
func isPubKeyRelatedError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, sdk.ErrRelayResponseValidationGetPubKey) ||
		errors.Is(err, sdk.ErrRelayResponseValidationNilSupplierPubKey) ||
		isSignatureError(err)
}

// blacklistReason maps a validation failure to its metric label.
func blacklistReason(err error) string {
	switch {
	case err == nil:
		return blacklistReasonUnknown
	case errors.Is(err, sdk.ErrRelayResponseValidationUnmarshal):
		return blacklistReasonUnmarshal
	case errors.Is(err, sdk.ErrRelayResponseValidationBasicValidation):
		return blacklistReasonBasicValidation
	case errors.Is(err, sdk.ErrRelayResponseValidationNilSupplierPubKey):
		return blacklistReasonNilPubKey
	case errors.Is(err, sdk.ErrRelayResponseValidationGetPubKey):
		return blacklistReasonPubKeyFetch
	case isSignatureError(err):
		return blacklistReasonSignature
	default:
		return blacklistReasonUnknown
	}
}

// validationErrorKind maps a validation failure to the domain error kind that
// carries the right attribution.
//
// A failed pubkey fetch is our full node, not the wire and not the supplier, so
// it is ErrTransport; everything else is a Shannon-level verification failure.
func validationErrorKind(err error) domain.ErrorKind {
	if errors.Is(err, sdk.ErrRelayResponseValidationGetPubKey) {
		return domain.ErrTransport
	}
	return domain.ErrProtocol
}

// supplierMetrics records supplier-attributable protocol events. Satisfied by
// metrics.Recorder; the protocol package does not import it, so that metrics
// stay optional and the protocol keeps no opinion about Prometheus.
type supplierMetrics interface {
	RecordSupplierBlacklist(serviceID domain.ServiceID, reason string)
	RecordRelayMinerError(serviceID domain.ServiceID, codespace string)
}

// noopSupplierMetrics is the default, so no call site needs a nil check.
type noopSupplierMetrics struct{}

func (noopSupplierMetrics) RecordSupplierBlacklist(domain.ServiceID, string) {}
func (noopSupplierMetrics) RecordRelayMinerError(domain.ServiceID, string)   {}

// SetMetrics attaches a metrics recorder to the protocol. Not safe to call
// concurrently with relays; call it at wire time.
func (p *Protocol) SetMetrics(m supplierMetrics) {
	if m == nil {
		m = noopSupplierMetrics{}
	}
	p.metrics = m
}

// supplierMetricsRecorder makes the zero value of the metrics field safe: a
// Protocol built as a struct literal (tests do this) has never had SetMetrics
// called on it, and a nil interface would panic rather than skip a metric.
func (p *Protocol) supplierMetricsRecorder() supplierMetrics {
	if p.metrics == nil {
		return noopSupplierMetrics{}
	}
	return p.metrics
}

// handleValidationFailure applies the blacklist policy for a relay response
// that failed verification and returns the error to propagate. Shared by the
// HTTP and WebSocket paths so the two cannot drift into different policies.
//
// logAttrs are appended to the log entry as slog key/value pairs.
func (p *Protocol) handleValidationFailure(
	serviceID domain.ServiceID,
	endpointAddr domain.EndpointAddr,
	supplierAddr string,
	err error,
	logAttrs ...any,
) *domain.RelayError {
	reason := blacklistReason(err)
	blacklisted := isSupplierValidationError(err)

	attrs := append([]any{
		"component", "shannon",
		"service_id", serviceID,
		"endpoint_addr", endpointAddr,
		"supplier_addr", supplierAddr,
		"reason", reason,
		"blacklisted", blacklisted,
		"error", err,
	}, logAttrs...)
	p.logger.Error("relay response validation failed", attrs...)

	if blacklisted {
		p.bl.BlacklistSupplier(serviceID, supplierAddr)
		p.supplierMetricsRecorder().RecordSupplierBlacklist(serviceID, reason)
	}

	return domain.NewRelayError(validationErrorKind(err), "relay response validation failed: "+reason, err, true)
}

// trackRelayMinerError surfaces the miner's own error report, if any.
//
// Call it before branching on the validation error: the field is filled in on
// responses that then fail validation, and it is the only account of what went
// wrong inside the miner — without it a miner-side failure is indistinguishable
// from a backend one.
func (p *Protocol) trackRelayMinerError(
	serviceID domain.ServiceID,
	endpointAddr domain.EndpointAddr,
	supplierAddr string,
	resp *servicetypes.RelayResponse,
) {
	minerErr := relayMinerError(resp)
	if minerErr == nil {
		return
	}

	p.supplierMetricsRecorder().RecordRelayMinerError(serviceID, minerErr.Codespace)
	p.logger.Warn("relay miner reported an error",
		"component", "shannon",
		"service_id", serviceID,
		"endpoint_addr", endpointAddr,
		"supplier_addr", supplierAddr,
		"codespace", minerErr.Codespace,
		"code", minerErr.Code,
		"description", minerErr.Description,
		"message", minerErr.Message,
	)
}

// relayMinerError extracts the miner's own error report from a relay response.
//
// The relay miner reports its failures in this field rather than as a transport
// error, and fills it in on responses that then fail validation — so it must be
// read before branching on the validation error, or it is lost. Returns nil when
// there is nothing to report.
func relayMinerError(resp *servicetypes.RelayResponse) *servicetypes.RelayMinerError {
	if resp == nil {
		return nil
	}
	return resp.RelayMinerError
}
