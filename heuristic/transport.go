package heuristic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"

	"github.com/pokt-network/sage/domain"
)

// AnalyzeTransportError grades an attempt that failed before any response
// body existed — the branch the Heuristic middleware used to return from
// without a verdict, which is why a hanging host never reached the circuit
// breaker and a client hang-up was scored against whichever supplier held
// the relay.
//
// requestCtxErr is the request context's Err() at the time of the failure.
// It is passed in because the relayer cannot tell a client hang-up from
// anything else: the same "context canceled" reaches it either way.
//
// Verdicts, in the order they are checked:
//
//   - cancelled: requestCtxErr is context.Canceled, or the attempt itself
//     failed with one. Nobody is waiting for the answer, so nobody is at
//     fault. No retry, no penalty, and Observe records no signal for
//     AttrClient with an error.
//   - connect-level: the dial itself failed (refused, DNS, TLS handshake).
//     The host is not serving anything: critical, ShouldCircuitBreak.
//   - timeout after connect: the host accepted the connection and did not
//     answer in time. The one failure that says "cannot do THIS": major,
//     retry elsewhere, MethodBlocking.
//   - other: session fetch, signing, relay-miner validation, unknown. Today's
//     grading — minor, AttrUnknown, retryable per the error itself.
func AnalyzeTransportError(err error, requestCtxErr error) AnalysisResult {
	// An endpoint cannot cancel our context; only we can. So a
	// context.Canceled is never evidence about the endpoint, whether it
	// arrives as the request context's error or as the attempt's own — the
	// second is the belt, and it is here rather than at a call site because
	// this is the one place a transport failure becomes a verdict.
	//
	// PATH hit the reachable version of this in September 2026 (PR #529): its
	// hedge repointed the primary's protocol context at a detached parent,
	// cancelled it on exit, and the batch fallthrough then reused the dead
	// context, so a relay that never left the process was charged to whichever
	// endpoint happened to be primary — one operator circuit-broken across
	// 12-18 pods for six hours. SAGE's hedge arms each run on their own clone
	// and their own detached context, cancelled only after the arm returns, and
	// there is no fallthrough, so no path here produces this today. The guard
	// is for the path nobody has drawn yet: without it such an error lands in
	// the catch-all below and costs an innocent supplier a minor penalty.
	//
	// context.DeadlineExceeded is deliberately not covered. A host that
	// accepted the connection and did not answer in time is a real signal, and
	// it is graded below.
	if errors.Is(requestCtxErr, context.Canceled) || errors.Is(err, context.Canceled) {
		return AnalysisResult{
			Attribution: AttrClient,
			Confidence:  0.95,
			Reason:      "client_cancelled",
			Details:     "the relay was cancelled before the endpoint answered",
		}
	}

	if isConnectFailure(err) {
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityCritical,
			Attribution:        AttrSupplier,
			Confidence:         0.90,
			Reason:             "transport_connect_failed",
			Details:            "could not connect to endpoint: " + err.Error(),
		}
	}

	if isTimeout(err) || errors.Is(requestCtxErr, context.DeadlineExceeded) {
		return AnalysisResult{
			ShouldRetry:     true,
			ShouldPenalize:  true,
			PenaltySeverity: SeverityMajor,
			Attribution:     AttrSupplier,
			Confidence:      0.85,
			Reason:          "transport_timeout",
			Details:         "endpoint accepted the connection and did not answer in time",
			MethodBlocking:  true,
		}
	}

	return AnalysisResult{
		ShouldRetry:     domain.IsRetryable(err),
		ShouldPenalize:  true,
		PenaltySeverity: SeverityMinor,
		Attribution:     AttrUnknown,
		Confidence:      0.50,
		Reason:          "transport_error",
		Details:         err.Error(),
	}
}

// isConnectFailure reports whether the error happened before any byte was
// exchanged with the host: a dial that was refused or timed out, a name that
// did not resolve, a TLS handshake that failed.
//
// The authoritative signal is domain.ConnectError, which the protocol layer
// attaches from an httptrace observation: a host that drops SYNs surfaces as
// the same http timeout as a host that accepted and went quiet, and no error
// shape tells them apart. The shape checks below remain for callers that do
// not go through the traced client.
//
// A dial timeout is a connect failure, not a method timeout — the check for
// Op == "dial" runs before the generic Timeout() check on purpose.
func isConnectFailure(err error) bool {
	var connErr *domain.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var recordErr tls.RecordHeaderError
	if errors.As(err, &recordErr) {
		return true
	}
	var certErr x509.CertificateInvalidError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	if errors.As(err, &certErr) || errors.As(err, &unknownAuthority) || errors.As(err, &hostnameErr) {
		return true
	}
	var tlsAlert tls.AlertError
	return errors.As(err, &tlsAlert)
}

// isTimeout reports whether the error is a deadline that fired after the
// connection was established: net/http's Client.Timeout, or a context
// deadline the transport surfaced.
func isTimeout(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}
