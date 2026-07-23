package heuristic

import "fmt"

// gRPC status codes (google.rpc.Code). Spelled out rather than imported so the
// heuristic package keeps no dependency on grpc-go — it classifies wire
// outcomes, it does not speak the protocol.
const (
	grpcOK                 = 0
	grpcCancelled          = 1
	grpcUnknown            = 2
	grpcInvalidArgument    = 3
	grpcDeadlineExceeded   = 4
	grpcNotFound           = 5
	grpcAlreadyExists      = 6
	grpcPermissionDenied   = 7
	grpcResourceExhausted  = 8
	grpcFailedPrecondition = 9
	grpcAborted            = 10
	grpcOutOfRange         = 11
	grpcUnimplemented      = 12
	grpcInternal           = 13
	grpcUnavailable        = 14
	grpcDataLoss           = 15
	grpcUnauthenticated    = 16
)

// AnalyzeGRPC classifies a gRPC relay outcome.
//
// This is the gRPC counterpart to Analyze, and it exists separately because a
// gRPC call does not report failure the way the JSON transports do: the body is
// framed protobuf and the outcome is a grpc-status that arrives beside it. Only
// a caller holding both can tell "the chain said no" apart from "the supplier
// is broken", and that distinction is the whole point — the same rule as
// ErrorAttribution elsewhere, applied to a different protocol.
//
// hasStatus is false when the response carried no grpc-status at all, which
// gRPC defines as OK.
func AnalyzeGRPC(body []byte, grpcStatus int, grpcMessage string, hasStatus bool) AnalysisResult {
	if !hasStatus || grpcStatus == grpcOK {
		return analyzeGRPC(body)
	}
	return grpcStatusResult(grpcStatus, grpcMessage)
}

// grpcStatusResult maps a non-OK gRPC status onto a retry/penalty decision.
//
// The split that matters: a status describing the *request* (not found, invalid
// argument, unauthenticated) is the client's or the chain's answer, and the
// supplier delivered it faithfully — retrying it on another supplier gets the
// same answer while charging a reputation penalty to each. A status describing
// the *serving* (unavailable, internal, data loss) is the supplier's problem
// and is worth both.
func grpcStatusResult(code int, message string) AnalysisResult {
	details := fmt.Sprintf("grpc-status %d (%s)", code, grpcCodeName(code))
	if message != "" {
		details += ": " + message
	}

	switch code {
	case grpcInvalidArgument, grpcNotFound, grpcAlreadyExists, grpcPermissionDenied,
		grpcFailedPrecondition, grpcOutOfRange, grpcUnauthenticated, grpcUnimplemented:
		// The chain answered. Nothing is wrong with the supplier.
		return AnalysisResult{
			ShouldRetry:        false,
			ShouldCircuitBreak: false,
			ShouldPenalize:     false,
			Attribution:        AttrClient,
			Confidence:         0.90,
			Reason:             "grpc_request_error",
			Details:            details,
		}

	case grpcCancelled, grpcDeadlineExceeded:
		// Ran out of time. Worth another supplier, but a deadline says little
		// about whether this one is unhealthy, so the penalty stays light.
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityMinor,
			Attribution:        AttrSupplier,
			Confidence:         0.70,
			Reason:             "grpc_timeout",
			Details:            details,
		}

	case grpcResourceExhausted:
		// Busy, not broken — the same reading as HTTP 429.
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityMinor,
			Attribution:        AttrSupplier,
			Confidence:         0.85,
			Reason:             "grpc_resource_exhausted",
			Details:            details,
		}

	case grpcUnavailable, grpcInternal, grpcDataLoss, grpcAborted, grpcUnknown:
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityCritical,
			Attribution:        AttrSupplier,
			Confidence:         0.85,
			Reason:             "grpc_backend_error",
			Details:            details,
		}

	default:
		// An unknown code is not evidence of anything. Retry without blaming.
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false,
			ShouldPenalize:     false,
			Attribution:        AttrUnknown,
			Confidence:         0.40,
			Reason:             "grpc_unknown_status",
			Details:            details,
		}
	}
}

// grpcCodeName renders a status code for logs and admin output.
func grpcCodeName(code int) string {
	names := map[int]string{
		grpcOK: "OK", grpcCancelled: "CANCELLED", grpcUnknown: "UNKNOWN",
		grpcInvalidArgument: "INVALID_ARGUMENT", grpcDeadlineExceeded: "DEADLINE_EXCEEDED",
		grpcNotFound: "NOT_FOUND", grpcAlreadyExists: "ALREADY_EXISTS",
		grpcPermissionDenied: "PERMISSION_DENIED", grpcResourceExhausted: "RESOURCE_EXHAUSTED",
		grpcFailedPrecondition: "FAILED_PRECONDITION", grpcAborted: "ABORTED",
		grpcOutOfRange: "OUT_OF_RANGE", grpcUnimplemented: "UNIMPLEMENTED",
		grpcInternal: "INTERNAL", grpcUnavailable: "UNAVAILABLE",
		grpcDataLoss: "DATA_LOSS", grpcUnauthenticated: "UNAUTHENTICATED",
	}
	if name, ok := names[code]; ok {
		return name
	}
	return "UNRECOGNIZED"
}
