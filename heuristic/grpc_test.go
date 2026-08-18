package heuristic

import "testing"

// The rule that matters: a status describing the *request* is the chain's
// answer, faithfully delivered, and must not cost the supplier anything.
// Retrying it just asks another supplier the same question.
func TestAnalyzeGRPC_Attribution(t *testing.T) {
	body := []byte{0, 0, 0, 0, 4, 0x0a, 0x02, 0x10, 0x01} // a framed protobuf reply

	tests := []struct {
		name          string
		status        int
		hasStatus     bool
		wantRetry     bool
		wantPenalize  bool
		wantBreak     bool
		wantAttribute ErrorAttribution
	}{
		// successResult() reports AttrClient to mean "nothing to act on".
		{"absent status is OK", 0, false, false, false, false, AttrClient},
		{"explicit OK", grpcOK, true, false, false, false, AttrClient},

		{"not found", grpcNotFound, true, false, false, false, AttrClient},
		{"invalid argument", grpcInvalidArgument, true, false, false, false, AttrClient},
		{"unimplemented", grpcUnimplemented, true, false, false, false, AttrClient},
		{"unauthenticated", grpcUnauthenticated, true, false, false, false, AttrClient},

		{"deadline exceeded", grpcDeadlineExceeded, true, true, true, false, AttrSupplier},
		{"resource exhausted", grpcResourceExhausted, true, true, true, false, AttrSupplier},

		{"unavailable", grpcUnavailable, true, true, true, true, AttrSupplier},
		{"internal", grpcInternal, true, true, true, true, AttrSupplier},

		{"unrecognised code blames nobody", 99, true, true, false, false, AttrUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AnalyzeGRPC(body, tt.status, "detail", tt.hasStatus)

			if got.ShouldRetry != tt.wantRetry {
				t.Errorf("ShouldRetry = %v, want %v", got.ShouldRetry, tt.wantRetry)
			}
			if got.ShouldPenalize != tt.wantPenalize {
				t.Errorf("ShouldPenalize = %v, want %v", got.ShouldPenalize, tt.wantPenalize)
			}
			if got.ShouldCircuitBreak != tt.wantBreak {
				t.Errorf("ShouldCircuitBreak = %v, want %v", got.ShouldCircuitBreak, tt.wantBreak)
			}
			if got.Attribution != tt.wantAttribute {
				t.Errorf("Attribution = %v, want %v", got.Attribution, tt.wantAttribute)
			}
		})
	}
}

// Every other tier reads the body as text, and IsPlainText returns true for
// anything not starting with '{', '[' or '<' — so without a gRPC-specific path
// a correct protobuf reply is graded "plain_text_response" and retried.
func TestAnalyze_GRPCBodyIsNotJudgedAsText(t *testing.T) {
	protobufBody := []byte{0, 0, 0, 0, 4, 0x0a, 0x02, 0x10, 0x01}

	if !IsPlainText(protobufBody) {
		t.Fatal("precondition changed: this body no longer looks like plain text, so the test proves nothing")
	}

	got := Analyze(protobufBody, 200, "grpc")
	if got.ShouldRetry {
		t.Errorf("a valid gRPC reply was marked for retry: %s (%s)", got.Reason, got.Details)
	}
	if got.ShouldPenalize {
		t.Errorf("a valid gRPC reply penalized the supplier: %s", got.Reason)
	}
}

func TestAnalyzeGRPC_EmptyBody(t *testing.T) {
	got := AnalyzeGRPC(nil, 0, "", false)
	if !got.ShouldRetry || !got.ShouldPenalize {
		t.Errorf("an empty gRPC response should retry and penalize, got %+v", got)
	}
	if got.Reason != "empty_response" {
		t.Errorf("Reason = %q, want empty_response", got.Reason)
	}
}
