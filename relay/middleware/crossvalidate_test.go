package middleware

import (
	"net/http"
	"sync"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// fakeCrossValidator records calls to RecordDigest.
type fakeCrossValidator struct {
	mu      sync.Mutex
	records []digestRecord
}

type digestRecord struct {
	serviceID domain.ServiceID
	endpoint  domain.EndpointAddr
	method    string
	body      []byte
}

func (f *fakeCrossValidator) RecordDigest(serviceID domain.ServiceID, endpoint domain.EndpointAddr, method string, responseBody []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, digestRecord{
		serviceID: serviceID,
		endpoint:  endpoint,
		method:    method,
		body:      append([]byte(nil), responseBody...),
	})
}

func (f *fakeCrossValidator) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

func (f *fakeCrossValidator) last() digestRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.records) == 0 {
		return digestRecord{}
	}
	return f.records[len(f.records)-1]
}

func TestCrossValidate_RecordsDigestWhenFlagEnabled(t *testing.T) {
	v := &fakeCrossValidator{}
	flags := newFlags(featureflag.FlagCrossValidation)

	endpoint := domain.EndpointAddr("supplierA-https://node.example.com")
	responseBody := []byte(`{"result":"0x1"}`)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = endpoint
		ctx.Response = &domain.Response{
			HTTPStatusCode: http.StatusOK,
			Body:           responseBody,
		}
		ctx.Payloads = []domain.Payload{
			domain.NewPayload(nil, domain.RPCTypeJSONRPC, "eth_blockNumber"),
		}
		return nil
	})

	ctx := baseContext()
	ctx.ServiceID = "eth"

	mw := CrossValidate(flags, v)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v.count() != 1 {
		t.Fatalf("expected 1 recorded digest, got %d", v.count())
	}

	rec := v.last()
	if rec.serviceID != "eth" {
		t.Errorf("serviceID: got %q, want %q", rec.serviceID, "eth")
	}
	if rec.endpoint != endpoint {
		t.Errorf("endpoint: got %q, want %q", rec.endpoint, endpoint)
	}
	if rec.method != "eth_blockNumber" {
		t.Errorf("method: got %q, want %q", rec.method, "eth_blockNumber")
	}
	if string(rec.body) != string(responseBody) {
		t.Errorf("body: got %q, want %q", rec.body, responseBody)
	}
}

func TestCrossValidate_NoRecordWhenFlagDisabled(t *testing.T) {
	v := &fakeCrossValidator{}
	flags := newFlags() // cross_validation not enabled

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{
			HTTPStatusCode: http.StatusOK,
			Body:           []byte(`{"result":"0x1"}`),
		}
		return nil
	})

	ctx := baseContext()
	_ = CrossValidate(flags, v)(inner).HandleRelay(ctx)

	if v.count() != 0 {
		t.Errorf("expected 0 records when flag disabled, got %d", v.count())
	}
}

func TestCrossValidate_NoRecordWhenResponseNil(t *testing.T) {
	v := &fakeCrossValidator{}
	flags := newFlags(featureflag.FlagCrossValidation)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		// No response set.
		return nil
	})

	ctx := baseContext()
	_ = CrossValidate(flags, v)(inner).HandleRelay(ctx)

	if v.count() != 0 {
		t.Errorf("expected 0 records when response is nil, got %d", v.count())
	}
}

func TestCrossValidate_NoRecordWhenBodyEmpty(t *testing.T) {
	v := &fakeCrossValidator{}
	flags := newFlags(featureflag.FlagCrossValidation)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK, Body: nil}
		return nil
	})

	ctx := baseContext()
	_ = CrossValidate(flags, v)(inner).HandleRelay(ctx)

	if v.count() != 0 {
		t.Errorf("expected 0 records for empty body, got %d", v.count())
	}
}

func TestCrossValidate_PassesThroughError(t *testing.T) {
	v := &fakeCrossValidator{}
	flags := newFlags(featureflag.FlagCrossValidation)

	sentErr := domain.NewRelayError(domain.ErrTransport, "bang", nil, true)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusInternalServerError, Body: []byte(`error`)}
		return sentErr
	})

	ctx := baseContext()
	err := CrossValidate(flags, v)(inner).HandleRelay(ctx)
	if err != sentErr {
		t.Errorf("expected sentErr to be propagated, got %v", err)
	}
}

func TestCrossValidate_EmptyMethodWhenNoPayloads(t *testing.T) {
	v := &fakeCrossValidator{}
	flags := newFlags(featureflag.FlagCrossValidation)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://node.example.com"
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK, Body: []byte(`{}`)}
		// No payloads — method should default to "".
		return nil
	})

	ctx := baseContext()
	ctx.Payloads = nil

	_ = CrossValidate(flags, v)(inner).HandleRelay(ctx)

	if v.count() != 1 {
		t.Fatalf("expected 1 record, got %d", v.count())
	}
	if v.last().method != "" {
		t.Errorf("expected empty method when no payloads, got %q", v.last().method)
	}
}
