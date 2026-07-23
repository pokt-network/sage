package middleware

import (
	"context"
	"sync/atomic"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// ---------------------------------------------------------------------------
// Mock Handler
// ---------------------------------------------------------------------------

// mockHandler is a test Handler that can be configured to succeed or fail.
// It records the endpoints it was called with for assertion.
type mockHandler struct {
	// responses is a slice of errors to return in order.
	// Once exhausted, always returns nil (success).
	responses []error
	calls     []domain.EndpointAddr
	callCount int32
}

func newMockHandler(responses ...error) *mockHandler {
	return &mockHandler{responses: responses}
}

func (m *mockHandler) HandleRelay(ctx *relay.Context) error {
	idx := int(atomic.AddInt32(&m.callCount, 1)) - 1
	m.calls = append(m.calls, ctx.Endpoint)

	// Auto-assign a fake endpoint if none is set, so retry can track it.
	if ctx.Endpoint == "" && len(ctx.Endpoints) > 0 {
		ctx.Endpoint = ctx.Endpoints[0]
	}

	if idx < len(m.responses) {
		err := m.responses[idx]
		if err != nil {
			return err
		}
	}
	// Success: set a dummy response.
	ctx.Response = &domain.Response{HTTPStatusCode: 200}
	return nil
}

func (m *mockHandler) Count() int { return int(atomic.LoadInt32(&m.callCount)) }

// ---------------------------------------------------------------------------
// Mock FlagStore
// ---------------------------------------------------------------------------

// mockFlags is a simple FlagStore where named flags are always enabled.
type mockFlags struct {
	enabled map[string]bool
}

func newFlags(flags ...string) *mockFlags {
	m := &mockFlags{enabled: make(map[string]bool)}
	for _, f := range flags {
		m.enabled[f] = true
	}
	return m
}

func (f *mockFlags) IsEnabled(_ context.Context, flag string, _ domain.ServiceID) bool {
	return f.enabled[flag]
}

func (f *mockFlags) Set(_ context.Context, flag string, enabled bool) error {
	f.enabled[flag] = enabled
	return nil
}

func (f *mockFlags) SetForService(_ context.Context, flag string, _ domain.ServiceID, enabled bool) error {
	f.enabled[flag] = enabled
	return nil
}

func (f *mockFlags) GetAll(_ context.Context) (map[string]featureflag.FlagState, error) {
	out := make(map[string]featureflag.FlagState, len(f.enabled))
	for k, v := range f.enabled {
		out[k] = featureflag.FlagState{Enabled: v}
	}
	return out, nil
}

func (f *mockFlags) Delete(_ context.Context, flag string, _ domain.ServiceID) error {
	delete(f.enabled, flag)
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// retryableErr creates a retryable RelayError.
func retryableErr(msg string) error {
	return domain.NewRelayError(domain.ErrTransport, msg, nil, true)
}

// nonRetryableErr creates a non-retryable RelayError.
func nonRetryableErr(msg string) error {
	return domain.NewRelayError(domain.ErrValidation, msg, nil, false)
}

// testEndpoints returns a slice of n fake endpoint addresses.
func testEndpoints(n int) domain.EndpointAddrList {
	eps := make(domain.EndpointAddrList, n)
	for i := range eps {
		eps[i] = domain.EndpointAddr("supplier" + string(rune('A'+i)) + "-https://node" + string(rune('A'+i)) + ".example.com")
	}
	return eps
}

// baseContext returns a minimal relay.Context ready for middleware tests.
func baseContext() *relay.Context {
	ctx := &relay.Context{}
	ctx.Ctx = context.Background()
	ctx.ServiceID = "eth"
	ctx.Endpoints = testEndpoints(3)
	return ctx
}
