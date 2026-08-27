package middleware_test

import (
	"context"
	"net/http"
	"strings"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// --- Shared test helpers --- //

// newGETRequest creates a plain GET request.
func newGETRequest(path string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, "http://localhost"+path, nil)
	return req
}

// newPOSTRequest creates a POST request with the given body.
func newPOSTRequest(path, body string) *http.Request {
	req, _ := http.NewRequest(http.MethodPost, "http://localhost"+path, strings.NewReader(body))
	return req
}

// newWSRequest creates a WebSocket upgrade request.
func newWSRequest() *http.Request {
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/v1", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	return req
}

// newCtx builds a minimal relay.Context for tests.
func newCtx(req *http.Request) *relay.Context {
	writer := &mockWriter{}
	return relay.NewContext(context.Background(), req, nil, writer)
}

// mockWriter implements relay.ResponseWriter and records calls.
type mockWriter struct {
	headers    map[string]string
	statusCode int
	body       []byte
}

func (m *mockWriter) SetHeader(key, value string) {
	if m.headers == nil {
		m.headers = make(map[string]string)
	}
	m.headers[key] = value
}

func (m *mockWriter) SetStatusCode(code int) { m.statusCode = code }

func (m *mockWriter) Write(body []byte) error {
	m.body = body
	return nil
}

func (m *mockWriter) SetShadow(_ bool) {}

// --- Mock Plugin --- //

type mockPlugin struct {
	parseErr      error
	parsedPayload domain.Payload
	selectResult  domain.EndpointAddrList
	selectErr     error
}

func (p *mockPlugin) ParseRequest(_ context.Context, _ *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	if p.parseErr != nil {
		return nil, p.parseErr
	}
	if p.parsedPayload.Bytes() != nil {
		return []domain.Payload{p.parsedPayload}, nil
	}
	return []domain.Payload{domain.NewPayload(body, rpcType, "")}, nil
}

func (p *mockPlugin) SelectEndpoints(endpoints domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	if p.selectErr != nil {
		return nil, p.selectErr
	}
	if p.selectResult != nil {
		return p.selectResult, nil
	}
	return endpoints, nil
}

// --- Mock Relayer --- //

type mockRelayer struct {
	response *domain.Response
	err      error
}

func (m *mockRelayer) SendRelay(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.Payload) (*domain.Response, error) {
	return m.response, m.err
}

// --- Mock reputation.Service --- //

type mockRepService struct {
	bestEndpoint domain.EndpointAddr
}

var _ reputation.Service = (*mockRepService)(nil)

func (m *mockRepService) RecordSignal(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType, _ reputation.Signal) error {
	return nil
}

func (m *mockRepService) GetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType) (float64, error) {
	return 100, nil
}

func (m *mockRepService) GetScores(_ context.Context, _ domain.ServiceID) (map[string]float64, error) {
	return nil, nil
}

func (m *mockRepService) SelectBest(_ context.Context, _ domain.ServiceID, endpoints domain.EndpointAddrList, _ domain.RPCType) domain.EndpointAddr {
	if m.bestEndpoint != "" {
		return m.bestEndpoint
	}
	if len(endpoints) > 0 {
		return endpoints[0]
	}
	return ""
}

func (m *mockRepService) SelectSpread(_ context.Context, _ domain.ServiceID, endpoints domain.EndpointAddrList, _ domain.RPCType, _ map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(endpoints) > 0 {
		return endpoints[0]
	}
	return ""
}

func (m *mockRepService) ResetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr) error {
	return nil
}

func (m *mockRepService) Vouched(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType) bool {
	return true
}

// --- Mock featureflag.FlagStore --- //

type mockFlagStore struct {
	enabled map[string]bool
}

var _ featureflag.FlagStore = (*mockFlagStore)(nil)

func newMockFlags(flags map[string]bool) *mockFlagStore {
	return &mockFlagStore{enabled: flags}
}

func (m *mockFlagStore) IsEnabled(_ context.Context, flag string, _ domain.ServiceID) bool {
	if m.enabled == nil {
		return true
	}
	v, ok := m.enabled[flag]
	if !ok {
		return true
	}
	return v
}

func (m *mockFlagStore) Set(_ context.Context, _ string, _ bool) error { return nil }

func (m *mockFlagStore) SetForService(_ context.Context, _ string, _ domain.ServiceID, _ bool) error {
	return nil
}

func (m *mockFlagStore) GetAll(_ context.Context) (map[string]featureflag.FlagState, error) {
	return nil, nil
}

func (m *mockFlagStore) Delete(_ context.Context, _ string, _ domain.ServiceID) error { return nil }
func (m *mockFlagStore) DeleteGlobal(_ context.Context, _ string) error               { return nil }
