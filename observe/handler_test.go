package observe_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/observe"
	"github.com/pokt-network/sage/qos"
)

// --- fakes ---

// fakePlugin implements qos.Plugin, qos.DataExtractor, and qos.BlockHeightTracker.
type fakePlugin struct {
	extractedBlock *uint64
	extractErr     error
	updatedAddr    domain.EndpointAddr
	updatedHeight  uint64
}

func (f *fakePlugin) ParseRequest(_ context.Context, _ *http.Request, _ []byte, _ domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}

func (f *fakePlugin) SelectEndpoints(endpoints domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return endpoints, nil
}

func (f *fakePlugin) ExtractData(_ domain.EndpointAddr, _, _ []byte) (*qos.ExtractedData, error) {
	if f.extractErr != nil {
		return nil, f.extractErr
	}
	return &qos.ExtractedData{BlockHeight: f.extractedBlock}, nil
}

func (f *fakePlugin) UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64) {
	f.updatedAddr = endpoint
	f.updatedHeight = height
}

func (f *fakePlugin) PerceivedBlockHeight() uint64 { return 0 }
func (f *fakePlugin) StartSync(_ context.Context)  {}

// fakePluginNoExtract implements only qos.Plugin (no DataExtractor).
type fakePluginNoExtract struct{}

func (f *fakePluginNoExtract) ParseRequest(_ context.Context, _ *http.Request, _ []byte, _ domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}
func (f *fakePluginNoExtract) SelectEndpoints(ep domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return ep, nil
}

// fakePluginExtractOnly implements DataExtractor but NOT BlockHeightTracker.
type fakePluginExtractOnly struct {
	extractedBlock *uint64
}

func (f *fakePluginExtractOnly) ParseRequest(_ context.Context, _ *http.Request, _ []byte, _ domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}
func (f *fakePluginExtractOnly) SelectEndpoints(ep domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return ep, nil
}
func (f *fakePluginExtractOnly) ExtractData(_ domain.EndpointAddr, _, _ []byte) (*qos.ExtractedData, error) {
	return &qos.ExtractedData{BlockHeight: f.extractedBlock}, nil
}

// --- helpers ---

func makeObs(serviceID domain.ServiceID, endpointAddr domain.EndpointAddr) observe.Observation {
	return observe.Observation{
		ServiceID:    serviceID,
		EndpointAddr: endpointAddr,
		Source:       observe.SourceRelay,
		RequestBody:  []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`),
		ResponseBody: []byte(`{"jsonrpc":"2.0","result":"0x3e8","id":1}`),
	}
}

func newRegistry(id domain.ServiceID, p qos.Plugin) *qos.Registry {
	reg := qos.NewRegistry()
	_ = reg.Register(id, p)
	return reg
}

// --- tests ---

func TestDefaultHandler_UpdatesBlockHeight(t *testing.T) {
	height := uint64(1000)
	plugin := &fakePlugin{extractedBlock: &height}
	reg := newRegistry("eth", plugin)

	h := observe.NewDefaultHandler(reg, slog.Default())
	obs := makeObs("eth", "pokt1supplier-https://rpc.example.com")

	if err := h.HandleObservation(context.Background(), obs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plugin.updatedHeight != height {
		t.Errorf("UpdateBlockHeight called with height %d, want %d", plugin.updatedHeight, height)
	}
	if plugin.updatedAddr != obs.EndpointAddr {
		t.Errorf("UpdateBlockHeight called with addr %q, want %q", plugin.updatedAddr, obs.EndpointAddr)
	}
}

func TestDefaultHandler_NoPlugin_Skips(t *testing.T) {
	reg := qos.NewRegistry() // empty registry

	h := observe.NewDefaultHandler(reg, slog.Default())
	obs := makeObs("eth", "pokt1supplier-https://rpc.example.com")

	// Should not error — just skip.
	if err := h.HandleObservation(context.Background(), obs); err != nil {
		t.Fatalf("unexpected error for missing plugin: %v", err)
	}
}

func TestDefaultHandler_PluginNotDataExtractor_Skips(t *testing.T) {
	plugin := &fakePluginNoExtract{}
	reg := newRegistry("eth", plugin)

	h := observe.NewDefaultHandler(reg, slog.Default())
	obs := makeObs("eth", "pokt1supplier-https://rpc.example.com")

	if err := h.HandleObservation(context.Background(), obs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDefaultHandler_ExtractReturnsNilBlock_SkipsUpdate(t *testing.T) {
	// extractedBlock is nil — no block height in extracted data.
	plugin := &fakePlugin{extractedBlock: nil}
	reg := newRegistry("eth", plugin)

	h := observe.NewDefaultHandler(reg, slog.Default())
	obs := makeObs("eth", "pokt1supplier-https://rpc.example.com")

	if err := h.HandleObservation(context.Background(), obs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plugin.updatedHeight != 0 {
		t.Errorf("UpdateBlockHeight should not have been called, but height=%d", plugin.updatedHeight)
	}
}

func TestDefaultHandler_ExtractError_ReturnsError(t *testing.T) {
	parseErr := errors.New("parse failed")
	plugin := &fakePlugin{extractErr: parseErr}
	reg := newRegistry("eth", plugin)

	h := observe.NewDefaultHandler(reg, slog.Default())
	obs := makeObs("eth", "pokt1supplier-https://rpc.example.com")

	err := h.HandleObservation(context.Background(), obs)
	if err == nil {
		t.Fatal("expected error from ExtractData, got nil")
	}
	if !errors.Is(err, parseErr) {
		t.Errorf("expected wrapped parse error, got: %v", err)
	}
}

func TestDefaultHandler_PluginExtractOnly_NoUpdate(t *testing.T) {
	// Plugin has ExtractData but not UpdateBlockHeight.
	height := uint64(777)
	plugin := &fakePluginExtractOnly{extractedBlock: &height}
	reg := newRegistry("eth", plugin)

	h := observe.NewDefaultHandler(reg, slog.Default())
	obs := makeObs("eth", "pokt1supplier-https://rpc.example.com")

	// Should not error — UpdateBlockHeight simply not called.
	if err := h.HandleObservation(context.Background(), obs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
