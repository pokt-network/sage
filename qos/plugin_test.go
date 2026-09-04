package qos

import (
	"context"
	"net/http"
	"time"

	"github.com/pokt-network/sage/domain"
)

// Compile-time interface checks — these verify that a full mock can implement
// all optional extension interfaces alongside the core Plugin interface.

type fullPlugin struct{}

func (f *fullPlugin) ParseRequest(_ context.Context, _ *http.Request, _ []byte, _ domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}

func (f *fullPlugin) SelectEndpoints(ep domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return ep, nil
}

func (f *fullPlugin) UpdateBlockHeight(_ domain.EndpointAddr, _ uint64) {}
func (f *fullPlugin) PerceivedBlockHeight() uint64                      { return 0 }
func (f *fullPlugin) StartSync(_ context.Context)                       {}
func (f *fullPlugin) IsArchivalRequest(_ []domain.Payload) bool         { return false }
func (f *fullPlugin) HealthChecks() []HealthCheck                       { return nil }
func (f *fullPlugin) ExtractData(_ domain.EndpointAddr, _, _ []byte) (*ExtractedData, error) {
	return nil, nil
}
func (f *fullPlugin) IsCoalescable(_ string) bool                         { return false }
func (f *fullPlugin) CacheTTL(_ string, _ []byte, _ []byte) time.Duration { return 0 }

// Compile-time assertions.
var (
	_ Plugin                = (*fullPlugin)(nil)
	_ BlockHeightTracker    = (*fullPlugin)(nil)
	_ ArchivalDetector      = (*fullPlugin)(nil)
	_ HealthChecker         = (*fullPlugin)(nil)
	_ DataExtractor         = (*fullPlugin)(nil)
	_ CoalescenceClassifier = (*fullPlugin)(nil)
	_ CachePolicy           = (*fullPlugin)(nil)
)
