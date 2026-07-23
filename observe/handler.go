package observe

import (
	"context"
	"log/slog"

	"github.com/pokt-network/sage/qos"
)

// DefaultHandler processes observations by routing extracted data back to QoS plugins.
// For each observation it:
//  1. Looks up the QoS plugin for the observation's ServiceID from the registry.
//  2. If the plugin implements DataExtractor, calls ExtractData with the raw request/response bytes.
//  3. If extracted data contains a BlockHeight and the plugin implements BlockHeightTracker,
//     calls UpdateBlockHeight to propagate the result.
type DefaultHandler struct {
	registry *qos.Registry
	logger   *slog.Logger
}

// NewDefaultHandler creates a DefaultHandler backed by the given QoS registry.
func NewDefaultHandler(registry *qos.Registry, logger *slog.Logger) *DefaultHandler {
	return &DefaultHandler{
		registry: registry,
		logger:   logger.With("component", "obs_handler"),
	}
}

// HandleObservation implements Handler.
func (h *DefaultHandler) HandleObservation(ctx context.Context, obs Observation) error {
	plugin := h.registry.Get(obs.ServiceID)
	if plugin == nil {
		h.logger.Debug("no QoS plugin for service, skipping observation",
			"service_id", obs.ServiceID,
			"endpoint_addr", obs.EndpointAddr,
		)
		return nil
	}

	extractor, ok := plugin.(qos.DataExtractor)
	if !ok {
		return nil
	}

	extracted, err := extractor.ExtractData(obs.EndpointAddr, obs.RequestBody, obs.ResponseBody)
	if err != nil {
		h.logger.Error("failed to extract data from observation",
			"error", err,
			"service_id", obs.ServiceID,
			"endpoint_addr", obs.EndpointAddr,
			"source", obs.Source,
		)
		return err
	}

	if extracted == nil || extracted.BlockHeight == nil {
		return nil
	}

	tracker, ok := plugin.(qos.BlockHeightTracker)
	if !ok {
		return nil
	}

	tracker.UpdateBlockHeight(obs.EndpointAddr, *extracted.BlockHeight)
	h.logger.Debug("updated block height from observation",
		"service_id", obs.ServiceID,
		"endpoint_addr", obs.EndpointAddr,
		"block_height", *extracted.BlockHeight,
		"source", obs.Source,
	)

	return nil
}
