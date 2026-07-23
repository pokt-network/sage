package observe

import (
	"encoding/json"
	"testing"
	"time"
)

func TestObservation_JSONSerialization(t *testing.T) {
	blockHeight := uint64(12345)
	chainID := "1"
	obs := Observation{
		ServiceID:    "eth",
		EndpointAddr: "pokt1abc-https://example.com",
		Timestamp:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Source:       SourceRelay,
		Latency:      150 * time.Millisecond,
		HTTPStatus:   200,
		RequestBody:  []byte(`{"method":"eth_blockNumber"}`),
		ResponseBody: []byte(`{"result":"0x3039"}`),
		Extracted: &ExtractedData{
			BlockHeight: &blockHeight,
			ChainID:     &chainID,
		},
	}

	data, err := json.Marshal(obs)
	if err != nil {
		t.Fatal(err)
	}

	// RequestBody and ResponseBody should NOT appear in JSON (json:"-").
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["request_body"]; ok {
		t.Error("RequestBody should not be serialized")
	}
	if _, ok := raw["response_body"]; ok {
		t.Error("ResponseBody should not be serialized")
	}

	// Round-trip.
	var decoded Observation
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ServiceID != obs.ServiceID {
		t.Errorf("ServiceID mismatch: got %q, want %q", decoded.ServiceID, obs.ServiceID)
	}
	if decoded.Source != SourceRelay {
		t.Errorf("Source mismatch: got %q, want %q", decoded.Source, SourceRelay)
	}
	if decoded.Extracted == nil || decoded.Extracted.BlockHeight == nil {
		t.Fatal("Extracted.BlockHeight should be present")
	}
	if *decoded.Extracted.BlockHeight != blockHeight {
		t.Errorf("BlockHeight mismatch: got %d, want %d", *decoded.Extracted.BlockHeight, blockHeight)
	}
}

func TestObservation_JSONWithoutExtracted(t *testing.T) {
	obs := Observation{
		ServiceID: "eth",
		Source:    SourceHealthCheck,
	}
	data, err := json.Marshal(obs)
	if err != nil {
		t.Fatal(err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["extracted"]; ok {
		t.Error("extracted should be omitted when nil")
	}
}
