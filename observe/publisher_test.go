package observe

import (
	"context"
	"testing"
	"time"
)

func TestChannelPublisher_PublishSubscribe(t *testing.T) {
	pub := NewChannelPublisher(10)
	ctx := context.Background()

	ch, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}

	blockHeight := uint64(100)
	obs := Observation{
		ServiceID:    "eth",
		EndpointAddr: "pokt1abc-https://example.com",
		Source:       SourceRelay,
		Extracted: &ExtractedData{
			BlockHeight: &blockHeight,
		},
	}

	if err := pub.Publish(ctx, obs); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-ch:
		if got.ServiceID != "eth" {
			t.Errorf("ServiceID = %q, want %q", got.ServiceID, "eth")
		}
		if got.Extracted == nil || *got.Extracted.BlockHeight != 100 {
			t.Error("Extracted data mismatch")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observation")
	}
}

func TestChannelPublisher_Close(t *testing.T) {
	pub := NewChannelPublisher(1)
	ctx := context.Background()

	if err := pub.Close(); err != nil {
		t.Fatal(err)
	}

	err := pub.Publish(ctx, Observation{})
	if err == nil {
		t.Error("expected error publishing to closed publisher")
	}
}

func TestChannelPublisher_Full(t *testing.T) {
	pub := NewChannelPublisher(1)
	ctx := context.Background()

	// Fill the channel.
	if err := pub.Publish(ctx, Observation{ServiceID: "a"}); err != nil {
		t.Fatal(err)
	}
	// Next publish should fail (channel full).
	if err := pub.Publish(ctx, Observation{ServiceID: "b"}); err == nil {
		t.Error("expected error when channel full")
	}
}
