package evm

import (
	"testing"

	"github.com/pokt-network/sage/qos"
)

// Frames as geth emits them.
func TestSubscriptions_EVMRoundTrip(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":7,"method":"eth_subscribe","params":["newHeads"]}`))
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","id":7,"result":"0xcd0c3e8af590364c09d0fa6a1210faf5"}`))
	if a := r.Active(); len(a) != 1 || a[0].ID != "0xcd0c3e8af590364c09d0fa6a1210faf5" || a[0].Method != "eth_subscribe" {
		t.Fatalf("Active = %+v", a)
	}
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xcd0c3e8af590364c09d0fa6a1210faf5","result":{"number":"0x1"}}}`))
	if r.LastData().IsZero() {
		t.Fatal("newHeads notification must count as data")
	}
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":8,"method":"eth_unsubscribe","params":["0xcd0c3e8af590364c09d0fa6a1210faf5"]}`))
	if r.HasActive() {
		t.Fatal("eth_unsubscribe must close the subscription")
	}
}

func TestSubscriptions_EVMErrorAndPlainCalls(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["bogus"]}`))
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid subscription"}}`))
	if r.HasActive() {
		t.Fatal("a rejected subscribe is not a subscription")
	}
	// Ordinary request/response on the same socket: nothing to track.
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}`))
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","id":2,"result":"0x10"}`))
	if r.HasActive() {
		t.Fatal("a plain call's response must not open a subscription")
	}
}
