package solana

import (
	"strings"

	"github.com/pokt-network/sage/qos"
)

// ClassifyClientFrame implements qos.SubscriptionClassifier for Solana's
// pub/sub API. Every subscribe method ends in "Subscribe" (accountSubscribe,
// logsSubscribe, slotSubscribe, …) and its twin in "Unsubscribe" with the
// integer subscription id as params[0]; the suffix is the contract, not a
// list this file could fall behind.
func (p *Plugin) ClassifyClientFrame(data []byte) qos.ClientFrameInfo {
	method := qos.JSONRPCMethod(data)
	switch {
	case strings.HasSuffix(method, "Unsubscribe"):
		id, span := qos.JSONRPCFirstParam(data)
		return qos.ClientFrameInfo{Action: qos.SubscriptionUnsubscribe, SubscriptionID: id, SubscriptionIDSpan: span, Method: method}
	case strings.HasSuffix(method, "Subscribe"):
		// Each Solana feed has its own method, so the method is the topic.
		// slot and root advance several times a second whatever happens on
		// chain; every other feed (account, logs, program, signature, block
		// with a filter) delivers only what matches.
		return qos.ClientFrameInfo{
			Action:    qos.SubscriptionSubscribe,
			RequestID: qos.JSONRPCRequestID(data),
			Method:    method,
			Topic:     method,
			Periodic:  method == "slotSubscribe" || method == "rootSubscribe",
		}
	}
	return qos.ClientFrameInfo{}
}

// ClassifyEndpointFrame implements qos.SubscriptionClassifier. A subscribe
// response carries the integer subscription id as its result; a notification
// is a "<x>Notification" call with params.subscription.
func (p *Plugin) ClassifyEndpointFrame(data []byte) qos.EndpointFrameInfo {
	if method := qos.JSONRPCMethod(data); strings.HasSuffix(method, "Notification") {
		id, span := qos.JSONRPCPath(data, "params.subscription")
		return qos.EndpointFrameInfo{Kind: qos.EndpointFrameNotification, SubscriptionID: id, SubscriptionIDSpan: span}
	}
	id := qos.JSONRPCRequestID(data)
	if id == "" {
		return qos.EndpointFrameInfo{}
	}
	return qos.EndpointFrameInfo{
		Kind:           qos.EndpointFrameResponse,
		RequestID:      id,
		SubscriptionID: qos.JSONRPCResultScalar(data),
		IsError:        qos.JSONRPCHasError(data),
	}
}

// WebSocketProbe implements qos.WebSocketProber. getSlot: served over the pub/sub socket by the Solana RPC, and cheap.
func (p *Plugin) WebSocketProbe() []byte {
	return []byte(`{"jsonrpc":"2.0","id":1,"method":"getSlot"}`)
}

var (
	_ qos.SubscriptionClassifier = (*Plugin)(nil)
	_ qos.WebSocketProber        = (*Plugin)(nil)
)
