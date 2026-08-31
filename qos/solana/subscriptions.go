package solana

import (
	"strings"

	"github.com/tidwall/gjson"

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
		return qos.ClientFrameInfo{Action: qos.SubscriptionUnsubscribe, SubscriptionID: qos.JSONRPCFirstParam(data), Method: method}
	case strings.HasSuffix(method, "Subscribe"):
		return qos.ClientFrameInfo{Action: qos.SubscriptionSubscribe, RequestID: qos.JSONRPCRequestID(data), Method: method}
	}
	return qos.ClientFrameInfo{}
}

// ClassifyEndpointFrame implements qos.SubscriptionClassifier. A subscribe
// response carries the integer subscription id as its result; a notification
// is a "<x>Notification" call with params.subscription.
func (p *Plugin) ClassifyEndpointFrame(data []byte) qos.EndpointFrameInfo {
	if method := qos.JSONRPCMethod(data); strings.HasSuffix(method, "Notification") {
		return qos.EndpointFrameInfo{
			Kind:           qos.EndpointFrameNotification,
			SubscriptionID: gjson.GetBytes(data, "params.subscription").Raw,
		}
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

var _ qos.SubscriptionClassifier = (*Plugin)(nil)
