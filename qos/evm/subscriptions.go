package evm

import (
	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/qos"
)

// ClassifyClientFrame implements qos.SubscriptionClassifier for the EVM
// pub/sub API: eth_subscribe opens, eth_unsubscribe(params[0]) closes.
func (p *Plugin) ClassifyClientFrame(data []byte) qos.ClientFrameInfo {
	switch method := qos.JSONRPCMethod(data); method {
	case "eth_subscribe":
		return qos.ClientFrameInfo{Action: qos.SubscriptionSubscribe, RequestID: qos.JSONRPCRequestID(data), Method: method}
	case "eth_unsubscribe":
		return qos.ClientFrameInfo{Action: qos.SubscriptionUnsubscribe, SubscriptionID: qos.JSONRPCFirstParam(data), Method: method}
	}
	return qos.ClientFrameInfo{}
}

// ClassifyEndpointFrame implements qos.SubscriptionClassifier. A subscribe
// response carries the subscription id as a hex string result; a
// notification is an eth_subscription call with params.subscription.
func (p *Plugin) ClassifyEndpointFrame(data []byte) qos.EndpointFrameInfo {
	if qos.JSONRPCMethod(data) == "eth_subscription" {
		return qos.EndpointFrameInfo{
			Kind:           qos.EndpointFrameNotification,
			SubscriptionID: gjson.GetBytes(data, "params.subscription").String(),
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
