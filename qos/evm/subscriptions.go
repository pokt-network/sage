package evm

import (
	"slices"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/qos"
)

// ClassifyClientFrame implements qos.SubscriptionClassifier for the EVM
// pub/sub API: eth_subscribe opens, eth_unsubscribe(params[0]) closes.
func (p *Plugin) ClassifyClientFrame(data []byte) qos.ClientFrameInfo {
	switch method := qos.JSONRPCMethod(data); method {
	case "eth_subscribe":
		// The topic is params[0]: newHeads, logs, newPendingTransactions.
		// Which one matters for accounting — a pending-transactions feed is
		// as chatty as the supplier's mempool, a heads feed is one frame a
		// block — so it is reported, verbatim; the metric bounds it.
		topic := gjson.GetBytes(data, "params.0").String()
		feed := qos.FeedNone
		switch {
		case topic == "newPendingTransactions":
			feed = qos.FeedPendingTransactions
		case topic == "logs" && unfilteredLogs(data):
			feed = qos.FeedAllLogs
		}
		return qos.ClientFrameInfo{
			Action:    qos.SubscriptionSubscribe,
			RequestID: qos.JSONRPCRequestID(data),
			Method:    method,
			Topic:     topic,
			// One header per block, always. logs and pendingTransactions
			// deliver only what matches, so their silence proves nothing.
			Periodic: topic == "newHeads",
			Feed:     feed,
		}
	case "eth_unsubscribe":
		id, span := qos.JSONRPCPath(data, "params.0")
		return qos.ClientFrameInfo{Action: qos.SubscriptionUnsubscribe, SubscriptionID: id, SubscriptionIDSpan: span, Method: method}
	}
	return qos.ClientFrameInfo{}
}

// unfilteredLogs reports whether a logs subscribe's filter (params[1]) names
// no address and no topic, which subscribes to every log the chain emits. A
// params[1] that is not an object is not a filter, and is the node's to
// answer.
func unfilteredLogs(data []byte) bool {
	filter := gjson.GetBytes(data, "params.1")
	if !filter.Exists() || filter.Type == gjson.Null {
		return true
	}
	return filter.IsObject() && !narrows(filter.Get("address")) && !narrows(filter.Get("topics"))
}

// narrows reports whether a filter value names anything: a non-empty string,
// or a list holding one. null and [] in a topics position match everything.
func narrows(v gjson.Result) bool {
	if v.IsArray() {
		return slices.ContainsFunc(v.Array(), narrows)
	}
	return v.Type == gjson.String && v.Str != ""
}

// ClassifyEndpointFrame implements qos.SubscriptionClassifier. A subscribe
// response carries the subscription id as a hex string result; a
// notification is an eth_subscription call with params.subscription.
func (p *Plugin) ClassifyEndpointFrame(data []byte) qos.EndpointFrameInfo {
	return qos.JSONRPCEndpointFrame(data, func(method string) bool { return method == "eth_subscription" })
}

// WebSocketProbe implements qos.WebSocketProber. eth_blockNumber: served by every EVM node over WebSocket, and cheap.
func (p *Plugin) WebSocketProbe() []byte {
	return []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
}

var (
	_ qos.SubscriptionClassifier = (*Plugin)(nil)
	_ qos.WebSocketProber        = (*Plugin)(nil)
)
