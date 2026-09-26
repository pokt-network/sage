package cosmos

import (
	"strings"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/qos"
)

// ClassifyClientFrame implements qos.SubscriptionClassifier for CometBFT's
// event API: subscribe {query}, unsubscribe {query}, unsubscribe_all.
//
// CometBFT assigns no subscription id. Events are delivered as responses
// carrying the ORIGINAL request id, so the request id is the subscription
// id — which is also why an unsubscribe cannot name one: it names the query,
// and the registry cannot map a query back to a request. An unsubscribe is
// therefore treated as unsubscribe_all; the cost is a stall detector that
// stays quiet on a connection that still holds other subscriptions, never a
// false alarm.
func (p *Plugin) ClassifyClientFrame(data []byte) qos.ClientFrameInfo {
	switch method := qos.JSONRPCMethod(data); method {
	case "subscribe":
		// The topic is the query's tm.event ("NewBlock", "Tx", …). Only the
		// block events fire on every block; a Tx query fires on matches.
		event := cometEvent(data)
		return qos.ClientFrameInfo{
			Action:    qos.SubscriptionSubscribe,
			RequestID: qos.JSONRPCRequestID(data),
			Method:    method,
			Topic:     event,
			Periodic:  event == "NewBlock" || event == "NewBlockHeader",
		}
	case "unsubscribe", "unsubscribe_all":
		return qos.ClientFrameInfo{Action: qos.SubscriptionUnsubscribeAll, Method: method}
	}
	return qos.ClientFrameInfo{}
}

// ClassifyEndpointFrame implements qos.SubscriptionClassifier. Both the
// subscribe acknowledgement and every event carry the request id; they are
// told apart by shape — the ack's result is an empty object, an event's
// result carries query and data.
func (p *Plugin) ClassifyEndpointFrame(data []byte) qos.EndpointFrameInfo {
	id := qos.JSONRPCRequestID(data)
	if id == "" {
		return qos.EndpointFrameInfo{}
	}
	if qos.JSONRPCHasError(data) {
		return qos.EndpointFrameInfo{Kind: qos.EndpointFrameResponse, RequestID: id, IsError: true}
	}
	result := gjson.GetBytes(data, "result")
	if !result.Exists() || !result.IsObject() {
		return qos.EndpointFrameInfo{Kind: qos.EndpointFrameResponse, RequestID: id}
	}
	if result.Get("query").Exists() && result.Get("data").Exists() {
		// Events carry the subscription's request id AS their id, so after a
		// rebind that is the replay id and must be rewritten to the client's.
		return qos.EndpointFrameInfo{Kind: qos.EndpointFrameNotification, SubscriptionID: id, SubscriptionIDSpan: qos.JSONRPCRequestIDSpan(data)}
	}
	// {} — the subscribe acknowledgement. The registry only promotes ids it
	// has a pending subscribe for, so an empty-object result to some other
	// call opens nothing.
	return qos.EndpointFrameInfo{Kind: qos.EndpointFrameResponse, RequestID: id, SubscriptionID: id}
}

// cometEvent returns the tm.event a subscribe query names ("" when it names
// none). The query is params.query, or params[0] in the positional form, and
// reads like "tm.event='NewBlock' AND …".
func cometEvent(data []byte) string {
	query := gjson.GetBytes(data, "params.query").String()
	if query == "" {
		query = gjson.GetBytes(data, "params.0").String()
	}
	_, rest, ok := strings.Cut(query, "tm.event")
	if !ok {
		return ""
	}
	rest = strings.TrimLeft(rest, " =")
	if len(rest) == 0 || (rest[0] != '\'' && rest[0] != '"') {
		return ""
	}
	event, _, _ := strings.Cut(rest[1:], rest[:1])
	return event
}

// WebSocketProbe implements qos.WebSocketProber. status: CometBFT serves every RPC method over its WebSocket, and status is the cheapest.
func (p *Plugin) WebSocketProbe() []byte {
	return []byte(`{"jsonrpc":"2.0","id":1,"method":"status","params":{}}`)
}

var (
	_ qos.SubscriptionClassifier = (*Plugin)(nil)
	_ qos.WebSocketProber        = (*Plugin)(nil)
)
