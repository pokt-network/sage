package shannon

import (
	"cmp"
	"context"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
)

// wsFeedDenial is how the gateway answers a subscribe to one chain-wide feed:
// the per-service flag that has it answer, and the fixed JSON-RPC error.
type wsFeedDenial struct {
	flag    string
	code    int
	message string
}

// wsFeedDenials: method not found for a feed the service does not stream,
// invalid params for a logs subscribe that needs a filter.
var wsFeedDenials = map[qos.SubscribeFeed]wsFeedDenial{
	qos.FeedPendingTransactions: {featureflag.FlagWSDenyPendingTransactions, -32601, "pending transaction subscriptions are not available on this endpoint"},
	qos.FeedAllLogs:             {featureflag.FlagWSDenyUnfilteredLogs, -32602, "logs subscriptions need an address or topics filter on this endpoint"},
}

// subscribeDenial returns serviceID's answer to a client frame the bridge
// keeps from the supplier (websockets.WithClientAnswer): a JSON-RPC error for
// a subscribe to a feed whose flag is on for the service, nil for any other
// frame. Nil when the service's plugin classifies no subscriptions.
//
// A batch holding a denied subscribe is answered whole and sent nowhere: its
// other members get an error saying the batch was not sent, since a supplier
// cannot be asked for part of it.
func (r *WSRelayer) subscribeDenial(serviceID domain.ServiceID) func([]byte) []byte {
	var classifier qos.SubscriptionClassifier
	if r.deps.QoS != nil {
		classifier, _ = r.deps.QoS.Get(serviceID).(qos.SubscriptionClassifier)
	}
	if classifier == nil {
		return nil
	}
	// denial answers one request, "" when it is not denied.
	denial := func(frame []byte) string {
		info := classifier.ClassifyClientFrame(frame)
		d, ok := wsFeedDenials[info.Feed]
		if !ok || info.Action != qos.SubscriptionSubscribe || !r.deps.Flags.IsEnabled(context.Background(), d.flag, serviceID) {
			return ""
		}
		if r.deps.Metrics != nil {
			r.deps.Metrics.SubscriptionDenied(serviceID, info.Topic)
		}
		return jsonRPCError(info.RequestID, d.code, strconv.Quote(d.message))
	}
	return func(data []byte) []byte {
		frame := gjson.ParseBytes(data)
		if !frame.IsArray() {
			if answer := denial(data); answer != "" {
				return []byte(answer)
			}
			return nil
		}
		var answers []string
		denied := false
		for _, member := range frame.Array() {
			raw := []byte(member.Raw)
			if answer := denial(raw); answer != "" {
				answers, denied = append(answers, answer), true
			} else if id := qos.JSONRPCRequestID(raw); id != "" {
				answers = append(answers, jsonRPCError(id, -32600, `"batch not sent: it holds a subscription this endpoint does not serve"`))
			}
		}
		if !denied {
			return nil
		}
		return []byte("[" + strings.Join(answers, ",") + "]")
	}
}

// jsonRPCError is a JSON-RPC error response on a raw id; message is already
// a JSON string.
func jsonRPCError(id string, code int, message string) string {
	return `{"jsonrpc":"2.0","id":` + cmp.Or(id, "null") + `,"error":{"code":` + strconv.Itoa(code) + `,"message":` + message + `}}`
}
