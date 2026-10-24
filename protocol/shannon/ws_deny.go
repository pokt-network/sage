package shannon

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

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
	classifier := r.classifier(serviceID)
	if classifier == nil {
		return nil
	}
	// denial answers one request, "" when it is not denied.
	denial := func(frame []byte) string {
		info, d, denied := r.deniedSubscribe(classifier, serviceID, frame)
		if !denied {
			return ""
		}
		if r.deps.Metrics != nil {
			r.deps.Metrics.SubscriptionDenied(serviceID, info.Topic)
		}
		return jsonRPCError(info.RequestID, d.code, jsonString(cmp.Or(r.denyMessage(serviceID), d.message)))
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

// holdsDeniedSubscription reports whether the subscriptions a rebind would
// replay include one serviceID's flags now deny: a flag turned on after the
// client subscribed.
func (r *WSRelayer) holdsDeniedSubscription(serviceID domain.ServiceID, subs *qos.SubscriptionRegistry) bool {
	classifier := r.classifier(serviceID)
	if classifier == nil {
		return false
	}
	return slices.ContainsFunc(subs.Subscribes(), func(frame []byte) bool {
		_, _, denied := r.deniedSubscribe(classifier, serviceID, frame)
		return denied
	})
}

// deniedSubscribe reads frame and reports whether it is a subscribe to a feed
// whose flag is on for serviceID, with how to answer it.
func (r *WSRelayer) deniedSubscribe(classifier qos.SubscriptionClassifier, serviceID domain.ServiceID, frame []byte) (qos.ClientFrameInfo, wsFeedDenial, bool) {
	info := classifier.ClassifyClientFrame(frame)
	d, ok := wsFeedDenials[info.Feed]
	return info, d, ok && info.Action == qos.SubscriptionSubscribe && r.deps.Flags.IsEnabled(context.Background(), d.flag, serviceID)
}

// classifier is serviceID's subscription classifier, nil when its plugin has
// none.
func (r *WSRelayer) classifier(serviceID domain.ServiceID) qos.SubscriptionClassifier {
	if r.deps.QoS == nil {
		return nil
	}
	c, _ := r.deps.QoS.Get(serviceID).(qos.SubscriptionClassifier)
	return c
}

// denyMessage is the operator's text for serviceID's denials, "" for the
// built-in wording.
func (r *WSRelayer) denyMessage(serviceID domain.ServiceID) string {
	if r.deps.DenyMessage == nil {
		return ""
	}
	return r.deps.DenyMessage(serviceID)
}

// maxCloseReason is the most a close frame's reason can hold: a control
// frame's 125-byte payload less the 2-byte code (RFC 6455 §5.5).
const maxCloseReason = 123

// denyCloseReason is the reason of the 1008 close a connection holding a
// denied subscription gets, cut on a character boundary to what a close frame
// holds.
func (r *WSRelayer) denyCloseReason(serviceID domain.ServiceID) string {
	reason := cmp.Or(r.denyMessage(serviceID), "subscription no longer served on this endpoint")
	if len(reason) <= maxCloseReason {
		return reason
	}
	cut := maxCloseReason
	for cut > 0 && !utf8.RuneStart(reason[cut]) {
		cut--
	}
	return reason[:cut]
}

// jsonString is s as a JSON string, escaped.
func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// jsonRPCError is a JSON-RPC error response on a raw id; message is already
// a JSON string.
func jsonRPCError(id string, code int, message string) string {
	return `{"jsonrpc":"2.0","id":` + cmp.Or(id, "null") + `,"error":{"code":` + strconv.Itoa(code) + `,"message":` + message + `}}`
}
