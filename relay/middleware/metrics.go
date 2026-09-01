package middleware

import (
	"net/http"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// MetricsRecorder is the interface that metrics backends must implement.
// Implementations should be safe for concurrent use.
type MetricsRecorder interface {
	// RecordRelay records the outcome of a single relay attempt.
	// statusCode is the HTTP status code of the backend response (0 if unknown).
	// latency is the total time from the start of the relay to response receipt.
	// err is non-nil if the relay failed.
	RecordRelay(serviceID domain.ServiceID, endpoint domain.EndpointAddr, statusCode int, latency time.Duration, err error)
}

// Metrics returns a middleware that records one upstream attempt via recorder
// after it completes: status, the endpoint the attempt picked, and the
// attempt's own latency. It belongs inside retry, hedge and batch and outside
// select_endpoint — relay/chain_order.go enforces that — so a retried or
// hedged request is recorded once per attempt. The middleware always calls
// next.HandleRelay; recording is best-effort and never affects the returned
// error.
func Metrics(recorder MetricsRecorder) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			start := time.Now()

			err := next.HandleRelay(ctx)

			latency := time.Since(start)

			statusCode := 0
			if ctx.Response != nil {
				statusCode = ctx.Response.HTTPStatusCode
			} else if err != nil {
				// Use 502 Bad Gateway as a sentinel when the relay itself failed.
				statusCode = http.StatusBadGateway
			}

			recorder.RecordRelay(ctx.ServiceID, ctx.Endpoint, statusCode, latency, err)

			return err
		})
	}
}
