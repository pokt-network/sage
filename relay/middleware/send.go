package middleware

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/relay"
)

// SendRelay returns the terminal middleware that sends payloads to the selected
// endpoint via the provided Relayer. It is the innermost middleware and does
// NOT call next.
//
// For each payload in ctx.Payloads the relay is sent; the first successful
// response is stored in ctx.Response. If any send fails, the error is wrapped
// as a retryable RelayError and stored in ctx.Err.
func SendRelay(relayer protocol.Relayer) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			for _, payload := range ctx.Payloads {
				resp, err := relayer.SendRelay(ctx.Ctx, ctx.ServiceID, ctx.Endpoint, payload)
				if err != nil {
					relayErr := domain.NewRelayError(
						domain.ErrEndpoint,
						"relay send failed",
						err,
						true,
					)
					ctx.Err = relayErr
					return relayErr
				}
				// Store first response and stop; batching is handled upstream.
				if ctx.Response == nil {
					ctx.Response = resp
				}
				// The split inside the upstream call, for sage_stage_seconds_total:
				// these are a breakdown of send_relay's own time, not additions.
				if st := ctx.Stages; st != nil {
					st.Add("send_relay.prepare", resp.Phases.Prepare)
					st.Add("send_relay.sign", resp.Phases.Sign)
					st.Add("send_relay.http", resp.Phases.HTTP)
					st.Add("send_relay.verify", resp.Phases.Verify)
				}
			}
			return nil
		})
	}
}
