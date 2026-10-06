package domain

// AnswerOrigin says who produced the answer a client got: the chain's node
// (its answer delivered as it was, error or not), a supplier that failed the
// relay, or the gateway answering on its own. A status alone cannot say it:
// CometBFT answers a GET /tx for an unknown hash with HTTP 500, which is the
// chain saying "not found", not a failure to serve, and counted as a failure
// it put a chain's not-found in the same bucket as the gateway failing.
type AnswerOrigin string

const (
	// OriginChain is a node's answer delivered as it was: a success, or an
	// error the heuristic attributes to the chain or to the client's request.
	OriginChain AnswerOrigin = "chain"
	// OriginSupplier is a relay a supplier failed: a gateway-made error after
	// at least one attempt, or a delivered answer graded the supplier's fault.
	OriginSupplier AnswerOrigin = "supplier"
	// OriginGateway is the gateway answering with no supplier involved: a
	// request refused before any attempt (validation, batch cap, no endpoint
	// to send to) or a static route.
	OriginGateway AnswerOrigin = "gateway"
)
