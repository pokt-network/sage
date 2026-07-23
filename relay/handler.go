package relay

// Handler processes a relay request. This is the core abstraction.
// Analogous to http.Handler but for relay processing.
type Handler interface {
	HandleRelay(ctx *Context) error
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx *Context) error

func (f HandlerFunc) HandleRelay(ctx *Context) error { return f(ctx) }

// Middleware wraps a Handler to add behavior before/after.
type Middleware func(next Handler) Handler

// Chain composes middlewares into a single Handler.
// Middlewares are applied in order: first in the list is outermost (runs first).
//
// Example:
//
//	chain := Chain(sendRelay, retry, hedge, selectEndpoint)
//	// Execution order: retry → hedge → selectEndpoint → sendRelay
func Chain(handler Handler, middlewares ...Middleware) Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// Noop is a terminal handler that does nothing. Useful for testing.
var Noop Handler = HandlerFunc(func(ctx *Context) error { return nil })
