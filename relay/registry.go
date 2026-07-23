package relay

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// MiddlewareFactory builds a middleware.
//
// It takes no arguments on purpose. Dependencies are captured by the closure at
// registration time, where they are already in scope and typed; passing a bag of
// `any` through the registry instead would trade compile-time checking for
// runtime type assertions and buy nothing, since whoever registers a middleware
// is also whoever constructed what it needs.
type MiddlewareFactory func() Middleware

// MiddlewareRegistry maps canonical middleware names to factories, so a chain
// can be composed by name from configuration (gateway_config.middleware_chain)
// rather than hard-coded at the call site. It is the seam a module adds itself
// through without editing the chain: register a name, then name it in the YAML.
//
// Registration errors are collected rather than returned per call — a caller
// registering twenty middlewares should not need twenty error checks, and a
// name collision is a startup-time programmer error, not a runtime condition.
// BuildChain reports them before it builds anything.
type MiddlewareRegistry struct {
	factories map[string]MiddlewareFactory
	errs      []error
}

// NewMiddlewareRegistry creates an empty registry.
func NewMiddlewareRegistry() *MiddlewareRegistry {
	return &MiddlewareRegistry{factories: make(map[string]MiddlewareFactory)}
}

// Register adds a named middleware factory.
//
// A duplicate name is an error rather than an overwrite: silently keeping the
// last registration is how two modules both claiming "rate_limit" would ship a
// gateway that runs one of them, with nothing said about the other.
func (r *MiddlewareRegistry) Register(name string, factory MiddlewareFactory) {
	if name == "" {
		r.errs = append(r.errs, errors.New("middleware registered with an empty name"))
		return
	}
	if factory == nil {
		r.errs = append(r.errs, fmt.Errorf("middleware %q registered with a nil factory", name))
		return
	}
	if _, dup := r.factories[name]; dup {
		r.errs = append(r.errs, fmt.Errorf("middleware %q registered twice", name))
		return
	}
	r.factories[name] = factory
}

// BuildChain composes the named middlewares into a Handler. First in the order
// is outermost (runs first); see Chain.
//
// The order is checked by ValidateChainOrder — the same invariants that guarded
// the chain when it was a literal. Taking the order from YAML without that check
// is the whole risk of taking it from YAML: a config could otherwise put
// select_endpoint outside retry (rotation stops working, silently) or heuristic
// after send_relay (it analyzes a response that isn't there yet).
func (r *MiddlewareRegistry) BuildChain(order []string) (Handler, error) {
	if err := errors.Join(r.errs...); err != nil {
		return nil, err
	}
	if len(order) == 0 {
		return nil, errors.New("no middleware order specified")
	}
	if err := ValidateChainOrder(order); err != nil {
		return nil, err
	}

	mws := make([]Middleware, 0, len(order))
	for _, name := range order {
		factory, ok := r.factories[name]
		if !ok {
			return nil, fmt.Errorf("unknown middleware %q (registered: %s)",
				name, strings.Join(r.RegisteredNames(), ", "))
		}
		mw := factory()
		if mw == nil {
			return nil, fmt.Errorf("factory for middleware %q returned nil", name)
		}
		mws = append(mws, mw)
	}

	// Reaching the terminal means the chain ran out of middlewares with nobody
	// answering — in production send_relay is terminal and never calls next.
	// Erroring is deliberate: returning nil would answer the client with an
	// empty 200, which is the failure this is here to make loud.
	terminal := HandlerFunc(func(_ *Context) error {
		return errors.New("relay: chain reached the terminal handler without being handled")
	})
	return Chain(terminal, mws...), nil
}

// RegisteredNames returns the registered middleware names, sorted.
func (r *MiddlewareRegistry) RegisteredNames() []string {
	names := make([]string, 0, len(r.factories))
	for name := range r.factories {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
