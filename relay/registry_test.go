package relay

import (
	"slices"
	"strings"
	"testing"
)

// recorder returns a factory whose middleware appends its name to *order when
// the chain runs, so tests can assert composition order rather than just that
// BuildChain returned something.
//
// A nil order means the test only cares that the name is registered and never
// runs the chain; pass through rather than dereference, so a later test that
// does run it fails on its assertion instead of a panic here.
func recorder(name string, order *[]string) MiddlewareFactory {
	return func() Middleware {
		return func(next Handler) Handler {
			return HandlerFunc(func(ctx *Context) error {
				if order != nil {
					*order = append(*order, name)
				}
				return next.HandleRelay(ctx)
			})
		}
	}
}

func TestMiddlewareRegistry_BuildChain(t *testing.T) {
	var ran []string
	reg := NewMiddlewareRegistry()
	reg.Register(MWParse, recorder(MWParse, &ran))
	reg.Register(MWValidate, recorder(MWValidate, &ran))
	reg.Register(MWSendRelay, recorder(MWSendRelay, &ran))

	chain, err := reg.BuildChain([]string{MWParse, MWValidate, MWSendRelay})
	if err != nil {
		t.Fatal(err)
	}
	_ = chain.HandleRelay(&Context{})

	want := []string{MWParse, MWValidate, MWSendRelay}
	if !slices.Equal(ran, want) {
		t.Errorf("ran = %v, want %v", ran, want)
	}
}

// TestMiddlewareRegistry_OrderComesFromArgNotRegistration is the whole point of
// the registry: the chain follows the requested order, so a config can reorder
// it without touching the code that registers.
func TestMiddlewareRegistry_OrderComesFromArgNotRegistration(t *testing.T) {
	var ran []string
	reg := NewMiddlewareRegistry()
	// Registered send-first, deliberately the reverse of the built order.
	reg.Register(MWSendRelay, recorder(MWSendRelay, &ran))
	reg.Register(MWValidate, recorder(MWValidate, &ran))
	reg.Register(MWParse, recorder(MWParse, &ran))

	chain, err := reg.BuildChain([]string{MWParse, MWValidate, MWSendRelay})
	if err != nil {
		t.Fatal(err)
	}
	_ = chain.HandleRelay(&Context{})

	want := []string{MWParse, MWValidate, MWSendRelay}
	if !slices.Equal(ran, want) {
		t.Errorf("ran = %v, want %v — chain must follow the requested order, not registration order", ran, want)
	}
}

// TestMiddlewareRegistry_BuildsDefaultChain checks the two halves that must stay
// in agreement: every name in DefaultChainOrder can be built, and the default
// order satisfies the invariants BuildChain enforces.
func TestMiddlewareRegistry_BuildsDefaultChain(t *testing.T) {
	var ran []string
	reg := NewMiddlewareRegistry()
	for _, name := range DefaultChainOrder() {
		reg.Register(name, recorder(name, &ran))
	}

	chain, err := reg.BuildChain(DefaultChainOrder())
	if err != nil {
		t.Fatalf("the default chain must build: %v", err)
	}
	_ = chain.HandleRelay(&Context{})

	if !slices.Equal(ran, DefaultChainOrder()) {
		t.Errorf("ran = %v, want %v", ran, DefaultChainOrder())
	}
}

// TestMiddlewareRegistry_ValidatesOrder is the guard that makes a YAML-driven
// chain safe. Without it a config could put select_endpoint outside retry and
// silently lose endpoint rotation.
func TestMiddlewareRegistry_ValidatesOrder(t *testing.T) {
	var ran []string
	reg := NewMiddlewareRegistry()
	reg.Register(MWParse, recorder(MWParse, &ran))
	reg.Register(MWRetry, recorder(MWRetry, &ran))
	reg.Register(MWSelectEndpoint, recorder(MWSelectEndpoint, &ran))
	reg.Register(MWSendRelay, recorder(MWSendRelay, &ran))

	// select_endpoint before retry — rotation would not work.
	_, err := reg.BuildChain([]string{MWParse, MWSelectEndpoint, MWRetry, MWSendRelay})
	if err == nil {
		t.Fatal("expected an ordering error; BuildChain must apply ValidateChainOrder")
	}
	if !strings.Contains(err.Error(), "must precede") {
		t.Errorf("error = %v, want an ordering violation", err)
	}
}

func TestMiddlewareRegistry_UnknownMiddleware(t *testing.T) {
	reg := NewMiddlewareRegistry()
	reg.Register(MWParse, recorder(MWParse, nil))

	_, err := reg.BuildChain([]string{"nonexistent"})
	if err == nil {
		t.Fatal("expected an error for an unregistered name")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error = %v, want it to name the unknown middleware", err)
	}
	// The registered set is listed to make a typo self-diagnosing.
	if !strings.Contains(err.Error(), MWParse) {
		t.Errorf("error = %v, want it to list the registered names", err)
	}
}

// TestMiddlewareRegistry_DuplicateRegistration covers the collision that the
// old registry resolved by silently keeping the last one — the failure mode
// that matters once more than one module registers middleware.
func TestMiddlewareRegistry_DuplicateRegistration(t *testing.T) {
	var ran []string
	reg := NewMiddlewareRegistry()
	reg.Register(MWParse, recorder("first", &ran))
	reg.Register(MWParse, recorder("second", &ran))

	_, err := reg.BuildChain([]string{MWParse})
	if err == nil {
		t.Fatal("expected an error for a duplicate registration, not a silent overwrite")
	}
	if !strings.Contains(err.Error(), "twice") {
		t.Errorf("error = %v, want a duplicate-registration error", err)
	}
}

func TestMiddlewareRegistry_NilFactory(t *testing.T) {
	reg := NewMiddlewareRegistry()
	reg.Register(MWParse, nil)

	_, err := reg.BuildChain([]string{MWParse})
	if err == nil || !strings.Contains(err.Error(), "nil factory") {
		t.Errorf("error = %v, want a nil-factory error", err)
	}
}

func TestMiddlewareRegistry_EmptyName(t *testing.T) {
	reg := NewMiddlewareRegistry()
	reg.Register("", recorder("x", nil))

	_, err := reg.BuildChain([]string{MWParse})
	if err == nil || !strings.Contains(err.Error(), "empty name") {
		t.Errorf("error = %v, want an empty-name error", err)
	}
}

func TestMiddlewareRegistry_FactoryReturningNil(t *testing.T) {
	reg := NewMiddlewareRegistry()
	reg.Register(MWParse, func() Middleware { return nil })

	_, err := reg.BuildChain([]string{MWParse})
	if err == nil || !strings.Contains(err.Error(), "returned nil") {
		t.Errorf("error = %v, want a nil-middleware error", err)
	}
}

func TestMiddlewareRegistry_NoOrder(t *testing.T) {
	reg := NewMiddlewareRegistry()
	if _, err := reg.BuildChain(nil); err == nil {
		t.Error("expected an error when no order is specified")
	}
}

// TestMiddlewareRegistry_TerminalErrors pins that a chain nobody answers fails
// loudly. Returning nil here would send the client an empty 200.
func TestMiddlewareRegistry_TerminalErrors(t *testing.T) {
	var ran []string
	reg := NewMiddlewareRegistry()
	// parse passes through to next; nothing terminates the chain.
	reg.Register(MWParse, recorder(MWParse, &ran))

	chain, err := reg.BuildChain([]string{MWParse})
	if err != nil {
		t.Fatal(err)
	}
	if err := chain.HandleRelay(&Context{}); err == nil {
		t.Error("a chain that nothing handled must error, not return nil")
	}
}

func TestMiddlewareRegistry_RegisteredNamesSorted(t *testing.T) {
	reg := NewMiddlewareRegistry()
	reg.Register(MWSendRelay, recorder(MWSendRelay, nil))
	reg.Register(MWParse, recorder(MWParse, nil))
	reg.Register(MWCache, recorder(MWCache, nil))

	got := reg.RegisteredNames()
	want := []string{MWCache, MWParse, MWSendRelay}
	if !slices.Equal(got, want) {
		t.Errorf("RegisteredNames() = %v, want %v (sorted)", got, want)
	}
}
