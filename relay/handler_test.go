package relay

import (
	"errors"
	"testing"
)

func TestChain_ExecutionOrder(t *testing.T) {
	var order []string

	mw := func(name string) Middleware {
		return func(next Handler) Handler {
			return HandlerFunc(func(ctx *Context) error {
				order = append(order, name+":before")
				err := next.HandleRelay(ctx)
				order = append(order, name+":after")
				return err
			})
		}
	}

	terminal := HandlerFunc(func(ctx *Context) error {
		order = append(order, "terminal")
		return nil
	})

	chain := Chain(terminal, mw("retry"), mw("hedge"), mw("send"))
	err := chain.HandleRelay(&Context{})
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"retry:before", "hedge:before", "send:before",
		"terminal",
		"send:after", "hedge:after", "retry:after",
	}
	if len(order) != len(expected) {
		t.Fatalf("order = %v, want %v", order, expected)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("order[%d] = %q, want %q", i, order[i], v)
		}
	}
}

func TestChain_ErrorPropagation(t *testing.T) {
	errBoom := errors.New("boom")

	failMiddleware := func(next Handler) Handler {
		return HandlerFunc(func(ctx *Context) error {
			return errBoom
		})
	}

	neverReached := HandlerFunc(func(ctx *Context) error {
		t.Fatal("should not reach terminal handler")
		return nil
	})

	chain := Chain(neverReached, failMiddleware)
	err := chain.HandleRelay(&Context{})
	if !errors.Is(err, errBoom) {
		t.Errorf("error = %v, want %v", err, errBoom)
	}
}

func TestChain_ShortCircuit(t *testing.T) {
	called := false

	cacheHit := func(next Handler) Handler {
		return HandlerFunc(func(ctx *Context) error {
			// Cache hit — don't call next
			return nil
		})
	}

	inner := func(next Handler) Handler {
		return HandlerFunc(func(ctx *Context) error {
			called = true
			return next.HandleRelay(ctx)
		})
	}

	chain := Chain(Noop, cacheHit, inner)
	_ = chain.HandleRelay(&Context{})
	if called {
		t.Error("inner middleware should not be called on cache hit")
	}
}

func TestHandlerFunc(t *testing.T) {
	called := false
	h := HandlerFunc(func(ctx *Context) error {
		called = true
		return nil
	})
	_ = h.HandleRelay(&Context{})
	if !called {
		t.Error("HandlerFunc should have been called")
	}
}
