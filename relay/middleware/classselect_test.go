package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos/solana"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// classCapture records the method class each selection was asked to rank by.
type classCapture struct {
	*stubRepService
	classes []string
}

func (c *classCapture) SelectBest(ctx context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType) domain.EndpointAddr {
	c.classes = append(c.classes, reputation.MethodClassFrom(ctx))
	return eps[0]
}

// Selection ranks by the request's method class; a batch, which has no single
// class, is ranked without one.
func TestSelectEndpoint_RanksByMethodClass(t *testing.T) {
	rep := &classCapture{stubRepService: &stubRepService{}}
	mw := SelectEndpoint(rep, nil)
	next := relay.HandlerFunc(func(*relay.Context) error { return nil })
	for _, c := range []struct {
		methods []string
		want    string
	}{
		{[]string{"getProgramAccounts"}, domain.MethodClassHeavy},
		{[]string{"getSlot"}, domain.MethodClassLight},
		{[]string{"getAccountInfo"}, domain.MethodClassStandard},
		{[]string{"getSlot", "getBlock"}, ""},
	} {
		ctx := relay.NewContext(context.Background(), httptest.NewRequest(http.MethodPost, "/v1", nil), nil, nil)
		ctx.ServiceID = "solana"
		ctx.Plugin = solana.NewPlugin(nil, 0)
		ctx.Endpoints = domain.EndpointAddrList{"pokt1a-https://r001.a.example"}
		for _, m := range c.methods {
			ctx.Payloads = append(ctx.Payloads, domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, m))
		}
		rep.classes = nil
		if err := mw(next).HandleRelay(ctx); err != nil {
			t.Fatal(err)
		}
		if len(rep.classes) != 1 || rep.classes[0] != c.want {
			t.Errorf("%v: selection ranked by %q, want %q", c.methods, rep.classes, c.want)
		}
	}
}
