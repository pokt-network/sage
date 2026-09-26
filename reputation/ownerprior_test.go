package reputation

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// A new brand of an owner whose other brand is failing is charged the
// owner's rate from its first attempt, instead of entering the top tier on no
// evidence; a new brand of an owner nobody has seen is charged nothing.
func TestOperatorChronic_NewBrandInheritsOwnerRate(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, DefaultServiceConfig())
	svc.SetOperatorChronic(func(id domain.ServiceID) bool { return id == "base" })
	ctx := context.Background()

	domain.RecordOwner("pokt1brandA", "pokt1ownerP", "https://s001.brand-a.test")
	domain.RecordOwner("pokt1brandB", "pokt1ownerP", "https://rel001.brand-b.test")
	domain.RecordOwner("pokt1lone", "pokt1ownerL", "https://n1.lone.test")
	brandA := domain.EndpointAddr("pokt1brandA-https://s001.brand-a.test")
	brandB := domain.EndpointAddr("pokt1brandB-https://rel001.brand-b.test")
	lone := domain.EndpointAddr("pokt1lone-https://n1.lone.test")

	// Brand A: 400 attempts, half of them major errors (weight 0.5): 25%.
	for i := 0; i < 200; i++ {
		_ = svc.RecordSignal(ctx, "base", brandA, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0))
		_ = svc.RecordSignal(ctx, "base", brandA, domain.RPCTypeJSONRPC, NewMajorErrorSignal("timeout", 0))
	}
	// Brand B and the lone operator: one clean attempt each.
	_ = svc.RecordSignal(ctx, "base", brandB, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0))
	_ = svc.RecordSignal(ctx, "base", lone, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0))

	svc.refreshBaselines()
	v := svc.chronic.Load()

	charged, ok := v.byKey[keyID{"base", "https://rel001.brand-b.test|json_rpc"}]
	if !ok || charged < 0.2 {
		t.Fatalf("brand B charged %v (present=%v), want about its owner's 0.25", charged, ok)
	}
	if r, ok := v.byKey[keyID{"base", "https://n1.lone.test|json_rpc"}]; ok && r > 0 {
		t.Errorf("an unseen owner's new brand is charged %v, want nothing", r)
	}
	for id := range v.byOp {
		if isOwnerOp(id.op) {
			t.Errorf("owner evidence leaked into the operator view as %q", id.op)
		}
	}
	if eff := svc.effectiveFor("base", "https://rel001.brand-b.test|json_rpc", State{Score: 100}); eff >= 80 {
		t.Errorf("brand B's fresh key reads %v, want it out of the top tier", eff)
	}
}
