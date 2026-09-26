package domain

import (
	"fmt"
	"testing"
)

var stakeSeq int

// stake records n suppliers of owner on host, returning one of their addresses.
// Supplier IDs carry no "-": an EndpointAddr is "supplier-url".
func stake(owner, host string, n int) EndpointAddr {
	var ep EndpointAddr
	for i := 0; i < n; i++ {
		stakeSeq++
		sup := fmt.Sprintf("pokt1sup%d", stakeSeq)
		ep = EndpointAddr(sup + "-https://" + host)
		RecordOwner(sup, owner, ep.Operator())
	}
	return ep
}

func TestOwnerRegistry(t *testing.T) {
	RecordOwner("pokt1opB", "pokt1ownerX", "brand-b.example")
	if got := EndpointAddr("pokt1opB-https://rel001.brand-b.example").Owner(); got != "pokt1ownerX" {
		t.Errorf("Owner = %q, want pokt1ownerX", got)
	}
	if got := EndpointAddr("pokt1unknown-https://x.example").Owner(); got != "" {
		t.Errorf("an unrecorded supplier has no owner, got %q", got)
	}
}

// One owner's own brands are one provider; an owner staked with two providers
// that host many owners is not - those are two infrastructures.
func TestAffiliates_SoleTenantBrandsLinkMultiTenantProvidersDoNot(t *testing.T) {
	brand1 := stake("pokt1hydra", "s001.hydra-one.test", 6)
	brand2 := stake("pokt1hydra", "rel001.hydra-two.test", 6)

	for _, other := range []string{"pokt1o1", "pokt1o2", "pokt1o3", "pokt1o4"} {
		stake(other, "rm01.provider-k.test", 3)
		stake(other, "rm01.provider-m.test", 3)
	}
	tenantK := stake("pokt1tenant", "rm02.provider-k.test", 3)
	tenantM := stake("pokt1tenant", "rm02.provider-m.test", 3)
	independent := stake("pokt1solo", "n1.solo.test", 6)

	if !AffiliatesOf(brand1).Contains(brand2) {
		t.Error("two domains dedicated to one owner are one provider")
	}
	if AffiliatesOf(tenantK).Contains(tenantM) {
		t.Error("an owner staked with two multi-tenant providers must keep them independent")
	}
	if !AffiliatesOf(tenantK).Contains(stake("pokt1o1", "rm03.provider-k.test", 1)) {
		t.Error("the same operator is always affiliated")
	}
	if AffiliatesOf(brand1).Contains(independent) {
		t.Error("a different owner's dedicated domain is independent")
	}

	got := EndpointAddrList{brand2, tenantM, independent}.ExcludeAffiliates(AffiliatesOf(brand1))
	if len(got) != 2 || got.Contains(brand2) {
		t.Errorf("ExcludeAffiliates = %v, want the other brand removed", got)
	}
	if got := (EndpointAddrList{brand2}).ExcludeAffiliates(AffiliatesOf(brand1)); len(got) != 1 {
		t.Errorf("never empties: got %v", got)
	}
}

// Dedication needs a sample and a clear majority.
func TestAffiliates_DedicationThresholds(t *testing.T) {
	few1 := stake("pokt1few", "a.few-one.test", 2)
	few2 := stake("pokt1few", "a.few-two.test", 2)
	if AffiliatesOf(few1).Contains(few2) {
		t.Error("two registrations seen is too few to call a domain dedicated")
	}

	mostly1 := stake("pokt1most", "a.mostly-one.test", 9)
	stake("pokt1guest", "a.mostly-one.test", 1)
	mostly2 := stake("pokt1most", "a.mostly-two.test", 10)
	if !AffiliatesOf(mostly1).Contains(mostly2) {
		t.Error("an owner at 90% of a domain's registrations is its dedicated tenant")
	}

	shared1 := stake("pokt1share", "a.shared-one.test", 8)
	stake("pokt1guest2", "a.shared-one.test", 2)
	shared2 := stake("pokt1share", "a.shared-two.test", 10)
	if AffiliatesOf(shared1).Contains(shared2) {
		t.Error("an owner at 80% is not a dedicated tenant")
	}
}
