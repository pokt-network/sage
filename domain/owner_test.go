package domain

import "testing"

func TestOwnerRegistry(t *testing.T) {
	RecordOwner("pokt1opA", "pokt1ownerX")
	RecordOwner("pokt1opB", "pokt1ownerX")
	RecordOwner("pokt1opC", "pokt1ownerY")
	RecordOwner("pokt1opD", "pokt1ownerZ")

	if got := EndpointAddr("pokt1opB-https://rel001.brand-b.example").Owner(); got != "pokt1ownerX" {
		t.Errorf("Owner = %q, want pokt1ownerX", got)
	}
	if got := EndpointAddr("pokt1unknown-https://x.example").Owner(); got != "" {
		t.Errorf("an unrecorded supplier has no owner, got %q", got)
	}
}

// Two brands of one owner are one provider; a provider hosting many owners
// is one provider too; but affiliation does not chain through them.
func TestAffiliates(t *testing.T) {
	RecordOwner("pokt1a1", "pokt1ownerQ")
	RecordOwner("pokt1a2", "pokt1ownerQ")
	RecordOwner("pokt1h1", "pokt1ownerQ")
	RecordOwner("pokt1h2", "pokt1ownerR")
	RecordOwner("pokt1o1", "pokt1ownerS")

	brandQ1 := EndpointAddr("pokt1a1-https://s001.brand-q1.example")
	brandQ2 := EndpointAddr("pokt1a2-https://rel001.brand-q2.example")
	hostR := EndpointAddr("pokt1h2-https://n2.host.example")
	other := EndpointAddr("pokt1o1-https://x.other.example")

	tried := AffiliatesOf(brandQ1)
	if !tried.Contains(brandQ2) {
		t.Error("another brand of the tried owner is affiliated")
	}
	if tried.Contains(hostR) {
		t.Error("an owner sharing a provider with the tried owner is NOT affiliated with it: no chaining")
	}
	got := EndpointAddrList{brandQ2, hostR, other}.ExcludeAffiliates(tried)
	if len(got) != 2 || got.Contains(brandQ2) {
		t.Errorf("ExcludeAffiliates = %v, want the other brand removed", got)
	}
	if got := (EndpointAddrList{brandQ2}).ExcludeAffiliates(tried); len(got) != 1 {
		t.Errorf("never empties: got %v", got)
	}
}
