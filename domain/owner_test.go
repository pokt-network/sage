package domain

import "testing"

func TestOwnerRegistry(t *testing.T) {
	RecordOwner("pokt1opA", "pokt1ownerX", "https://s001.brand-a.example", "wss://s001.brand-a.example")
	RecordOwner("pokt1opB", "pokt1ownerX", "https://rel001.brand-b.example")
	RecordOwner("pokt1opC", "pokt1ownerY", "https://shared.provider.example")
	RecordOwner("pokt1opD", "pokt1ownerZ", "https://shared.provider.example")

	if got := EndpointAddr("pokt1opB-https://rel001.brand-b.example").Owner(); got != "pokt1ownerX" {
		t.Errorf("Owner = %q, want pokt1ownerX", got)
	}
	if got := EndpointAddr("pokt1unknown-https://x.example").Owner(); got != "" {
		t.Errorf("an unrecorded supplier has no owner, got %q", got)
	}
	if got := OwnerOfURL("wss://s001.brand-a.example"); got != "pokt1ownerX" {
		t.Errorf("OwnerOfURL = %q, want pokt1ownerX", got)
	}
	if got := OwnerOfURL("https://shared.provider.example"); got != "" {
		t.Errorf("a URL two owners stake has no single owner, got %q", got)
	}
}

// Two brands of one owner are one provider; a provider hosting many owners
// is one provider too; but affiliation does not chain through them.
func TestAffiliates(t *testing.T) {
	RecordOwner("pokt1a1", "pokt1ownerQ", "https://s001.brand-q1.example")
	RecordOwner("pokt1a2", "pokt1ownerQ", "https://rel001.brand-q2.example")
	RecordOwner("pokt1h1", "pokt1ownerQ", "https://n1.host.example")
	RecordOwner("pokt1h2", "pokt1ownerR", "https://n2.host.example")
	RecordOwner("pokt1o1", "pokt1ownerS", "https://x.other.example")

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
