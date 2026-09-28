package metrics

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
)

type fakeSession struct{ eps domain.EndpointAddrList }

func (f fakeSession) RegisteredEndpoints(context.Context, domain.ServiceID, domain.RPCType) (domain.EndpointAddrList, error) {
	return f.eps, nil
}

type fakeScorer map[domain.EndpointAddr]float64

func (f fakeScorer) ScoreOf(_ domain.ServiceID, ep domain.EndpointAddr, _ domain.RPCType) (float64, bool) {
	s, ok := f[ep]
	return s, ok
}

// Counted per registration: two registrations behind one host are two, an
// unscored one counts toward the total but not the mean or the low count.
func TestSessionCollector_CountsRegistrations(t *testing.T) {
	eps := domain.EndpointAddrList{
		"s1-https://one.example.com", "s2-https://one.example.com", "s3-https://two.example.com",
		"s4-https://x.other.net",
	}
	scores := fakeScorer{
		"s1-https://one.example.com": 100, "s2-https://one.example.com": 100, "s3-https://two.example.com": 40,
	}
	c := NewSessionCollector(fakeSession{eps}, scores, map[domain.ServiceID][]domain.RPCType{"eth": {domain.RPCTypeJSONRPC}}, 80)
	mfs := gather(t, c)

	value := func(name, op string) (float64, bool) {
		mf := familyByName(mfs, name)
		if mf == nil {
			return 0, false
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "operator" && l.GetValue() == op {
					return m.GetGauge().GetValue(), true
				}
			}
		}
		return 0, false
	}
	if v, _ := value("sage_session_endpoints", "example.com"); v != 3 {
		t.Errorf("endpoints example.com = %v, want 3", v)
	}
	if v, _ := value("sage_session_endpoints_low", "example.com"); v != 1 {
		t.Errorf("low example.com = %v, want 1", v)
	}
	if v, _ := value("sage_operator_reputation_mean", "example.com"); v != 80 {
		t.Errorf("mean example.com = %v, want 80", v)
	}
	if v, _ := value("sage_session_endpoints", "other.net"); v != 1 {
		t.Errorf("endpoints other.net = %v, want 1", v)
	}
	if _, ok := value("sage_operator_reputation_mean", "other.net"); ok {
		t.Error("an operator with no scored registration must report no mean")
	}
}
