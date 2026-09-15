package router

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/healthcheck"
)

type fakeHealthChecks struct {
	set    map[domain.ServiceID]healthcheck.ServiceChecksSpec
	setErr error
}

func (f *fakeHealthChecks) View() []healthcheck.CheckView { return nil }

func (f *fakeHealthChecks) Get(id domain.ServiceID) (healthcheck.CheckView, bool) {
	spec, ok := f.set[id]
	return healthcheck.CheckView{ServiceID: id, Origin: healthcheck.SourceOriginAdmin, Effective: spec}, ok
}

func (f *fakeHealthChecks) Set(id domain.ServiceID, spec healthcheck.ServiceChecksSpec) (healthcheck.CheckView, error) {
	if f.setErr != nil {
		return healthcheck.CheckView{}, f.setErr
	}
	f.set[id] = spec
	v, _ := f.Get(id)
	return v, nil
}

func (f *fakeHealthChecks) Remove(id domain.ServiceID) bool {
	_, ok := f.set[id]
	delete(f.set, id)
	return ok
}

func (f *fakeHealthChecks) Persistent() bool { return true }

func TestAdmin_HealthChecksRoutes(t *testing.T) {
	a, srv := newAdminServer(t)
	do := func(method, path, body string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}

	if r := do(http.MethodGet, "/admin/health-checks/sei", ""); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("no manager: %d, want 503", r.StatusCode)
	}

	fake := &fakeHealthChecks{set: map[domain.ServiceID]healthcheck.ServiceChecksSpec{}}
	a.SetHealthChecks(fake)

	r := do(http.MethodPut, "/admin/health-checks/sei", `{"checks":[{"name":"evm_block_number","type":"json_rpc","body":"{}"}]}`)
	if r.StatusCode != http.StatusOK || len(fake.set["sei"].Checks) != 1 || fake.set["sei"].Checks[0].Name != "evm_block_number" {
		t.Fatalf("PUT: %d, stored %+v", r.StatusCode, fake.set["sei"])
	}
	var got healthCheckReply
	if err := json.NewDecoder(do(http.MethodGet, "/admin/health-checks/sei", "").Body).Decode(&got); err != nil || !got.Persisted || len(got.Effective.Checks) != 1 {
		t.Fatalf("GET after PUT = %+v (%v)", got, err)
	}
	if r := do(http.MethodPut, "/admin/health-checks/sei", `not json`); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad body: %d, want 400", r.StatusCode)
	}
	if r := do(http.MethodDelete, "/admin/health-checks/sei", ""); r.StatusCode != http.StatusOK || len(fake.set) != 0 {
		t.Fatalf("DELETE: %d, left %v", r.StatusCode, fake.set)
	}

	fake.setErr = healthcheck.ErrInvalidChecks
	if r := do(http.MethodPut, "/admin/health-checks/sei", `{"checks":[]}`); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid checks: %d, want 400", r.StatusCode)
	}
	fake.setErr = healthcheck.ErrUnknownService
	if r := do(http.MethodPut, "/admin/health-checks/nope", `{"checks":[]}`); r.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown service: %d, want 404", r.StatusCode)
	}
}
