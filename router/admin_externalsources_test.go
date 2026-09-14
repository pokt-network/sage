package router

import (
	"net/http"
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/healthcheck"
)

// fakeExternalSources records what the admin route asked for.
type fakeExternalSources struct {
	set     map[domain.ServiceID][]config.ExternalBlockSource
	setErr  error
	removed []domain.ServiceID
}

func (f *fakeExternalSources) View() []healthcheck.ExternalSourceView {
	out := []healthcheck.ExternalSourceView{}
	for id := range f.set {
		v, _ := f.Get(id)
		out = append(out, v)
	}
	return out
}

func (f *fakeExternalSources) Get(id domain.ServiceID) (healthcheck.ExternalSourceView, bool) {
	srcs, ok := f.set[id]
	if !ok {
		return healthcheck.ExternalSourceView{}, false
	}
	v := healthcheck.ExternalSourceView{ServiceID: id, Origin: healthcheck.SourceOriginAdmin}
	for _, s := range srcs {
		v.Sources = append(v.Sources, healthcheck.SpecFromSource(s))
	}
	return v, true
}

func (f *fakeExternalSources) Set(id domain.ServiceID, srcs []config.ExternalBlockSource) (healthcheck.ExternalSourceView, error) {
	if f.setErr != nil {
		return healthcheck.ExternalSourceView{}, f.setErr
	}
	if f.set == nil {
		f.set = map[domain.ServiceID][]config.ExternalBlockSource{}
	}
	f.set[id] = srcs
	v, _ := f.Get(id)
	return v, nil
}

func (f *fakeExternalSources) Persistent() bool { return false }

func (f *fakeExternalSources) Remove(id domain.ServiceID) bool {
	_, ok := f.set[id]
	delete(f.set, id)
	f.removed = append(f.removed, id)
	return ok
}

// The route exists so a dead source can be replaced without reaching the
// config file. The mutation must be observable on the manager: the sources
// as parsed, durations included.
func TestAdminExternalSources_SetGetDelete(t *testing.T) {
	admin, srv := newAdminServer(t)
	defer srv.Close()
	fake := &fakeExternalSources{}
	admin.SetExternalSources(fake)

	if status, _ := doTuning(t, srv.URL, http.MethodGet, "/admin/external-sources/sui", ""); status != http.StatusNotFound {
		t.Fatalf("GET unknown: status %d, want 404", status)
	}

	body := `{"sources":[{"url":"https://fullnode.example/sui","type":"json_rpc","method":"sui_getLatestCheckpointSequenceNumber","interval":"15s","timeout":"5s"}]}`
	status, resp := doTuning(t, srv.URL, http.MethodPut, "/admin/external-sources/sui", body)
	if status != http.StatusOK {
		t.Fatalf("PUT: status %d body %v", status, resp)
	}
	got := fake.set["sui"]
	if len(got) != 1 || got[0].URL != "https://fullnode.example/sui" || got[0].Method != "sui_getLatestCheckpointSequenceNumber" || got[0].Interval.String() != "15s" || got[0].Timeout.String() != "5s" {
		t.Fatalf("manager received %+v", got)
	}
	if resp["origin"] != healthcheck.SourceOriginAdmin || resp["persisted"] != false {
		t.Fatalf("response = %v, want the view with origin admin and persisted false", resp)
	}
	if status, resp = doTuning(t, srv.URL, http.MethodGet, "/admin/external-sources/sui", ""); status != http.StatusOK || resp["origin"] != healthcheck.SourceOriginAdmin || resp["persisted"] != false {
		t.Fatalf("GET one: status %d body %v, want the view with persisted false", status, resp)
	}

	status, resp = doTuning(t, srv.URL, http.MethodGet, "/admin/external-sources", "")
	if status != http.StatusOK {
		t.Fatalf("GET list: status %d", status)
	}
	if services, _ := resp["services"].([]any); len(services) != 1 || resp["persisted"] != false {
		t.Fatalf("list = %v, want one service and persisted false", resp)
	}

	status, resp = doTuning(t, srv.URL, http.MethodDelete, "/admin/external-sources/sui", "")
	if status != http.StatusOK || resp["removed"] != true || resp["persisted"] != false {
		t.Fatalf("DELETE: status %d body %v, want removed true and persisted false", status, resp)
	}
	if _, still := fake.set["sui"]; still {
		t.Fatal("DELETE must reach the manager")
	}
	if status, resp = doTuning(t, srv.URL, http.MethodDelete, "/admin/external-sources/sui", ""); status != http.StatusOK || resp["removed"] != false {
		t.Fatalf("second DELETE: status %d body %v, want removed false", status, resp)
	}
}

func TestAdminExternalSources_Refusals(t *testing.T) {
	admin, srv := newAdminServer(t)
	defer srv.Close()

	// No manager: the routes say so rather than answering 200 for nothing.
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/external-sources/eth", `{"sources":[{"url":"https://x.example"}]}`); status != http.StatusServiceUnavailable {
		t.Fatalf("no manager: status %d, want 503", status)
	}

	fake := &fakeExternalSources{}
	admin.SetExternalSources(fake)
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/external-sources/eth", `not json`); status != http.StatusBadRequest {
		t.Fatalf("bad body: status %d, want 400", status)
	}
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/external-sources/eth", `{"sources":[{"url":"https://x.example","interval":"soon"}]}`); status != http.StatusBadRequest {
		t.Fatalf("bad duration: status %d, want 400", status)
	}
	fake.setErr = healthcheck.ErrInvalidSources
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/external-sources/eth", `{"sources":[{"url":"https://x.example"}]}`); status != http.StatusBadRequest {
		t.Fatalf("invalid sources: status %d, want 400", status)
	}
	fake.setErr = healthcheck.ErrNoFloor
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/external-sources/eth", `{"sources":[{"url":"https://x.example"}]}`); status != http.StatusConflict {
		t.Fatalf("no floor: status %d, want 409", status)
	}
	fake.setErr = healthcheck.ErrUnknownService
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/external-sources/eth", `{"sources":[{"url":"https://x.example"}]}`); status != http.StatusNotFound {
		t.Fatalf("unknown service: status %d, want 404", status)
	}
	if len(fake.set) != 0 {
		t.Fatalf("a refused PUT must not set anything: %v", fake.set)
	}
}
