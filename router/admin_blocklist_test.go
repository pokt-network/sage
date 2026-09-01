package router

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/blocklist"
	"github.com/pokt-network/sage/config"
)

// recordingApplier is the protocol's SetBlockedDomains: it keeps the last
// union and can reject one.
type recordingApplier struct {
	last   []config.BlockedDomain
	reject error
}

func (r *recordingApplier) SetBlockedDomains(entries []config.BlockedDomain) error {
	if r.reject != nil {
		return r.reject
	}
	r.last = entries
	return nil
}

func newBlocklistServer(t *testing.T, base []config.BlockedDomain) (*recordingApplier, *httptest.Server) {
	t.Helper()
	admin, srv := newAdminServer(t)
	ap := &recordingApplier{}
	m := blocklist.New(ap, blocklist.NewMemoryBackend(), base)
	require.NoError(t, m.SetBlockedDomains(base))
	admin.SetBlocklist(m)
	return ap, srv
}

func do(t *testing.T, method, url string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req, err := http.NewRequest(method, url, &buf)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func TestAdminBlockedDomains_SetListRelease(t *testing.T) {
	ap, srv := newBlocklistServer(t, []config.BlockedDomain{{Domain: "config.example"}})

	resp, out := do(t, http.MethodPut, srv.URL+"/admin/blocked-domains/NodeFleet.net",
		map[string]any{"rpc_types": []string{"websocket"}, "reason": "dead since July"})
	require.Equal(t, http.StatusOK, resp.StatusCode, out)
	assert.Equal(t, true, out["applied"])
	assert.Equal(t, "nodefleet.net", out["domain"])
	assert.Equal(t, false, out["shared"], "memory backend is this replica only")

	require.Len(t, ap.last, 2, "protocol got config + admin")
	assert.Equal(t, "nodefleet.net", ap.last[1].Domain)
	assert.Equal(t, []string{"websocket"}, ap.last[1].RPCTypes)

	resp, out = do(t, http.MethodGet, srv.URL+"/admin/blocked-domains", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, out["config"], 1)
	assert.Len(t, out["admin"], 1)
	adminEntry := out["admin"].([]any)[0].(map[string]any)
	assert.Equal(t, "dead since July", adminEntry["reason"])
	assert.NotEmpty(t, adminEntry["since"])

	resp, out = do(t, http.MethodDelete, srv.URL+"/admin/blocked-domains/nodefleet.net", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, out)
	assert.Equal(t, true, out["released"])
	require.Len(t, ap.last, 1)
	assert.Equal(t, "config.example", ap.last[0].Domain)
}

func TestAdminBlockedDomains_EmptyBodyBansEveryType(t *testing.T) {
	ap, srv := newBlocklistServer(t, nil)
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/admin/blocked-domains/gone.example", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, ap.last, 1)
	assert.Empty(t, ap.last[0].RPCTypes)
}

func TestAdminBlockedDomains_Rejections(t *testing.T) {
	_, srv := newBlocklistServer(t, []config.BlockedDomain{{Domain: "config.example"}})

	resp, out := do(t, http.MethodPut, srv.URL+"/admin/blocked-domains/x.example", map[string]any{"rpc_types": []string{"telnet"}})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, out)

	resp, out = do(t, http.MethodDelete, srv.URL+"/admin/blocked-domains/config.example", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "config entries are not releasable here")
	assert.Contains(t, out["error"], "config")

	resp, _ = do(t, http.MethodDelete, srv.URL+"/admin/blocked-domains/never.example", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestAdminBlockedDomains_NoBlocklistIs503(t *testing.T) {
	_, srv := newAdminServer(t)
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/admin/blocked-domains"},
		{http.MethodPut, "/admin/blocked-domains/x.example"},
		{http.MethodDelete, "/admin/blocked-domains/x.example"},
	} {
		resp, _ := do(t, c.method, srv.URL+c.path, nil)
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, c.path)
	}
}
