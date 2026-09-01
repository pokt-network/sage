package router

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/pokt-network/sage/blocklist"
	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// Blocklist is the admin API's view of the dynamic domain ban.
// blocklist.Manager satisfies it.
type Blocklist interface {
	Set(ctx context.Context, e blocklist.Entry) error
	Release(ctx context.Context, domain string) error
	Entries() []blocklist.Entry
	Base() []config.BlockedDomain
	Shared() bool
}

// SetBlocklist installs the dynamic domain ban the blocked-domains routes
// act on. Nil (the mock backend) makes those routes answer 503.
func (a *AdminAPI) SetBlocklist(b Blocklist) { a.blocklist = b }

type blockedDomainRequest struct {
	RPCTypes []string `json:"rpc_types,omitempty"`
	Reason   string   `json:"reason,omitempty"`
}

type blockedDomainResponse struct {
	Domain           string   `json:"domain"`
	RPCTypes         []string `json:"rpc_types,omitempty"`
	Applied          bool     `json:"applied"`
	Released         bool     `json:"released"`
	Shared           bool     `json:"shared"`
	PropagationError string   `json:"propagation_error,omitempty"`
}

type blockedDomainsListResponse struct {
	// Config is gateway_config.blocked_domains plus SAGE_BLOCKED_DOMAINS as
	// the file has them; only a config edit and reload change these.
	Config []config.BlockedDomain `json:"config"`
	// Admin is what was set through this API.
	Admin []blocklist.Entry `json:"admin"`
	// Shared is whether admin entries reach every replica (Redis) or only
	// this one (memory).
	Shared bool `json:"shared"`
}

// handleListBlockedDomains lists the blocked domains in force: the config
// base and the admin-set entries, separately, plus whether admin entries are
// shared across replicas.
func (a *AdminAPI) handleListBlockedDomains(w http.ResponseWriter, _ *http.Request) {
	if a.blocklist == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "no blocklist is configured (mock protocol backend)")
		return
	}
	resp := blockedDomainsListResponse{
		Config: a.blocklist.Base(),
		Admin:  a.blocklist.Entries(),
		Shared: a.blocklist.Shared(),
	}
	if resp.Config == nil {
		resp.Config = []config.BlockedDomain{}
	}
	if resp.Admin == nil {
		resp.Admin = []blocklist.Entry{}
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleSetBlockedDomain bans a domain on every service, for every RPC type or
// only the listed ones, without a redeploy. The ban is permanent until released,
// applies on this replica immediately and, with Redis, reaches every replica
// within its poll interval and survives restarts. Body: {"rpc_types":
// ["websocket"], "reason": "..."}; an empty rpc_types bans every type. A
// domain the config already lists is widened, never narrowed. A ban that
// applied here but did not reach Redis is reported in propagation_error with
// status 200: it is real on this replica.
func (a *AdminAPI) handleSetBlockedDomain(w http.ResponseWriter, req *http.Request) {
	if a.blocklist == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "no blocklist is configured (mock protocol backend)")
		return
	}
	target := strings.ToLower(strings.TrimSpace(req.PathValue("domain")))
	if target == "" {
		writeJSONError(w, http.StatusBadRequest, "domain is required")
		return
	}
	var body blockedDomainRequest
	if req.Body != nil && req.ContentLength != 0 {
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
	}
	for _, t := range body.RPCTypes {
		if !validRPCType(domain.RPCType(strings.ToLower(strings.TrimSpace(t)))) {
			writeJSONError(w, http.StatusBadRequest, "rpc_type "+strings.TrimSpace(t)+" is not recognised")
			return
		}
	}

	resp := blockedDomainResponse{Domain: target, RPCTypes: body.RPCTypes, Shared: a.blocklist.Shared()}
	err := a.blocklist.Set(req.Context(), blocklist.Entry{Domain: target, RPCTypes: body.RPCTypes, Reason: body.Reason})
	switch {
	case errors.Is(err, blocklist.ErrPropagation):
		resp.Applied = true
		resp.PropagationError = err.Error()
	case err != nil:
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	default:
		resp.Applied = true
	}
	a.logger.Warn("blocked domain set", "domain", target, "rpc_types", body.RPCTypes, "reason", body.Reason, "propagation_error", resp.PropagationError)
	writeJSON(w, http.StatusOK, resp)
}

// handleReleaseBlockedDomain lifts an admin-set ban. A domain only the config
// lists is 404: remove it from the file and reload.
func (a *AdminAPI) handleReleaseBlockedDomain(w http.ResponseWriter, req *http.Request) {
	if a.blocklist == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "no blocklist is configured (mock protocol backend)")
		return
	}
	target := strings.ToLower(strings.TrimSpace(req.PathValue("domain")))
	if target == "" {
		writeJSONError(w, http.StatusBadRequest, "domain is required")
		return
	}
	resp := blockedDomainResponse{Domain: target, Released: true, Shared: a.blocklist.Shared()}
	err := a.blocklist.Release(req.Context(), target)
	switch {
	case errors.Is(err, blocklist.ErrNotFound):
		writeJSONError(w, http.StatusNotFound, "no admin-set ban for "+target+" (a config entry is removed by editing the file and reloading)")
		return
	case errors.Is(err, blocklist.ErrPropagation):
		resp.PropagationError = err.Error()
	case err != nil:
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.logger.Warn("blocked domain released", "domain", target, "propagation_error", resp.PropagationError)
	writeJSON(w, http.StatusOK, resp)
}
