package router

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/healthcheck"
)

// ExternalSourceAdmin is what the admin API needs from the external block
// source manager: read, replace and remove a service's sources on the
// running process. healthcheck.ExternalSourceManager implements it.
type ExternalSourceAdmin interface {
	View() []healthcheck.ExternalSourceView
	Get(serviceID domain.ServiceID) (healthcheck.ExternalSourceView, bool)
	Set(serviceID domain.ServiceID, sources []config.ExternalBlockSource) (healthcheck.ExternalSourceView, error)
	Remove(serviceID domain.ServiceID) bool
}

// SetExternalSources hands the admin API the manager. Without one the
// external-source routes answer 503.
func (a *AdminAPI) SetExternalSources(m ExternalSourceAdmin) { a.externalSources = m }

// handleListExternalSources lists every service's external block sources with
// their poll status.
//
// Each entry carries `origin` (config or admin), the sources as submitted
// (durations as strings), and `status`: whether a fetcher is running, whether
// its last poll failed and with what error, the last height seen and when.
func (a *AdminAPI) handleListExternalSources(w http.ResponseWriter, _ *http.Request) {
	if a.externalSources == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "external block sources are not adjustable on this instance")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": a.externalSources.View()})
}

// handleGetExternalSources returns one service's external block sources and
// poll status.
func (a *AdminAPI) handleGetExternalSources(w http.ResponseWriter, req *http.Request) {
	if a.externalSources == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "external block sources are not adjustable on this instance")
		return
	}
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	v, ok := a.externalSources.Get(serviceID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "service has no external block sources")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// handleSetExternalSources replaces a service's external block sources on the
// running process and restarts its polling.
//
// Body: `{"sources": [{"url": "https://…", "type": "json_rpc|rest|comet_bft",
// "method": "…", "path": "…", "interval": "15s", "timeout": "5s"}]}`; `url` is
// required, the rest optional with the config file's defaults. The change is
// per process and does not survive a restart: the process comes back on the
// file's `external_block_sources`, as with PUT /admin/log-level. It exists
// because the file may be a sealed secret and a retired source polls every
// fifteen seconds until someone can edit it. 400 for an invalid source, 409
// when the service's plugin tracks no block height (nothing to lift).
func (a *AdminAPI) handleSetExternalSources(w http.ResponseWriter, req *http.Request) {
	if a.externalSources == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "external block sources are not adjustable on this instance")
		return
	}
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	var body struct {
		Sources []healthcheck.ExternalSourceSpec `json:"sources"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	sources := make([]config.ExternalBlockSource, 0, len(body.Sources))
	for _, spec := range body.Sources {
		src, err := healthcheck.SourceFromSpec(spec)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		sources = append(sources, src)
	}
	v, err := a.externalSources.Set(serviceID, sources)
	switch {
	case err == nil:
		a.logger.Warn("admin: external block sources replaced", "service_id", serviceID, "sources", len(sources))
		writeJSON(w, http.StatusOK, v)
	case errors.Is(err, healthcheck.ErrInvalidSources):
		writeJSONError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, healthcheck.ErrUnknownService):
		writeJSONError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, healthcheck.ErrNoFloor):
		writeJSONError(w, http.StatusConflict, err.Error())
	default:
		a.logger.Error("admin: set external block sources", "service_id", serviceID, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to set external block sources")
	}
}

// handleDeleteExternalSources stops polling a service's external block
// sources on the running process.
//
// The service's external floor is no longer lifted; its pool consensus stands
// alone, as for a service with no sources configured. Not persisted: the
// file's sources return on restart. The body says whether there was anything
// to remove.
func (a *AdminAPI) handleDeleteExternalSources(w http.ResponseWriter, req *http.Request) {
	if a.externalSources == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "external block sources are not adjustable on this instance")
		return
	}
	serviceID := domain.ServiceID(req.PathValue("serviceID"))
	removed := a.externalSources.Remove(serviceID)
	if removed {
		a.logger.Warn("admin: external block sources removed", "service_id", serviceID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"service_id": serviceID, "removed": removed})
}
