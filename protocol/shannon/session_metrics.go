package shannon

import "github.com/pokt-network/sage/domain"

// Session fetch paths and outcomes, the label values of
// sessionMetrics.RecordSessionFetch.
const (
	fetchBackground = "background" // during grace, off the request path
	fetchSync       = "sync"       // past grace, a request waits on it
	fetchCold       = "cold"       // nothing cached for (service, app)

	fetchOK          = "ok"
	fetchError       = "error"
	fetchSameSession = "same_session" // ends no later than the cached session
)

// sessionMetrics records session fetches. Satisfied by metrics.Recorder.
type sessionMetrics interface {
	RecordSessionFetch(serviceID domain.ServiceID, path, outcome string)
}

// SetSessionMetrics attaches the session fetch recorder. Wire time.
func (p *Protocol) SetSessionMetrics(m sessionMetrics) {
	p.sessions.metrics.Store(m)
}

// recordFetch counts one fetch, when a recorder is attached.
func (sm *sessionManager) recordFetch(serviceID, path, outcome string) {
	if m, ok := sm.metrics.Load().(sessionMetrics); ok {
		m.RecordSessionFetch(domain.ServiceID(serviceID), path, outcome)
	}
}
