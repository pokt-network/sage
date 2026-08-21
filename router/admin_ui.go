package router

import (
	_ "embed"
	"net/http"
)

// adminUI is the whole admin dashboard: one self-contained HTML file with its
// CSS and JS inline.
//
// Embedded rather than served from disk so the binary is still the whole
// gateway, and single-file rather than a built front-end so there is no
// toolchain between editing it and shipping it. It loads no external asset,
// which matters more here than usual: the admin port is meant to be reachable
// over an SSH tunnel on a host with no route to a CDN.
//
//go:embed admin_ui.html
var adminUI []byte

// RegisterUIRoutes registers the dashboard.
//
// It is deliberately separate from RegisterRoutes so the caller can serve the
// page WITHOUT the bearer-token check while every data route keeps it: a
// browser cannot attach an Authorization header to a top-level navigation, so
// requiring one for the page itself would make the UI unreachable exactly when
// authentication is configured. The page carries no gateway data — it is markup
// and script that then asks for a token and calls the authenticated API — so
// serving it unauthenticated discloses nothing beyond the fact that a SAGE
// admin port is listening, which anyone who can reach the port already knows.
func (a *AdminAPI) RegisterUIRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/ui", a.handleUI)
	mux.HandleFunc("GET /{$}", a.handleUIRedirect)
}

// handleUI serves the admin dashboard.
func (a *AdminAPI) handleUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Everything the page needs is inline, so nothing outside this origin may
	// be loaded or contacted. Stated rather than assumed: this is a control
	// plane, and a page that can be made to fetch is a page that can be made to
	// exfiltrate.
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; form-action 'none'; base-uri 'none'")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(adminUI)
}

// handleUIRedirect sends the admin port's root to the dashboard, because that
// is what an operator who typed the address into a browser was looking for.
func (a *AdminAPI) handleUIRedirect(w http.ResponseWriter, req *http.Request) {
	http.Redirect(w, req, "/admin/ui", http.StatusFound)
}
