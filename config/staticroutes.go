package config

import (
	"fmt"
	"net/http"
	"strings"
)

// StaticRoute is a fixed response the gateway serves itself for one exact
// path, without relaying: a small service-scoped metadata route such as
// /identity, whose body is a configured constant rather than anything a
// supplier says. A matching request never reaches RPC-type detection or
// endpoint selection. Same keys and matching as PATH's static_routes.
type StaticRoute struct {
	// Path is the exact request path served, after the /v1 mount point is
	// stripped ("/identity"). Must begin with "/" and must not be "/", which
	// would shadow all of a service's relay traffic.
	Path string `yaml:"path"`
	// Methods restricts the route to these HTTP methods, case-insensitively.
	// Empty matches every method.
	Methods []string `yaml:"methods"`
	// StatusCode is the HTTP status returned. Zero means 200.
	StatusCode int `yaml:"status_code"`
	// ContentType is the Content-Type returned. Empty means
	// "text/plain; charset=utf-8".
	ContentType string `yaml:"content_type"`
	// Body is returned verbatim.
	Body string `yaml:"body"`
	// Headers are extra response headers. Content-Type is always the one
	// ContentType resolves to, whatever this map says.
	Headers map[string]string `yaml:"headers"`
}

// EffectiveStatusCode is StatusCode with its zero value resolved to 200.
func (r StaticRoute) EffectiveStatusCode() int {
	if r.StatusCode == 0 {
		return http.StatusOK
	}
	return r.StatusCode
}

// EffectiveContentType is ContentType with its zero value resolved to plain
// text.
func (r StaticRoute) EffectiveContentType() string {
	if r.ContentType == "" {
		return "text/plain; charset=utf-8"
	}
	return r.ContentType
}

// StaticRouteFor returns the static route answering path and method for a
// configured service: the service's own routes first, then the defaults'
// (gateway_config.defaults, then unified_services.defaults). An unconfigured
// service gets none, so it meets the ordinary "not configured" refusal.
func (g *GatewayConfig) StaticRouteFor(serviceID, path, method string) (StaticRoute, bool) {
	svc := g.GetServiceConfig(serviceID)
	if svc == nil {
		return StaticRoute{}, false
	}
	for _, routes := range [][]StaticRoute{svc.StaticRoutes, g.Defaults.StaticRoutes, g.UnifiedServices.Defaults.StaticRoutes} {
		for _, rt := range routes {
			if rt.Path == path && (len(rt.Methods) == 0 || containsFold(rt.Methods, method)) {
				return rt, true
			}
		}
	}
	return StaticRoute{}, false
}

func containsFold(list []string, s string) bool {
	for _, v := range list {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}

// validateStaticRoutes refuses a route set that is malformed or where two
// routes claim the same request, as PATH does: which of the two answered
// would otherwise depend on list order.
func validateStaticRoutes(where string, routes []StaticRoute) error {
	anyMethod := map[string]bool{}
	pathMethod := map[string]bool{}
	for i, rt := range routes {
		switch {
		case rt.Path == "" || rt.Path[0] != '/':
			return fmt.Errorf("%s.static_routes[%d]: path must begin with '/'", where, i)
		case rt.Path == "/":
			return fmt.Errorf("%s.static_routes[%d]: path '/' would shadow all relay traffic", where, i)
		case rt.StatusCode != 0 && (rt.StatusCode < 100 || rt.StatusCode > 599):
			return fmt.Errorf("%s.static_routes[%d] (%s): status_code %d is not 100-599", where, i, rt.Path, rt.StatusCode)
		case anyMethod[rt.Path]:
			return fmt.Errorf("%s.static_routes[%d]: path %s is already served for every method", where, i, rt.Path)
		}
		if len(rt.Methods) == 0 {
			for key := range pathMethod {
				if strings.HasPrefix(key, rt.Path+" ") {
					return fmt.Errorf("%s.static_routes[%d]: path %s is already served for some methods", where, i, rt.Path)
				}
			}
			anyMethod[rt.Path] = true
			continue
		}
		for _, m := range rt.Methods {
			key := rt.Path + " " + strings.ToUpper(m)
			if pathMethod[key] {
				return fmt.Errorf("%s.static_routes[%d]: duplicate path and method %s", where, i, key)
			}
			pathMethod[key] = true
		}
	}
	return nil
}

// validateAllStaticRoutes checks every place static routes can be written.
func validateAllStaticRoutes(g GatewayConfig) error {
	if err := validateStaticRoutes("gateway_config.defaults", g.Defaults.StaticRoutes); err != nil {
		return err
	}
	if err := validateStaticRoutes("gateway_config.unified_services.defaults", g.UnifiedServices.Defaults.StaticRoutes); err != nil {
		return err
	}
	for _, svc := range g.AllServices() {
		if err := validateStaticRoutes(fmt.Sprintf("service %q", svc.ID), svc.StaticRoutes); err != nil {
			return err
		}
	}
	return nil
}
