package config

import (
	"strings"
	"testing"
)

// A service's own route wins over the defaults' for the same path; the
// defaults cover the rest; a method-scoped route answers only its methods;
// an unconfigured service is served nothing.
func TestStaticRouteFor_Precedence(t *testing.T) {
	cfg, err := LoadFromFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	g := cfg.Gateway

	if rt, ok := g.StaticRouteFor("eth", "/identity", "GET"); !ok || rt.Body != "0xfixtureethpayout" {
		t.Errorf("eth /identity = %+v, %v; want the service's own route", rt, ok)
	}
	if rt, ok := g.StaticRouteFor("poly", "/identity", "POST"); !ok || rt.Body != "0xfixturepayoutaddress" {
		t.Errorf("poly /identity = %+v, %v; want the default route", rt, ok)
	}
	if rt, ok := g.StaticRouteFor("poly", "/info", "get"); !ok || rt.EffectiveContentType() != "application/json" || rt.Headers["Cache-Control"] != "max-age=60" {
		t.Errorf("poly GET /info = %+v, %v; want the JSON route, method matched case-insensitively", rt, ok)
	}
	if _, ok := g.StaticRouteFor("poly", "/info", "POST"); ok {
		t.Error("POST /info matched a GET-only route")
	}
	if _, ok := g.StaticRouteFor("poly", "/identity/", "GET"); ok {
		t.Error("matching must be exact on the path")
	}
	if _, ok := g.StaticRouteFor("not-configured", "/identity", "GET"); ok {
		t.Error("an unconfigured service must get the ordinary refusal, not a static answer")
	}
	if rt, _ := g.StaticRouteFor("poly", "/identity", "GET"); rt.EffectiveStatusCode() != 200 || rt.EffectiveContentType() != "text/plain; charset=utf-8" {
		t.Errorf("unset status/content type = %d %q, want 200 and text/plain", rt.EffectiveStatusCode(), rt.EffectiveContentType())
	}
}

// Routes that are malformed, or that two entries would both answer, fail the
// load: which one served would otherwise depend on list order.
func TestStaticRoutes_Validation(t *testing.T) {
	cases := map[string]string{
		"no leading slash": `[{path: identity, body: x}]`,
		"root":             `[{path: /, body: x}]`,
		"status":           `[{path: /x, status_code: 700}]`,
		"duplicate":        `[{path: /x}, {path: /x}]`,
		"method after all": `[{path: /x}, {path: /x, methods: [GET]}]`,
		"all after method": `[{path: /x, methods: [GET]}, {path: /x}]`,
		"duplicate method": `[{path: /x, methods: [GET]}, {path: /x, methods: [get]}]`,
	}
	for name, routes := range cases {
		t.Run(name, func(t *testing.T) {
			yaml := ownedAppsBase + "  owned_apps_addresses: [pokt1e3scnf3tfs9t44pawlvpemm6r0ggy3un4avmdk]\n  defaults:\n    static_routes: " + routes + "\n"
			if _, err := LoadFromBytes([]byte(yaml)); err == nil || !strings.Contains(err.Error(), "static_routes") {
				t.Fatalf("err = %v, want a static_routes refusal", err)
			}
		})
	}

	ok := ownedAppsBase + "  defaults:\n    static_routes: [{path: /x, methods: [GET]}, {path: /x, methods: [POST]}]\n"
	if _, err := LoadFromBytes([]byte(ok)); err != nil {
		t.Fatalf("one path split across methods must load: %v", err)
	}
}
