package docgen

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

const adminPreamble = `# HTTP API reference

Every route SAGE serves, generated from the mux registrations in ` + "`router/`" + `.

SAGE listens on **three separate ports**, and which port a route is on is a
security property, not a detail:

| Port (default) | Config key | Routes | Exposure |
|---|---|---|---|
| ` + "`3069`" + ` | ` + "`router_config.port`" + ` | relays, health, readiness | public |
| ` + "`9091`" + ` | ` + "`admin_config.addr`" + ` | ` + "`/admin/*`" + ` | **loopback only** |
| ` + "`9090`" + ` | ` + "`metrics_config.prometheus_addr`" + ` | ` + "`/metrics`" + ` | scrape-only |

**The admin API is unauthenticated.** Anyone who can reach it can flip feature
flags — turning on ` + "`shadow_mode`" + ` alone stops the gateway answering anything
— reset reputation, and clear circuit breakers. It defaults to
` + "`localhost:9091`" + ` for that reason, and binding it anywhere non-loopback is
warned about at startup. Put a TLS-terminating proxy with its own authentication
in front of it, or leave it on loopback and reach it through an SSH tunnel.

Relay requests name their service with the ` + "`Target-Service-Id`" + ` header. The
` + "`/v1`" + ` mount point belongs to the gateway, not to the service: the router
strips it before the chain runs, so a REST or CometBFT request addressed by path
reaches the supplier as ` + "`/status`" + `, not ` + "`/v1/status`" + `.
`

// routeDoc is one registered HTTP route.
type routeDoc struct {
	method  string
	path    string
	handler string
	doc     string
	group   string
}

// GenerateAdminAPIReference renders the HTTP route reference from the mux
// registrations in routerDir.
//
// Routes are read from the registration calls rather than listed by hand, so a
// route added without a doc comment on its handler shows up in the reference as
// undocumented instead of not showing up at all.
func GenerateAdminAPIReference(routerDir string) (string, error) {
	entries, err := os.ReadDir(routerDir)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", routerDir, err)
	}

	fset := token.NewFileSet()
	handlerDocs := make(map[string]string)
	var routes []routeDoc

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(routerDir, name), nil, parser.ParseComments)
		if err != nil {
			return "", fmt.Errorf("parse %s: %w", name, err)
		}

		group := "Gateway"
		if strings.Contains(name, "admin") {
			group = "Admin"
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if doc := strings.TrimSpace(fn.Doc.Text()); doc != "" {
				handlerDocs[fn.Name.Name] = doc
			}
		}

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "HandleFunc" || len(call.Args) < 2 {
				return true
			}
			pattern := stringLit(call.Args[0])
			if pattern == "" {
				return true
			}
			method, path, found := strings.Cut(pattern, " ")
			if !found {
				method, path = "ANY", pattern
			}
			routes = append(routes, routeDoc{
				method:  method,
				path:    path,
				handler: handlerName(call.Args[1]),
				group:   group,
			})
			return true
		})
	}

	for i := range routes {
		routes[i].doc = handlerDocs[routes[i].handler]
	}

	var b strings.Builder
	b.WriteString(generatedNotice)
	b.WriteString("\n\n")
	b.WriteString(adminPreamble)

	for _, group := range []string{"Gateway", "Admin"} {
		var inGroup []routeDoc
		for _, r := range routes {
			if r.group == group {
				inGroup = append(inGroup, r)
			}
		}
		if len(inGroup) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n## %s routes\n\n", group)
		b.WriteString("| Method | Path | Description |\n|---|---|---|\n")
		for _, r := range inGroup {
			desc := mdEscape(firstSentence(stripSubject(r.handler, r.doc)))
			if desc == "" {
				desc = "**undocumented** — add a doc comment to `" + r.handler + "`"
			}
			fmt.Fprintf(&b, "| `%s` | `%s` | %s |\n", r.method, r.path, desc)
		}
		// Detail sections are per handler, not per route: several patterns
		// routinely share one handler (`/v1` and `/v1/{path...}`), and
		// repeating the same prose under each is noise that makes the file
		// look longer than it is informative.
		documented := map[string]bool{}
		for _, r := range inGroup {
			if r.doc == "" || documented[r.handler] {
				continue
			}
			documented[r.handler] = true
			var patterns []string
			for _, other := range inGroup {
				if other.handler == r.handler {
					patterns = append(patterns, fmt.Sprintf("`%s %s`", other.method, other.path))
				}
			}
			fmt.Fprintf(&b, "\n### %s\n\n%s\n", strings.Join(patterns, ", "), stripSubject(r.handler, r.doc))
		}
	}

	return b.String(), nil
}

// handlerName renders the handler expression as a bare function name, so
// `a.handleListFlags` and `handleListFlags` both resolve to the same doc.
func handlerName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.SelectorExpr:
		return t.Sel.Name
	case *ast.Ident:
		return t.Name
	}
	return ""
}

// firstSentence trims a doc comment down to something that fits a table cell.
func firstSentence(doc string) string {
	if idx := strings.Index(doc, ". "); idx > 0 {
		doc = doc[:idx+1]
	}
	if idx := strings.Index(doc, ".\n"); idx > 0 {
		doc = doc[:idx+1]
	}
	return strings.Join(strings.Fields(doc), " ")
}
