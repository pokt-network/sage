package config

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestInertKeys_ReportsOnlyWhatTheOperatorWrote(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want []string // substrings expected in the report
		none bool     // expect an empty report
	}{
		{
			name: "a config touching nothing inert reports nothing",
			yaml: `
router_config:
  port: 3069
gateway_config:
  gateway_mode: centralized
`,
			none: true,
		},
		{
			name: "a block-level inert key is reported once, not per leaf",
			yaml: `
gateway_config:
  latency_profiles:
    evm:
      fast_threshold: 100ms
      slow_threshold: 2s
      slow_penalty: -3
`,
			want: []string{"gateway_config.latency_profiles is parsed but not implemented"},
		},
		{
			name: "a live block reports only the leaves that are still inert",
			yaml: `
gateway_config:
  reputation_config:
    signal_impacts:
      success: 1
      minor_error: -2
      slow_response: -3
`,
			want: []string{"reputation_config.signal_impacts.slow_response is parsed but not implemented"},
		},
		{
			name: "the same key under different parents is reported at each path",
			yaml: `
gateway_config:
  retry_config:
    connect_timeout: 1s
  services:
    - id: eth
      retry_config:
        connect_timeout: 2s
`,
			want: []string{
				"gateway_config.retry_config.connect_timeout is parsed but not implemented",
				"gateway_config.services[0].retry_config.connect_timeout is parsed but not implemented",
			},
		},
		{
			name: "a key that only matches under a different parent is not reported",
			yaml: `
gateway_config:
  active_health_checks:
    enabled: true
`,
			none: true, // "enabled" is inert under reputation_config, not here
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tree any
			if err := yaml.Unmarshal([]byte(tt.yaml), &tree); err != nil {
				t.Fatalf("bad test fixture: %v", err)
			}
			got := InertKeys(tree)

			if tt.none {
				if len(got) != 0 {
					t.Fatalf("expected no inert keys, got %v", got)
				}
				return
			}

			for _, want := range tt.want {
				found := false
				for _, g := range got {
					if strings.Contains(g, want) {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("expected a report containing %q, got %v", want, got)
				}
			}
			if len(got) != len(tt.want) {
				t.Fatalf("expected %d reports, got %d: %v", len(tt.want), len(got), got)
			}
		})
	}
}

// TestInertRegistryCoversDocComments is the anti-drift half.
//
// The registry and the doc comments are two statements of the same fact, and
// the failure mode is silent: a field documented as "parsed and not
// implemented" but missing from the registry produces no warning at startup,
// which is exactly the silence the registry exists to end. So the doc comments
// are the source of truth and this test holds the registry to them.
func TestInertRegistryCoversDocComments(t *testing.T) {
	demanded := documentedInertKeys(t)
	registered := make(map[string]bool)
	for _, key := range InertRegistryKeys() {
		// Registry keys are "parent.key" or bare "key"; the doc scan knows only
		// the leaf, so compare on the leaf.
		parts := strings.Split(key, ".")
		registered[parts[len(parts)-1]] = true
	}

	var missing []string
	for _, key := range demanded {
		if !registered[key] {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("config fields documented as unimplemented but missing from inertFields: %v\n"+
			"add them to config/inert.go so they are reported at startup, or drop the doc comment if they are implemented now",
			missing)
	}
}

// documentedInertKeys returns the yaml key of every config field whose own doc
// comment, or whose type's doc comment, says it is not implemented.
func documentedInertKeys(t *testing.T) []string {
	t.Helper()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}

	fset := token.NewFileSet()
	structs := map[string]*ast.StructType{}   // type name -> struct
	typeInert := map[string]bool{}            // type name -> its doc says unimplemented
	fieldsOfType := map[string][]*ast.Field{} // type name -> fields declaring it
	fieldDocs := map[*ast.Field]string{}      // field -> doc text
	fieldOwner := map[*ast.Field]string{}     // field -> type that declares it

	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				structs[ts.Name.Name] = st

				doc := gen.Doc.Text() + ts.Doc.Text()
				if saysUnimplemented(doc) {
					typeInert[ts.Name.Name] = true
				}

				for _, field := range st.Fields.List {
					fieldDocs[field] = field.Doc.Text()
					fieldOwner[field] = ts.Name.Name
					if name := namedType(field.Type); name != "" {
						fieldsOfType[name] = append(fieldsOfType[name], field)
					}
				}
			}
		}
	}

	demanded := map[string]bool{}

	// A field documented as unimplemented, in a struct that is not itself
	// unimplemented (there, the block-level entry already covers it).
	for typeName, st := range structs {
		if typeInert[typeName] {
			continue
		}
		for _, field := range st.Fields.List {
			if saysUnimplemented(fieldDocs[field]) {
				if key := yamlKey(field); key != "" {
					demanded[key] = true
				}
			}
		}
	}

	// A field whose TYPE is documented as unimplemented: the key that holds the
	// block is what gets reported.
	for typeName := range typeInert {
		for _, field := range fieldsOfType[typeName] {
			// A holder that itself sits inside an inert block needs no entry of
			// its own: reputation_config.tiered_selection already covers the
			// probation block nested under it.
			if typeInert[fieldOwner[field]] {
				continue
			}
			if key := yamlKey(field); key != "" {
				demanded[key] = true
			}
		}
	}

	out := make([]string, 0, len(demanded))
	for key := range demanded {
		out = append(out, key)
	}
	return out
}

// saysUnimplemented matches the phrasings this package uses to mark a field
// that parses and does nothing. It is deliberately narrow: a looser match
// ("no", "not") would demand registry entries for fields that are implemented.
func saysUnimplemented(doc string) bool {
	lower := strings.ToLower(doc)
	for _, marker := range []string{
		"parsed and not implemented",
		"is not read",
		"no field here is read",
		"read by nothing",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// yamlKey returns a field's yaml tag name, ignoring options like omitempty.
func yamlKey(field *ast.Field) string {
	if field.Tag == nil {
		return ""
	}
	tag := strings.Trim(field.Tag.Value, "`")
	idx := strings.Index(tag, `yaml:"`)
	if idx < 0 {
		return ""
	}
	rest := tag[idx+len(`yaml:"`):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	name, _, _ := strings.Cut(rest[:end], ",")
	if name == "-" {
		return ""
	}
	return name
}

// namedType returns the name of a struct type a field declares, looking through
// maps and slices so `map[string]LatencyProfile` reports LatencyProfile.
func namedType(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.StarExpr:
		return namedType(e.X)
	case *ast.ArrayType:
		return namedType(e.Elt)
	case *ast.MapType:
		return namedType(e.Value)
	}
	return ""
}

// A key with no Go field is already reported as unknown. These are the few
// where that answer is true and unhelpful, so the warning has to say what
// decides the behaviour instead.
func TestUnimplementedKeys(t *testing.T) {
	tree := map[string]any{
		"gateway_config": map[string]any{
			"active_health_checks": map[string]any{
				"interval": "60s",
				"external": map[string]any{
					"url":              "https://example.com/rules.yaml",
					"refresh_interval": "5m",
				},
			},
		},
	}

	got := UnimplementedKeys(tree)
	if len(got) != 1 {
		t.Fatalf("got %d findings, want 1: %v", len(got), got)
	}
	line := got[0]
	for _, want := range []string{
		"active_health_checks.external",
		"does not fetch",
		"check_interval",
		"active_health_checks.interval",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("finding does not mention %q — an operator cannot act on it: %s", want, line)
		}
	}
}

// A key of the same name under a different parent is a different key.
func TestUnimplementedKeys_ParentScoped(t *testing.T) {
	tree := map[string]any{
		"something_else": map[string]any{
			"external": map[string]any{"url": "https://example.com"},
		},
	}
	if got := UnimplementedKeys(tree); len(got) != 0 {
		t.Errorf("matched external under the wrong parent: %v", got)
	}
}

// The two registries stay separate: an inert key is parsed and unread, an
// unimplemented one has no field at all, and reporting a key as both would
// tell an operator two different stories about it.
func TestRegistriesDoNotOverlap(t *testing.T) {
	for _, u := range unimplementedFields {
		if reason, ok := matchInert(u.Parent, u.Key); ok {
			t.Errorf("%s.%s is in both registries; inert says %q", u.Parent, u.Key, reason)
		}
	}
}

// The startup report exists to be read. A key repeated once per service
// defeats that by volume: the canary's first boot report on 2026-09-03 was 97
// lines, 73 of them services[N].latency_profile saying the identical thing,
// and the 18 lines an operator would act on were buried under them.
func TestInertKeys_CollapsesAKeyRepeatedDownAList(t *testing.T) {
	var services []any
	for i := range 73 {
		services = append(services, map[string]any{
			"service_id":      fmt.Sprintf("svc-%d", i),
			"latency_profile": "fast",
		})
	}
	tree := map[string]any{"gateway_config": map[string]any{"services": services}}

	got := InertKeys(tree)

	if len(got) != 1 {
		t.Fatalf("got %d lines for one key on 73 services, want 1:\n%s", len(got), strings.Join(got, "\n"))
	}
	line := got[0]
	for _, want := range []string{"services[].latency_profile", "on 73 of them"} {
		if !strings.Contains(line, want) {
			t.Errorf("line does not mention %q: %s", want, line)
		}
	}
	// The index of one arbitrary service must not be named: it would invite
	// somebody to go and look at that one as though it were special.
	if strings.Contains(line, "[0]") {
		t.Errorf("line names one instance of a repeated key: %s", line)
	}
}

// Collapsing is by path SHAPE, not by key. The same key under two different
// parents is two findings and an operator wants both — they are two different
// places to go and edit.
func TestInertKeys_KeepsTheSameKeyAtDifferentShapes(t *testing.T) {
	tree := map[string]any{
		"gateway_config": map[string]any{
			"retry_config": map[string]any{"connect_timeout": "1s"},
			"services": []any{
				map[string]any{"retry_config": map[string]any{"connect_timeout": "1s"}},
				map[string]any{"retry_config": map[string]any{"connect_timeout": "1s"}},
			},
		},
	}

	got := InertKeys(tree)
	if len(got) != 2 {
		t.Fatalf("got %d lines, want 2 (the gateway one and the services one):\n%s", len(got), strings.Join(got, "\n"))
	}
	joined := strings.Join(got, "\n")
	if !strings.Contains(joined, "gateway_config.retry_config.connect_timeout is parsed") {
		t.Errorf("the gateway-level finding is missing:\n%s", joined)
	}
	if !strings.Contains(joined, "services[].retry_config.connect_timeout") || !strings.Contains(joined, "on 2 of them") {
		t.Errorf("the per-service findings did not collapse into one shaped line:\n%s", joined)
	}
}

// A single occurrence keeps its real path — there is nothing to generalise and
// the operator should be sent to the exact key.
func TestInertKeys_SingleOccurrenceKeepsItsPath(t *testing.T) {
	tree := map[string]any{
		"gateway_config": map[string]any{
			"services": []any{map[string]any{"latency_profile": "fast"}},
		},
	}
	got := InertKeys(tree)
	if len(got) != 1 || !strings.Contains(got[0], "services[0].latency_profile") {
		t.Errorf("want the exact path for a single occurrence, got: %v", got)
	}
}
