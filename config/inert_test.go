package config

import (
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
  reputation_config:
    signal_impacts:
      success: 1
      minor_error: -2
      major_error: -3
`,
			want: []string{"reputation_config.signal_impacts is parsed but not implemented"},
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
