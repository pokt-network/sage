package docgen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pokt-network/sage/config"
)

// repoRoot is this package's directory two levels up: internal/docgen → root.
const repoRoot = "../.."

// TestGeneratedDocsAreCurrent is the drift guard.
//
// It runs the same generators `make docs` runs and compares the result against
// what is committed. Adding a config key, a metric, or a route without
// regenerating fails here — which is the whole point of generating these files
// rather than maintaining them by hand.
func TestGeneratedDocsAreCurrent(t *testing.T) {
	files, err := GenerateAll(repoRoot)
	if err != nil {
		t.Fatalf("GenerateAll: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("GenerateAll returned nothing")
	}

	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(repoRoot, name))
		if err != nil {
			t.Errorf("%s: %v — run `make docs`", name, err)
			continue
		}
		if string(got) != want {
			t.Errorf("%s is out of date — run `make docs`\n%s", name, firstDiff(string(got), want))
		}
	}
}

// TestConfigReferenceCoversEveryKey guards the property the reference exists
// for: every yaml-tagged config field appears in it, described.
//
// The generator walks from Config, so a struct reachable only through a field
// it fails to recurse into would silently vanish from the docs rather than
// showing up undocumented. This catches that.
func TestConfigReferenceCoversEveryKey(t *testing.T) {
	pkg, err := parseStructs(filepath.Join(repoRoot, "config"))
	if err != nil {
		t.Fatalf("parseStructs: %v", err)
	}
	doc, err := GenerateConfigReference(repoRoot)
	if err != nil {
		t.Fatalf("GenerateConfigReference: %v", err)
	}

	for typeName, st := range pkg.structs {
		for _, f := range st.fields {
			if !mentionsKey(doc, f.yamlKey) {
				t.Errorf("%s.%s: yaml key %q is missing from the reference", typeName, f.goName, f.yamlKey)
			}
		}
	}
}

// mentionsKey reports whether the reference names a key, in either of the two
// forms it can take: a bare key in a table row, or the last segment of a
// section heading's dotted path (which may carry a `[]` or `.<name>` repeat
// marker).
func mentionsKey(doc, key string) bool {
	for _, prefix := range []string{"`", "."} {
		for _, suffix := range []string{"`", "[]`", ".<name>`"} {
			if strings.Contains(doc, prefix+key+suffix) {
				return true
			}
		}
	}
	return false
}

// TestEveryConfigKeyIsDescribed requires a doc comment on every config field.
// A key with no comment is a key an operator has to read Go to understand.
//
// It checks the parsed fields rather than scanning the rendered markdown for em
// dashes. Those are not the same test: a field that is both undocumented *and*
// unread renders with the "parsed, not implemented" marker instead of an em
// dash, so a scan of the output would wave it through — which is exactly the
// field most in need of an explanation.
func TestEveryConfigKeyIsDescribed(t *testing.T) {
	pkg, err := parseStructs(filepath.Join(repoRoot, "config"))
	if err != nil {
		t.Fatalf("parseStructs: %v", err)
	}
	for typeName, st := range pkg.structs {
		for _, f := range st.fields {
			if strings.TrimSpace(f.doc) != "" {
				continue
			}
			// A field whose type is another config struct renders as a section
			// heading introduced by that struct's own doc comment, so the type
			// documenting itself is enough. Only leaves — the actual settings,
			// which render as table rows — need a comment of their own.
			if nested, isStruct := pkg.isStructRef(f.typeExpr); isStruct {
				if strings.TrimSpace(nested.doc) != "" {
					continue
				}
				t.Errorf("%s.%s (yaml %q) and its type %s both lack a doc comment",
					typeName, f.goName, f.yamlKey, nested.name)
				continue
			}
			t.Errorf("%s.%s (yaml %q) has no doc comment — the config reference is generated from these",
				typeName, f.goName, f.yamlKey)
		}
	}
}

// firstDiff reports the first differing line, so a failure points at the change
// rather than dumping several hundred lines of markdown.
func firstDiff(got, want string) string {
	gotLines, wantLines := strings.Split(got, "\n"), strings.Split(want, "\n")
	for i := range max(len(gotLines), len(wantLines)) {
		g, w := "", ""
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if g != w {
			return "first difference at line " + itoa(i+1) + ":\n  committed: " + g + "\n  generated: " + w
		}
	}
	return ""
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestUnwiredConfigKeysAreRegistered is the layer-2 contract of
// docs/path-compat.md as a test: every config key that parses into a field
// nothing reads must be in config/inert.go, so that startup reports it.
//
// The reference generator has always computed this list and printed it at the
// end of docs/configuration.md; what it never did was fail. A key could sit in
// that list, marked with a bare warning, for as long as nobody happened to
// read the bottom of the reference. retry_config.enabled and
// retry_config.retry_on_5xx did, each parsed and unread while their doc
// comments claimed otherwise.
func TestUnwiredConfigKeysAreRegistered(t *testing.T) {
	unwired, err := UnwiredConfigKeys(repoRoot)
	if err != nil {
		t.Fatalf("UnwiredConfigKeys: %v", err)
	}
	var unregistered []string
	for _, k := range unwired {
		if _, ok := config.InertReason(k.Parent, k.Key); !ok {
			unregistered = append(unregistered, k.Path)
		}
	}
	if len(unregistered) > 0 {
		t.Fatalf("config keys that parse into a field nothing reads, and are not registered as inert: %v\n"+
			"either wire the field (read it somewhere that changes behaviour) or add an entry to config/inert.go "+
			"with its parent key, so the startup report says it has no effect", unregistered)
	}
}
