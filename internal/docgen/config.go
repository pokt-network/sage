package docgen

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pokt-network/sage/config"
)

// configPreamble is the hand-written framing around the generated tables. It is
// the part a generator cannot produce: what the file is, and the two rules
// (lenient parsing, value types) that explain why the tables look as they do.
const configPreamble = `# Configuration reference

Every key SAGE parses, generated from the config structs in ` + "`config/`" + `.

SAGE reads the **same YAML file as PATH**, unmodified. Two consequences show up
throughout this reference:

**An unknown key is not an error.** SAGE must load a PATH config that describes
features SAGE does not have, so unrecognised keys are collected into
` + "`Config.Ignored`" + ` and warned about individually at startup. They are never
silently dropped — but they are also never acted on. If you set a key and
nothing changes, check the startup log before checking your syntax.

**There are no optional fields.** Config uses value types, never ` + "`*bool`" + ` or
` + "`*int`" + `, so "unset" and "zero" are the same state. Each zero value is chosen
so that the unconfigured state is the safe one: ` + "`pprof_addr: \"\"`" + ` means pprof
is off, not that it listens on every interface. Where a zero genuinely cannot
mean "off" — a sample rate of 0 would disable observability an operator never
asked to lose — the field says what zero resolves to, and a negative value is
the way to ask for "actually none".

Durations are Go duration strings: ` + "`30s`" + `, ` + "`2m`" + `, ` + "`1h30m`" + `.
`

// GenerateConfigReference walks the Config struct in configDir and renders the
// full key reference as markdown.
func GenerateConfigReference(root string) (string, error) {
	configDir := filepath.Join(root, "config")
	pkg, err := parseStructs(configDir)
	if err != nil {
		return "", err
	}
	// docgen itself reads every config field to document it; counting that as
	// use would mark every field wired.
	reads, err := collectUsage(root, "docgen")
	if err != nil {
		return "", err
	}
	rootStruct, ok := pkg.structs["Config"]
	if !ok {
		return "", fmt.Errorf("no Config struct found in %s", configDir)
	}

	var b strings.Builder
	b.WriteString(generatedNotice)
	b.WriteString("\n\n")
	b.WriteString(configPreamble)

	// seen maps a struct type to the path it was first documented at, so a type
	// reused in several places (ReputationConfig sits under both gateway_config
	// and defaults) is described once and cross-referenced afterwards.
	seen := map[string]string{"Config": ""}

	// unwired collects keys that parse into a field nothing ever reads.
	var unwired []string

	type child struct {
		s *structDoc
		// path is the YAML path; goName is the Go field that leads here, which
		// is what qualifies a read as being of *this* struct's field.
		path   string
		goName string
		doc    string
	}

	// path keeps its repeat markers ("[]" for a list, ".<name>" for a keyed
	// map) all the way down, so a key nested inside a list of objects reports
	// as services[].external_block_sources[].url rather than losing the shape
	// that tells a reader where to put it.
	var walk func(s *structDoc, path, parentGoName string, depth int)
	walk = func(s *structDoc, path, parentGoName string, depth int) {
		var children []child
		var rows [][3]string

		// Leaf fields become a table row; struct fields become a subsection.
		parentKey := lastKey(path)
		for _, f := range s.fields {
			childPath := joinPath(path, f.yamlKey) + pathSuffix(f.typeExpr)
			c, isStruct := pkg.isStructRef(f.typeExpr)
			if !isStruct {
				desc := mdEscape(stripSubject(f.goName, f.doc))
				if !reads.used(parentGoName, f.goName) {
					unwired = append(unwired, joinPath(path, f.yamlKey))
					desc = strings.TrimSpace(unwiredMarker + " " + desc)
				}
				// The registry marker carries a reason and supersedes the
				// bare one; the key still lands in the list at the end.
				if reason, ok := config.InertReason(parentKey, f.yamlKey); ok {
					desc = inertDesc(reason, strings.TrimPrefix(desc, unwiredMarker+" "))
				}
				rows = append(rows, [3]string{
					"`" + f.yamlKey + "`",
					pkg.typeName(f.typeExpr),
					desc,
				})
				continue
			}
			if first, dup := seen[c.name]; dup {
				rows = append(rows, [3]string{
					"`" + f.yamlKey + "`",
					pkg.typeName(f.typeExpr),
					inertPrefix(parentKey, f.yamlKey) + fmt.Sprintf("Same keys as [`%s`](#%s).", first, anchor(first)),
				})
				continue
			}
			seen[c.name] = childPath
			children = append(children, child{c, childPath, f.goName, f.doc})
		}

		if len(rows) > 0 {
			b.WriteString("\n| Key | Type | Description |\n|---|---|---|\n")
			for _, r := range rows {
				desc := r[2]
				if desc == "" {
					desc = "—"
				}
				fmt.Fprintf(&b, "| %s | %s | %s |\n", r[0], r[1], desc)
			}
		}

		for _, c := range children {
			heading := strings.Repeat("#", min(depth+2, 6))
			fmt.Fprintf(&b, "\n%s `%s`\n", heading, c.path)
			if p := inertPrefix(parentKey, lastKey(c.path)); p != "" {
				b.WriteString("\n" + strings.TrimSpace(p) + "\n")
			}
			if doc := prose(c.s, c.doc); doc != "" {
				b.WriteString("\n")
				b.WriteString(doc)
			}
			walk(c.s, c.path, c.goName, depth+1)
		}
	}

	walk(rootStruct, "", "", 0)

	if len(unwired) > 0 {
		b.WriteString(unwiredSection(unwired))
	}
	return b.String(), nil
}

// unwiredMarker flags a key that parses into a field no code reads.
const unwiredMarker = "**⚠️ Parsed, not implemented.**"

// inertPrefix is the marker for a key the config package's inert registry
// names: one that parses into a field some code does read — usually because
// the same struct is live somewhere else — and that SAGE still does not act
// on at this path. The registry is what the startup log reports from, so the
// reference and the log agree. Empty when the key is not registered.
func inertPrefix(parentKey, key string) string {
	reason, ok := config.InertReason(parentKey, key)
	if !ok {
		return ""
	}
	return "**⚠️ Parsed, not implemented:** " + mdEscape(reason) + ". "
}

// inertDesc renders a registry-inert leaf: the marker, the registry's reason,
// then the doc comment minus the "parsed and not implemented" sentence it
// opens with (the marker says that) and minus the reason itself when the
// comment leads with the same words.
func inertDesc(reason, desc string) string {
	for _, pre := range []string{
		"Parsed and not implemented. ",
		"Parsed and not implemented: ",
		"Parsed and not implemented; ",
		"Not read; ",
	} {
		if strings.HasPrefix(desc, pre) {
			desc = strings.TrimSpace(desc[len(pre):])
			break
		}
	}
	reason = mdEscape(reason)
	if strings.HasPrefix(strings.ToLower(desc), strings.ToLower(strings.TrimSuffix(reason, "."))) {
		return "**⚠️ Parsed, not implemented:** " + desc
	}
	return "**⚠️ Parsed, not implemented:** " + reason + ". " + desc
}

// lastKey is the yaml key a path ends in, with the list and map markers
// stripped: "gateway_config.services[]" is a mapping whose key is services.
func lastKey(path string) string {
	if i := strings.LastIndex(path, "."); i >= 0 {
		path = path[i+1:]
	}
	return strings.TrimSuffix(path, "[]")
}

// unwiredSection explains the marker once, at the end, rather than repeating
// the reasoning on every affected row.
func unwiredSection(keys []string) string {
	var b strings.Builder
	b.WriteString("\n---\n\n## Parsed but not implemented\n\n")
	b.WriteString(`The keys below have a field in SAGE's config structs — so they parse cleanly
and are **not** reported as unknown keys at startup — but nothing in the
codebase reads them. Setting one has no effect, and produces no warning saying
so. They are listed here because a reference that quietly described them
alongside working keys would be worse than no reference at all.

Most are inherited from PATH's config surface, where the corresponding feature
exists. Treat them as documentation of PATH's format, not as SAGE settings.

`)
	for _, k := range keys {
		b.WriteString("- `" + k + "`\n")
	}
	b.WriteString(`
This list is generated by walking the config structs and checking each field
name against every selector expression in the codebase. It is deliberately
conservative — a field name that is read on *any* struct counts as read on all
of them — so it under-reports rather than accusing a live key of being dead.
`)
	return b.String()
}

// prose renders the description above a nested block. The field's own comment
// wins over the struct's when both exist: it describes this particular use of
// the type, which is more specific than what the type says about itself.
func prose(s *structDoc, fieldDocText string) string {
	text, subject := fieldDocText, ""
	if text == "" {
		text, subject = s.doc, s.name
	}
	text = stripSubject(subject, text)
	if text == "" {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(text), "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(lines[i])
	}
	return strings.Join(lines, "\n") + "\n"
}

// stripSubject drops the "TypeName is ..." / "FieldName does ..." lead-in that
// Go doc convention requires. A YAML reference has no use for the Go
// identifier — the reader is looking at the key name, not the field name — and
// leaving it in makes every description read as if it were about something
// else.
func stripSubject(subject, doc string) string {
	doc = strings.TrimSpace(doc)
	if subject == "" || doc == "" {
		return doc
	}
	rest, found := strings.CutPrefix(doc, subject+" ")
	if !found {
		return doc
	}
	// "X is the listen address" reads better as "The listen address" than as
	// "Is the listen address".
	if after, isCopula := strings.CutPrefix(rest, "is "); isCopula {
		rest = after
	}
	if rest == "" {
		return doc
	}
	return strings.ToUpper(rest[:1]) + rest[1:]
}

// anchor converts a heading into its GitHub markdown anchor.
func anchor(heading string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(heading) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == ' ', r == '.', r == '_', r == '-':
			b.WriteRune('-')
		}
	}
	return b.String()
}
