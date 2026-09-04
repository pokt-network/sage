package featureflag

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Every flag in DefaultFlags must be read somewhere outside this package.
//
// A flag with a defaults row and an admin route and no reader is a control
// that controls nothing, and an operator will believe it: health_checks
// was one from the first commit until 2026-09-04, and PUT
// /admin/flags/health_checks was a no-op for that whole time. The
// constants are what code references (CLAUDE.md forbids the string
// literal), so a constant with no reference is a flag with no reader.
func TestEveryFlagHasAReader(t *testing.T) {
	root := filepath.Join("..")
	fset := token.NewFileSet()

	// Flag constants: every top-level string const in this package whose
	// value is a key of DefaultFlags.
	byValue := map[string]string{}
	for name, value := range flagConstants(t, fset) {
		if _, known := DefaultFlags[value]; known {
			byValue[value] = name
		}
	}
	for value := range DefaultFlags {
		if _, ok := byValue[value]; !ok {
			t.Errorf("flag %q has no Flag* constant", value)
		}
	}

	referenced := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "local", "bin", "docs", "featureflag":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "featureflag" {
				referenced[sel.Sel.Name] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var unread []string
	for value, name := range byValue {
		if !referenced[name] {
			unread = append(unread, value+" ("+name+")")
		}
	}
	if len(unread) > 0 {
		t.Fatalf("flags in DefaultFlags that no code outside this package references: %v\n"+
			"a flag must gate something; either read it with flags.IsEnabled(ctx, featureflag.%s, serviceID) somewhere, or delete the row", unread, "Flag*")
	}
}

// flagConstants parses this package's non-test files for top-level string
// constants.
func flagConstants(t *testing.T, fset *token.FileSet) map[string]string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					out[name.Name] = strings.Trim(lit.Value, "`\"")
				}
			}
		}
	}
	return out
}
