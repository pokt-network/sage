package docgen

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
)

// usage records how the codebase reads struct fields, in the two forms the
// config reference needs to tell a live key from a dead one.
type usage struct {
	// qualified holds "Parent.Field" for every selector chain seen, so a read
	// can be attributed to the struct it actually came from.
	qualified map[string]bool
	// bareInConsumer holds "Field" for selectors appearing in a file that
	// imports the config package (or is part of it). Those files are the only
	// ones that can be holding a config value in a local variable, which is the
	// case a qualified chain misses.
	bareInConsumer map[string]bool
}

// used reports whether a config field is read anywhere.
//
// A field is a live setting if it is selected off its own parent struct
// somewhere (parent.Field), or if its bare name is selected inside a file that
// works with config values at all. Anything else parses and does nothing.
//
// The two-part rule matters because config field names are not unique across
// the repository. reputation.SelectorConfig has its own Tier1Threshold with its
// own defaults, and it is read as cfg.Tier1Threshold inside the reputation
// package. Matching on bare names alone would take that as proof that
// gateway_config.reputation_config.tiered_selection.tier1_threshold is wired —
// when in fact the selector is always built from DefaultSelectorConfig() and
// the configured value is never consulted. That is exactly the kind of key an
// operator would set and trust.
func (u usage) used(parentField, field string) bool {
	if parentField != "" && u.qualified[parentField+"."+field] {
		return true
	}
	return u.bareInConsumer[field]
}

// collectUsage walks every non-test Go file under root and records field reads.
//
// skipDirs are directory names to ignore anywhere in the tree — docgen skips
// itself, since reading every config field to document it would otherwise mark
// every field as used.
func collectUsage(root string, skipDirs ...string) (usage, error) {
	u := usage{qualified: map[string]bool{}, bareInConsumer: map[string]bool{}}
	skip := map[string]bool{".git": true, "local": true, "bin": true}
	for _, d := range skipDirs {
		skip[d] = true
	}

	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skip[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			// A file we cannot parse is not evidence that a field is unused.
			// Skipping it can only under-report, which is the safe direction.
			return nil
		}

		isConsumer := file.Name.Name == "config"
		for _, imp := range file.Imports {
			p, unquoteErr := strconv.Unquote(imp.Path.Value)
			if unquoteErr == nil && strings.HasSuffix(p, "/config") {
				isConsumer = true
			}
		}

		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if isConsumer {
				u.bareInConsumer[sel.Sel.Name] = true
			}
			if parent, ok := sel.X.(*ast.SelectorExpr); ok {
				u.qualified[parent.Sel.Name+"."+sel.Sel.Name] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		return usage{}, err
	}
	return u, nil
}
