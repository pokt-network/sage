// Command docgen regenerates the reference docs under docs/ from SAGE's source.
//
// Run it with `make docs`. It is a development tool, not part of the gateway:
// `make sage_build` builds ./cmd/sagegw only, so nothing here ships in the
// binary.
//
// The golden tests in internal/docgen run the same generators and compare
// against the committed files, so forgetting to run this fails CI rather than
// shipping a reference that has quietly stopped matching the code.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pokt-network/sage/internal/docgen"
)

func main() {
	root := flag.String("root", ".", "repository root")
	check := flag.Bool("check", false, "exit non-zero if any file is out of date, without writing")
	flag.Parse()

	files, err := docgen.GenerateAll(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "docgen: %v\n", err)
		os.Exit(1)
	}

	stale := false
	for name, content := range files {
		path := filepath.Join(*root, name)
		existing, readErr := os.ReadFile(path)
		if readErr == nil && string(existing) == content {
			continue
		}
		stale = true
		if *check {
			fmt.Fprintf(os.Stderr, "docgen: %s is out of date — run `make docs`\n", name)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "docgen: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "docgen: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s\n", name)
	}

	if *check && stale {
		os.Exit(1)
	}
	if !*check && !stale {
		fmt.Println("docs already up to date")
	}
}
