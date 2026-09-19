package config

import (
	"bytes"
	"fmt"
	"os"
	"strings"
)

// expandEnv substitutes every `${NAME}` in a config document with the value of
// the environment variable NAME, before the YAML is parsed. It is what lets a
// deployment keep the structure of its config in git and its secrets in the
// environment:
//
//	gateway_private_key_hex: "${SAGE_GATEWAY_KEY}"
//
// Four decisions, each chosen so that the failure is loud:
//
// A variable that is unset — or set to the empty string — is a startup error
// naming it, never an empty value. An empty gateway_private_key_hex, redis
// password or admin token is the dangerous case: SAGE would start, sign
// nothing correctly, and look configured. A genuinely empty value is written
// literally in the file, where it is visible.
//
// Only `${NAME}` is a reference. A bare `$` is left alone, because passwords
// contain them, and `$${` is the escape for a literal `${` (so `$${X}` yields
// the text `${X}`); two dollars anywhere else stay two dollars, since
// collapsing them would rewrite a password that contains `$$`. There is no
// `${NAME:-default}`: a default belongs in the file, where it can be read, and
// the zero values already carry SAGE's.
//
// A value containing a newline is refused. Expansion is textual, so a value
// with a newline could add keys the file does not show; every other character
// can at worst break the scalar, which the parse then reports. Quote the
// placeholder (`"${VAR}"`) when the value may contain a colon, a `#` or a
// quote.
//
// The error text names the variable and never the value, because the caller
// logs it at startup.
//
// It runs inside parse, so it covers every entry point equally: the file, the
// GATEWAY_CONFIG document, PUT /admin/config, and everything the reload path
// re-reads (SIGHUP, POST /admin/reload, the stored override). The stored
// override keeps the document as uploaded, so a config PUT with `${…}` in it
// holds placeholders in Redis rather than secrets, and each replica expands it
// from its own environment.
//
// A PATH config is unaffected: PATH has no expansion, so no PATH config
// contains `${`.
func expandEnv(data []byte) ([]byte, error) {
	if !bytes.Contains(data, []byte("$")) {
		return data, nil
	}

	var out strings.Builder
	out.Grow(len(data))

	for i := 0; i < len(data); {
		if data[i] != '$' {
			out.WriteByte(data[i])
			i++
			continue
		}
		// "$${" is the escape for a literal "${". Two dollars anywhere else
		// are two dollars: collapsing them would silently rewrite a password
		// that contains "$$", and the escape is only ever needed in front of
		// a brace.
		if i+2 < len(data) && data[i+1] == '$' && data[i+2] == '{' {
			out.WriteByte('$')
			i += 2
			continue
		}
		if i+1 >= len(data) || data[i+1] != '{' {
			out.WriteByte('$')
			i++
			continue
		}

		rest := data[i+2:]
		end := bytes.IndexByte(rest, '}')
		if end < 0 {
			return nil, fmt.Errorf("config: unterminated ${ at line %d", lineOf(data, i))
		}
		name := string(rest[:end])
		line := lineOf(data, i)
		if !validEnvName(name) {
			return nil, fmt.Errorf(
				"config: ${%s} at line %d is not a plain variable name: write ${NAME} with no default or modifier",
				name, line)
		}
		value, ok := os.LookupEnv(name)
		if !ok || value == "" {
			return nil, fmt.Errorf(
				"config: ${%s} at line %d: environment variable %s is not set (an empty value counts as unset; write the value literally if you mean empty)",
				name, line, name)
		}
		if strings.ContainsAny(value, "\r\n") {
			return nil, fmt.Errorf(
				"config: ${%s} at line %d: the value contains a newline, which would change the structure of the document (value not shown)",
				name, line)
		}
		out.WriteString(value)
		i += 2 + end + 1
	}

	return []byte(out.String()), nil
}

// validEnvName reports whether name is a plain shell variable name. Anything
// else — a default, an indirection, a stray space — is refused rather than
// guessed at.
func validEnvName(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// lineOf is the 1-based line the byte at idx sits on, for an error an operator
// can act on without counting.
func lineOf(data []byte, idx int) int {
	return bytes.Count(data[:idx], []byte("\n")) + 1
}
