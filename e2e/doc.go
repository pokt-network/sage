//go:build e2e

// Package e2e holds the end-to-end suite: black-box tests that drive a running
// gateway over HTTP and WebSocket the way a client would.
//
// It is behind the e2e build tag and excluded from the canonical unit run.
// Invoke it with `make e2e_test`, which needs a gateway already listening at
// SAGE_URL (default http://localhost:3069).
//
// These tests are written to run against **both SAGE and PATH**. That is the
// point of them — they are the executable half of the claim that SAGE is a
// drop-in successor, and a run against PATH is what catches a behaviour SAGE
// changed without meaning to. So nothing in this package may reach for a SAGE
// internal: no importing gateway packages, no asserting on SAGE-specific log
// lines, no admin endpoints PATH does not serve. If a test cannot be expressed
// through the public relay surface, it belongs in a unit test next to the code
// it covers.
package e2e
