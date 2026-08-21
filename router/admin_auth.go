package router

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// RequireAuth wraps an admin handler with bearer-token authentication.
//
// An empty token disables the check, which is the loopback-only posture SAGE
// shipped with. cmd/sagegw refuses to bind the admin API to a non-loopback
// address without a token, so "no token" and "reachable from off-host" cannot
// both be true in a running gateway.
//
// The credential is compared in constant time against the SHA-256 of both
// sides rather than the raw bytes: subtle.ConstantTimeCompare returns early
// when lengths differ, which would leak the token's length to anyone able to
// time the endpoint.
func RequireAuth(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := sha256.Sum256([]byte(token))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, ok := bearerToken(r)
		if !ok {
			unauthorized(w, "admin API requires an Authorization: Bearer <token> header")
			return
		}
		got := sha256.Sum256([]byte(presented))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			unauthorized(w, "invalid admin token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerToken extracts the credential from an Authorization header. The scheme
// is matched case-insensitively because RFC 7235 says it is case-insensitive
// and clients disagree about which case to send.
func bearerToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", false
	}
	scheme, credential, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "bearer") {
		return "", false
	}
	credential = strings.TrimSpace(credential)
	if credential == "" {
		return "", false
	}
	return credential, true
}

// unauthorized writes a 401 carrying the challenge, so a client (or the admin
// UI) can tell "you need a token" apart from "your token is wrong" only by the
// message, never by the status — both are 401 on purpose.
func unauthorized(w http.ResponseWriter, message string) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="sage-admin"`)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"error":"` + message + `"}`))
}
