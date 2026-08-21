package router

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func authTestHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
}

func TestRequireAuth(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"

	tests := []struct {
		name       string
		token      string
		authHeader string
		wantStatus int
	}{
		{
			name:       "no token configured passes everything through",
			token:      "",
			authHeader: "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "correct token",
			token:      token,
			authHeader: "Bearer " + token,
			wantStatus: http.StatusOK,
		},
		{
			// RFC 7235 makes the scheme case-insensitive and clients disagree
			// about which case to send.
			name:       "lowercase scheme",
			token:      token,
			authHeader: "bearer " + token,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing header",
			token:      token,
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong token",
			token:      token,
			authHeader: "Bearer 0000000000000000000000000000000f",
			wantStatus: http.StatusUnauthorized,
		},
		{
			// A prefix of the real token must not pass: this is the case a
			// length-sensitive comparison would answer early.
			name:       "prefix of the token",
			token:      token,
			authHeader: "Bearer 0123456789abcdef",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong scheme",
			token:      token,
			authHeader: "Basic " + token,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "bare token without a scheme",
			token:      token,
			authHeader: token,
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/flags", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			rec := httptest.NewRecorder()

			RequireAuth(tt.token, authTestHandler()).ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantStatus == http.StatusUnauthorized {
				if got := rec.Header().Get("WWW-Authenticate"); got == "" {
					t.Fatal("401 must carry a WWW-Authenticate challenge")
				}
			}
		})
	}
}
