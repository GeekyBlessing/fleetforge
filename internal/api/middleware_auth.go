package api

import (
	"net/http"
	"strings"

	"github.com/launchverse/fleetforge/internal/auth"
)

// RequireScope rejects any request that doesn't carry a valid, unexpired
// bearer token (internal/auth) with the given scope. Wired into
// router.go's write endpoints only when a JWT secret is configured --
// see NewRouter's doc comment for why reads stay open regardless.
func RequireScope(secret []byte, scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || token == "" {
				writeError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
				return
			}

			claims, err := auth.VerifyToken(secret, token)
			if err != nil {
				writeError(w, http.StatusUnauthorized, "unauthorized", "invalid or expired token")
				return
			}
			if !claims.HasScope(scope) {
				writeError(w, http.StatusForbidden, "forbidden", "token missing required scope: "+scope)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
