// Package auth implements the human/CI-facing bearer-token half of
// docs/09-design-rationale.md 9.4's security design (mTLS covers the other
// half, worker<->scheduler, in internal/auth/mtls.go).
//
// This is a minimal HS256 JSON Web Token implementation against the
// standard library only (crypto/hmac, crypto/sha256, encoding/base64,
// encoding/json) rather than a third-party JWT library. That's a deliberate
// trade-off, not an oversight: RFC 7519's compact serialization is a small,
// fully-specified format (base64url(header).base64url(payload).base64url(hmac)),
// and this service only ever needs to mint and verify its own tokens against
// its own shared secret: it never has to interoperate with someone else's
// JWT issuer, parse arbitrary third-party tokens, or support the wider
// algorithm zoo (RS256, key rotation via JWKS, etc.) that justifies pulling
// in a general-purpose library. Compare to internal/config's stated reason
// for skipping viper/koanf: one less dependency for a genuinely small,
// stable surface. The security-sensitive part, the HMAC comparison, uses
// hmac.Equal (constant-time), which is the one place hand-rolling would
// actually be dangerous to get wrong.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Scopes used by internal/api's route middleware. Kept here, not in
// internal/api, so the token-minting side (fleetforgectl) and the
// token-verifying side (the REST API) can't drift on the string values.
const (
	ScopeJobsSubmit   = "jobs:submit"
	ScopeWorkersDrain = "workers:drain"
)

var (
	// ErrInvalidToken covers malformed tokens and signature mismatches;
	// deliberately not distinguished further in the returned error, so
	// callers can't be tricked into leaking which part of a forged token
	// was wrong.
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
)

// Claims is intentionally a small, fixed set (not an arbitrary
// map[string]any), so scope checks (HasScope) are a plain slice
// contains-check rather than a type-asserting mess at every call site.
type Claims struct {
	Subject   string   `json:"sub"`
	Scopes    []string `json:"scopes"`
	IssuedAt  int64    `json:"iat"`
	ExpiresAt int64    `json:"exp"`
}

// HasScope reports whether the token carries the given scope. No wildcard
// or hierarchy support (e.g. "jobs:*"): scopes are an exact-match list,
// which is enough for the two scopes this service currently defines and
// easy to extend later without a breaking change to token holders.
func (c Claims) HasScope(scope string) bool {
	for _, s := range c.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// IssueToken mints a compact HS256 token for subject (a human operator or
// CI system identifier, recorded for audit purposes; see
// docs/09-design-rationale.md 9.4) carrying scopes, expiring after ttl.
func IssueToken(secret []byte, subject string, scopes []string, ttl time.Duration) (string, error) {
	if len(secret) == 0 {
		return "", errors.New("auth: empty signing secret")
	}
	now := time.Now()
	claims := Claims{
		Subject:   subject,
		Scopes:    scopes,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(ttl).Unix(),
	}
	return signToken(secret, claims)
}

func signToken(secret []byte, claims Claims) (string, error) {
	headerJSON, err := json.Marshal(jwtHeader{Alg: "HS256", Typ: "JWT"})
	if err != nil {
		return "", fmt.Errorf("auth: marshal header: %w", err)
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("auth: marshal claims: %w", err)
	}

	signingInput := b64(headerJSON) + "." + b64(claimsJSON)
	sig := sign(secret, signingInput)
	return signingInput + "." + b64(sig), nil
}

// VerifyToken checks the signature (constant-time) and expiry of token and
// returns its claims. A tampered payload, a signature from the wrong
// secret, and an expired-but-otherwise-valid token are all rejected here:
// this is the single choke point internal/api's middleware calls through,
// so there's exactly one place that logic can be wrong rather than one per
// route.
func VerifyToken(secret []byte, token string) (*Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	signingInput := parts[0] + "." + parts[1]
	gotSig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, ErrInvalidToken
	}
	if !hmac.Equal(sign(secret, signingInput), gotSig) {
		return nil, ErrInvalidToken
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrInvalidToken
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, ErrInvalidToken
	}
	if time.Now().Unix() > claims.ExpiresAt {
		return nil, ErrExpiredToken
	}
	return &claims, nil
}

func sign(secret []byte, signingInput string) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	return mac.Sum(nil)
}

func b64(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}
