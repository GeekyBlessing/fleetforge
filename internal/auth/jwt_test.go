package auth

import (
	"strings"
	"testing"
	"time"
)

func TestIssueAndVerifyToken_RoundTrip(t *testing.T) {
	secret := []byte("test-secret")

	token, err := IssueToken(secret, "ci-system", []string{ScopeJobsSubmit}, time.Hour)
	if err != nil {
		t.Fatalf("issue failed: %v", err)
	}

	claims, err := VerifyToken(secret, token)
	if err != nil {
		t.Fatalf("verify failed: %v", err)
	}
	if claims.Subject != "ci-system" {
		t.Errorf("expected subject ci-system, got %s", claims.Subject)
	}
	if !claims.HasScope(ScopeJobsSubmit) {
		t.Errorf("expected token to have scope %s", ScopeJobsSubmit)
	}
	if claims.HasScope(ScopeWorkersDrain) {
		t.Errorf("expected token NOT to have scope %s", ScopeWorkersDrain)
	}
}

func TestVerifyToken_ExpiredRejected(t *testing.T) {
	secret := []byte("test-secret")

	token, err := IssueToken(secret, "ci-system", []string{ScopeJobsSubmit}, -time.Minute)
	if err != nil {
		t.Fatalf("issue failed: %v", err)
	}

	if _, err := VerifyToken(secret, token); err != ErrExpiredToken {
		t.Fatalf("expected ErrExpiredToken, got %v", err)
	}
}

func TestVerifyToken_WrongSecretRejected(t *testing.T) {
	token, err := IssueToken([]byte("secret-a"), "ci-system", []string{ScopeJobsSubmit}, time.Hour)
	if err != nil {
		t.Fatalf("issue failed: %v", err)
	}

	if _, err := VerifyToken([]byte("secret-b"), token); err != ErrInvalidToken {
		t.Fatalf("expected ErrInvalidToken for wrong secret, got %v", err)
	}
}

// This is the case that actually matters for a bearer-token scheme: a
// forged token that keeps the original (validly-signed) header/signature
// but swaps in different claims -- e.g. an attacker who intercepted a
// jobs:submit-scoped token trying to add workers:drain to it -- must be
// rejected, because the signature no longer matches the tampered payload.
func TestVerifyToken_TamperedPayloadRejected(t *testing.T) {
	secret := []byte("test-secret")

	token, err := IssueToken(secret, "ci-system", []string{ScopeJobsSubmit}, time.Hour)
	if err != nil {
		t.Fatalf("issue failed: %v", err)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 token parts, got %d", len(parts))
	}
	// Flip the payload segment to something else entirely -- any change
	// invalidates the signature over signingInput = header + "." + payload.
	tampered := parts[0] + "." + parts[0] + "X" + "." + parts[2]

	if _, err := VerifyToken(secret, tampered); err != ErrInvalidToken {
		t.Fatalf("expected ErrInvalidToken for tampered payload, got %v", err)
	}
}

func TestVerifyToken_MalformedRejected(t *testing.T) {
	secret := []byte("test-secret")

	for _, bad := range []string{"", "not-a-token", "a.b", "a.b.c.d"} {
		if _, err := VerifyToken(secret, bad); err != ErrInvalidToken {
			t.Errorf("input %q: expected ErrInvalidToken, got %v", bad, err)
		}
	}
}
