package oauth

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// s256Challenge is the RFC 7636 S256 transform a client applies to its verifier.
func s256Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func TestValidateCodeChallenge(t *testing.T) {
	good := s256Challenge(strings.Repeat("a", 64))
	cases := []struct {
		name      string
		challenge string
		method    string
		wantErr   bool
	}{
		{"valid s256", good, "S256", false},
		{"missing method", good, "", true},
		{"plain rejected", good, "plain", true},
		{"empty challenge", "", "S256", true},
		{"not base64url", "not valid base64!!", "S256", true},
		{"wrong length", base64.RawURLEncoding.EncodeToString([]byte("short")), "S256", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCodeChallenge(tc.challenge, tc.method)
			if tc.wantErr != (err != nil) {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidGrant) {
				t.Fatalf("error should wrap ErrInvalidGrant: %v", err)
			}
		})
	}
}

func TestVerifyPKCE(t *testing.T) {
	verifier := strings.Repeat("x", 50)
	challenge := s256Challenge(verifier)

	if err := VerifyPKCE(verifier, challenge); err != nil {
		t.Fatalf("matching verifier should pass: %v", err)
	}
	if err := VerifyPKCE(strings.Repeat("y", 50), challenge); err == nil {
		t.Fatal("wrong verifier must fail")
	}
	if err := VerifyPKCE("", challenge); err == nil {
		t.Fatal("empty verifier must fail")
	}
	if err := VerifyPKCE(strings.Repeat("x", 10), challenge); err == nil {
		t.Fatal("too-short verifier must fail")
	}
	if err := VerifyPKCE(strings.Repeat("x", 200), challenge); err == nil {
		t.Fatal("too-long verifier must fail")
	}
}

func TestCodeIDAndHashAreDerived(t *testing.T) {
	secret, codeID, hash, err := generateCode()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !strings.HasPrefix(secret, "mcpa_") {
		t.Fatalf("code %q missing mcpa_ prefix", secret)
	}
	if CodeID(secret) != codeID {
		t.Fatal("CodeID(secret) must match generated code_id")
	}
	sum := sha256.Sum256([]byte(secret))
	if string(HashCode(secret)) != string(sum[:]) || string(hash) != string(sum[:]) {
		t.Fatal("HashCode must be SHA-256 of the secret")
	}
}

func TestCodeSubjectIDDeterministic(t *testing.T) {
	a := CodeSubjectID("abc")
	if a != CodeSubjectID("abc") {
		t.Fatal("subject id not stable")
	}
	if a == CodeSubjectID("def") {
		t.Fatal("distinct code ids collided")
	}
}
