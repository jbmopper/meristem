package oauth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
)

// codeSubjectNamespace is a fixed v5 namespace so CodeSubjectID is pure and
// stable across processes and rebuilds.
var codeSubjectNamespace = uuid.MustParse("c3e6f9a4-1d20-5b7c-9f42-6a8c0e2f4b16")

// PKCEMethodS256 is the only PKCE code_challenge_method the gateway supports,
// matching the advertised metadata (code_challenge_methods_supported: S256).
const PKCEMethodS256 = "S256"

// CodeTTLSeconds is the short lifetime of an issued authorization code. RFC
// 6749 §4.1.2 recommends a maximum of 10 minutes; the flow completes in
// seconds, so a tight 60s window bounds replay exposure.
const CodeTTLSeconds = 60

// CodeSubjectID derives the deterministic event subject id for an
// authorization code from its non-secret code_id (the hex code hash). The
// issue and redeem events for one code share this subject.
func CodeSubjectID(codeID string) uuid.UUID {
	return uuid.NewSHA1(codeSubjectNamespace, []byte("oauth_authorization_code|"+codeID))
}

// generateCode returns a fresh authorization code secret and its non-secret
// derived id (the hex SHA-256 of the secret). The raw code is delivered once
// to the client in the redirect; only its hash is persisted.
func generateCode() (secret, codeID string, hash []byte, err error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", "", nil, fmt.Errorf("oauth: generate authorization code: %w", err)
	}
	secret = "mcpa_" + base64.RawURLEncoding.EncodeToString(b[:])
	h := sha256.Sum256([]byte(secret))
	return secret, hex.EncodeToString(h[:]), h[:], nil
}

// HashCode returns the SHA-256 digest of an authorization code secret, matching
// what generateCode stores. Used by redemption to look up the code by hash.
func HashCode(secret string) []byte {
	h := sha256.Sum256([]byte(secret))
	return h[:]
}

// CodeID returns the non-secret code id (hex SHA-256) for a code secret.
func CodeID(secret string) string {
	return hex.EncodeToString(HashCode(secret))
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// issuedPayload is the structural payload of an oauth_authorization_code.issued
// event. Only the code hash appears — never the raw code.
type issuedPayload struct {
	PayloadVersion      int       `json:"payload_version"`
	CodeID              string    `json:"code_id"`
	CodeHashB64         string    `json:"code_hash_b64"`
	ClientID            string    `json:"client_id"`
	RedirectURI         string    `json:"redirect_uri"`
	CodeChallenge       string    `json:"code_challenge"`
	CodeChallengeMethod string    `json:"code_challenge_method"`
	Scope               string    `json:"scope,omitempty"`
	Resource            string    `json:"resource"`
	ActorTokenID        uuid.UUID `json:"actor_token_id"`
	AuthorityProfile    string    `json:"authority_profile"`
	ExpiresAtUnix       int64     `json:"expires_at_unix"`
}

// redeemedPayload is the structural payload of an
// oauth_authorization_code.redeemed event.
type redeemedPayload struct {
	PayloadVersion int    `json:"payload_version"`
	CodeID         string `json:"code_id"`
	RedeemedAtUnix int64  `json:"redeemed_at_unix"`
}

// ValidateCodeChallenge checks a PKCE code_challenge from an authorize request.
// Only S256 is accepted; the challenge must be a valid base64url (no padding)
// value of the expected SHA-256 length.
func ValidateCodeChallenge(challenge, method string) error {
	if method == "" {
		return fmt.Errorf("%w: code_challenge_method is required (S256)", ErrInvalidGrant)
	}
	if method != PKCEMethodS256 {
		return fmt.Errorf("%w: code_challenge_method %q unsupported; only S256", ErrInvalidGrant, method)
	}
	if challenge == "" {
		return fmt.Errorf("%w: code_challenge is required", ErrInvalidGrant)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(challenge)
	if err != nil {
		return fmt.Errorf("%w: code_challenge must be base64url (no padding)", ErrInvalidGrant)
	}
	if len(decoded) != sha256.Size {
		return fmt.Errorf("%w: code_challenge is not a SHA-256 digest", ErrInvalidGrant)
	}
	return nil
}

// VerifyPKCE recomputes the S256 challenge from the client's code_verifier and
// compares it to the stored challenge in constant time (RFC 7636 §4.6).
func VerifyPKCE(codeVerifier, storedChallenge string) error {
	if codeVerifier == "" {
		return fmt.Errorf("%w: code_verifier is required", ErrInvalidGrant)
	}
	// RFC 7636 §4.1: verifier is 43..128 chars from the unreserved set.
	if len(codeVerifier) < 43 || len(codeVerifier) > 128 {
		return fmt.Errorf("%w: code_verifier length must be 43..128", ErrInvalidGrant)
	}
	for _, r := range codeVerifier {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '.' || r == '_' || r == '~') {
			return fmt.Errorf("%w: code_verifier contains a character outside the RFC 7636 unreserved set", ErrInvalidGrant)
		}
	}
	sum := sha256.Sum256([]byte(codeVerifier))
	computed := base64.RawURLEncoding.EncodeToString(sum[:])
	if subtle.ConstantTimeCompare([]byte(computed), []byte(storedChallenge)) != 1 {
		return fmt.Errorf("%w: PKCE verification failed", ErrInvalidGrant)
	}
	return nil
}

func verifyStoredS256(codeVerifier, storedChallenge, storedMethod string) error {
	if err := ValidateCodeChallenge(storedChallenge, storedMethod); err != nil {
		return fmt.Errorf("%w: stored PKCE binding is invalid", ErrInvalidGrant)
	}
	return VerifyPKCE(codeVerifier, storedChallenge)
}
