package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strings"
)

const tokenPrefix = "wln_"

// NewSecret returns a new random bearer token. The raw value is shown once to
// the operator and only its SHA-256 digest is stored.
func NewSecret() (string, []byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, err
	}
	secret := tokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	hash := HashSecret(secret)
	return secret, hash, nil
}

// HashSecret returns the SHA-256 digest stored in the tokens projection.
func HashSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// ValidSecretShape performs a cheap shape check before hashing. It is not a
// security boundary; it just avoids accepting accidental non-meristem strings.
func ValidSecretShape(secret string) bool {
	if !strings.HasPrefix(secret, tokenPrefix) {
		return false
	}
	encoded := strings.TrimPrefix(secret, tokenPrefix)
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	return err == nil && len(decoded) == 32
}

// EqualHash compares two SHA-256 digests without leaking timing information.
func EqualHash(a, b []byte) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare(a, b) == 1
}

var ErrInvalidToken = errors.New("auth: invalid token")
