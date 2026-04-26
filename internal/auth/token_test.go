package auth

import (
	"strings"
	"testing"
)

func TestNewSecret_ShapeAndUniqueness(t *testing.T) {
	a, hashA, err := NewSecret()
	if err != nil {
		t.Fatalf("NewSecret: %v", err)
	}
	if !strings.HasPrefix(a, tokenPrefix) {
		t.Errorf("secret missing %q prefix: %q", tokenPrefix, a)
	}
	if !ValidSecretShape(a) {
		t.Errorf("freshly minted secret failed shape check: %q", a)
	}
	if len(hashA) != 32 {
		t.Errorf("expected 32-byte SHA-256 hash, got %d bytes", len(hashA))
	}

	b, hashB, err := NewSecret()
	if err != nil {
		t.Fatalf("NewSecret #2: %v", err)
	}
	if a == b {
		t.Error("two consecutive secrets must not collide; entropy source is broken")
	}
	if EqualHash(hashA, hashB) {
		t.Error("two consecutive hashes must not collide")
	}
}

func TestHashSecret_Stable(t *testing.T) {
	const secret = "mrs_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	a := HashSecret(secret)
	b := HashSecret(secret)
	if !EqualHash(a, b) {
		t.Errorf("HashSecret is non-deterministic: %x vs %x", a, b)
	}
	if len(a) != 32 {
		t.Errorf("expected 32-byte hash, got %d", len(a))
	}
}

func TestValidSecretShape(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"empty", "", false},
		{"missing prefix", "abc", false},
		{"prefix only", "mrs_", false},
		{"non-base64 body", "mrs_!!!notbase64!!!", false},
		{"too short body", "mrs_aaaa", false},
		{"valid", func() string { s, _, _ := NewSecret(); return s }(), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ValidSecretShape(c.in); got != c.want {
				t.Errorf("ValidSecretShape(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestEqualHash(t *testing.T) {
	a := HashSecret("mrs_aaaa")
	b := HashSecret("mrs_aaaa")
	c := HashSecret("mrs_bbbb")
	if !EqualHash(a, b) {
		t.Error("identical inputs should compare equal")
	}
	if EqualHash(a, c) {
		t.Error("different inputs should not compare equal")
	}
	if EqualHash(a, a[:16]) {
		t.Error("length mismatch should compare unequal, never panic")
	}
	if EqualHash(nil, nil) != true {
		t.Error("two nil slices should compare equal (both length 0)")
	}
}
