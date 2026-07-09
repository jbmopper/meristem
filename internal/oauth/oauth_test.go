package oauth

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateRedirectURIs(t *testing.T) {
	cases := []struct {
		name    string
		in      []string
		wantErr bool
	}{
		{"https ok", []string{"https://claude.ai/api/mcp/auth_callback"}, false},
		{"http loopback ok", []string{"http://127.0.0.1:8976/callback"}, false},
		{"http localhost ok", []string{"http://localhost:1234/cb"}, false},
		{"multiple ok", []string{"https://a.example/cb", "https://b.example/cb"}, false},
		{"empty list", nil, true},
		{"empty string", []string{"  "}, true},
		{"http non-loopback", []string{"http://evil.example/cb"}, true},
		{"non-absolute", []string{"/relative/cb"}, true},
		{"fragment", []string{"https://a.example/cb#frag"}, true},
		{"bad scheme", []string{"ftp://a.example/cb"}, true},
		{"custom scheme rejected", []string{"myapp://cb"}, true},
		{"too many", func() []string {
			s := make([]string, MaxRedirectURIs+1)
			for i := range s {
				s[i] = "https://a.example/cb"
			}
			return s
		}(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateRedirectURIs(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %v", got)
				}
				if !errors.Is(err, ErrInvalidRegistration) {
					t.Fatalf("error should wrap ErrInvalidRegistration, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.in) {
				t.Fatalf("got %d uris, want %d", len(got), len(tc.in))
			}
		})
	}
}

func TestGenerateClientIDIsUniqueAndPrefixed(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id, err := generateClientID()
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
		if !strings.HasPrefix(id, "mcpc_") {
			t.Fatalf("client_id %q missing mcpc_ prefix", id)
		}
		if seen[id] {
			t.Fatalf("duplicate client_id %q", id)
		}
		seen[id] = true
	}
}

func TestClientSubjectIDDeterministic(t *testing.T) {
	a := ClientSubjectID("mcpc_abc")
	b := ClientSubjectID("mcpc_abc")
	if a != b {
		t.Fatalf("subject id not stable: %s vs %s", a, b)
	}
	if c := ClientSubjectID("mcpc_def"); c == a {
		t.Fatalf("distinct client_ids collided on subject id")
	}
}

func TestAllowsRedirectURIExactMatch(t *testing.T) {
	c := Client{RedirectURIs: []string{"https://a.example/cb"}}
	if !c.AllowsRedirectURI("https://a.example/cb") {
		t.Fatal("exact match should be allowed")
	}
	if c.AllowsRedirectURI("https://a.example/cb/") {
		t.Fatal("trailing-slash variant must not match")
	}
	if c.AllowsRedirectURI("https://a.example") {
		t.Fatal("prefix must not match")
	}
}
