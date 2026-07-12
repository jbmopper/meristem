package domain

import (
	"errors"
	"testing"
)

func TestValidateNodeOrigin(t *testing.T) {
	for _, good := range []string{
		"https://node.example",
		"https://node.example:8443/",
		"http://127.0.0.1:8080",
		"http://[::1]:8080",
		"http://localhost:8080",
	} {
		if err := ValidateNodeOrigin(good); err != nil {
			t.Errorf("ValidateNodeOrigin(%q): %v", good, err)
		}
	}
	for _, bad := range []string{
		"http://10.0.0.63:8080",
		"https://10.0.0.63:8443",
		"https://169.254.169.254",
		"https://0.0.0.0",
		"https://224.0.0.1",
		"https://metadata.google.internal",
		"https://user:pass@node.example",
		"https://node.example/mcp",
		"https://node.example?x=1",
		"ftp://node.example",
		"node.example",
	} {
		if err := ValidateNodeOrigin(bad); !errors.Is(err, ErrInvalidNodeOrigin) {
			t.Errorf("ValidateNodeOrigin(%q) err = %v", bad, err)
		}
	}
}

func TestCanonicalNodeOrigin(t *testing.T) {
	tests := map[string]string{
		"HTTPS://Node.Example:443/": "https://node.example",
		"https://Node.Example:8443": "https://node.example:8443",
		"http://LOCALHOST:80/":      "http://localhost",
		"http://[::1]:8080/":        "http://[::1]:8080",
	}
	for raw, want := range tests {
		got, err := CanonicalNodeOrigin(raw)
		if err != nil {
			t.Errorf("CanonicalNodeOrigin(%q): %v", raw, err)
			continue
		}
		if got != want {
			t.Errorf("CanonicalNodeOrigin(%q) = %q, want %q", raw, got, want)
		}
	}
}
