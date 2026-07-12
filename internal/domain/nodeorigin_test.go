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
