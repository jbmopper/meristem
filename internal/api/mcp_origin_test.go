package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The Origin guard runs before any credential handling, so these tests need
// no database: a rejected Origin must never reach auth, and an accepted
// request without credentials fails at the auth layer instead (503/401 here
// with the nil-pool server), proving the ordering.

func originRequest(t *testing.T, method, origin string) *http.Response {
	t.Helper()
	s := New(nil, nil)
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)
	req, err := http.NewRequest(method, server.URL+"/mcp", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestMCPOriginAbsentAccepted(t *testing.T) {
	resp := originRequest(t, http.MethodPost, "")
	// No Origin header: the guard passes and the request proceeds to the
	// credential layer (which fails on this DB-less server — but NOT with the
	// origin_forbidden 403).
	if resp.StatusCode == http.StatusForbidden {
		t.Fatalf("absent Origin must not be rejected by the origin guard")
	}
}

func TestMCPOriginPresentRejectedByDefault(t *testing.T) {
	for _, origin := range []string{"https://evil.example", "http://localhost:3000", "null"} {
		resp := originRequest(t, http.MethodPost, origin)
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("origin %q: status = %d, want 403 (default-empty allowlist)", origin, resp.StatusCode)
		}
	}
}

func TestMCPOriginAllowlistedAccepted(t *testing.T) {
	t.Setenv(EnvMCPAllowedOrigins, "https://app.example.com, https://other.example.com")
	resp := originRequest(t, http.MethodPost, "https://app.example.com")
	if resp.StatusCode == http.StatusForbidden {
		t.Fatalf("allowlisted Origin rejected")
	}
	// Exact match only: a subdomain or scheme variation stays rejected.
	sub := originRequest(t, http.MethodPost, "https://sub.app.example.com")
	if sub.StatusCode != http.StatusForbidden {
		t.Errorf("non-exact Origin accepted: %d", sub.StatusCode)
	}
	insecure := originRequest(t, http.MethodPost, "http://app.example.com")
	if insecure.StatusCode != http.StatusForbidden {
		t.Errorf("scheme-variant Origin accepted: %d", insecure.StatusCode)
	}
}

func TestMCPDeleteMethodNotAllowed(t *testing.T) {
	resp := originRequest(t, http.MethodDelete, "")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /mcp status = %d, want 405", resp.StatusCode)
	}
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, "POST") {
		t.Errorf("Allow header = %q", allow)
	}
}

func TestMCPOriginGuardPrecedesDelete(t *testing.T) {
	resp := originRequest(t, http.MethodDelete, "https://evil.example")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("DELETE with bad Origin status = %d, want 403 before 405", resp.StatusCode)
	}
}
