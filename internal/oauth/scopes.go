package oauth

import (
	"fmt"
	"strings"

	"github.com/jbmopper/meristem/internal/access"
)

const (
	ScopeMCPRead         = "mcp:read"
	ScopeMCPTrackerWrite = "mcp:tracker_write"
)

var supportedOAuthScopes = []string{ScopeMCPRead, ScopeMCPTrackerWrite}

// OAuthScopeForAuthorityProfile is the canonical OAuth scope string for one
// sealed provider authority profile. OAuth scopes describe the coarse
// provider contract; the actor token's sealed Meristem scopes remain the
// object-level authorization boundary.
func OAuthScopeForAuthorityProfile(profile access.ProviderAuthorityProfile) (string, error) {
	switch profile {
	case access.ProviderOwnerTrackerReadV1, access.ProviderDelegatedTreeReadV1:
		return ScopeMCPRead, nil
	case access.ProviderOwnerTrackerWriteV1, access.ProviderDelegatedTreeWriteV1:
		return ScopeMCPRead + " " + ScopeMCPTrackerWrite, nil
	default:
		return "", access.ErrInvalidProviderAuthority
	}
}

// normalizeRegistrationScope accepts only the two exact contracts that a
// later sealed profile can realize. An omitted DCR scope defaults to read.
func normalizeRegistrationScope(raw string) (string, error) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return ScopeMCPRead, nil
	}
	set, ok := exactScopeSet(fields)
	if ok && len(set) == 1 && set[ScopeMCPRead] {
		return ScopeMCPRead, nil
	}
	if ok && len(set) == 2 && set[ScopeMCPRead] && set[ScopeMCPTrackerWrite] {
		return ScopeMCPRead + " " + ScopeMCPTrackerWrite, nil
	}
	return "", fmt.Errorf("supported scope contracts are %q or %q", ScopeMCPRead, ScopeMCPRead+" "+ScopeMCPTrackerWrite)
}

func normalizeScopeForProfile(raw string, profile access.ProviderAuthorityProfile) (string, error) {
	expected, err := OAuthScopeForAuthorityProfile(profile)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(raw) == "" {
		return expected, nil
	}
	got, err := normalizeRegistrationScope(raw)
	if err != nil {
		return "", fmt.Errorf("%w: scope must exactly match sealed profile scope %q", ErrInvalidAuthorizationRequest, expected)
	}
	if got != expected {
		return "", fmt.Errorf("%w: scope must exactly match sealed profile scope %q", ErrInvalidAuthorizationRequest, expected)
	}
	return expected, nil
}

func exactScopeSet(fields []string) (map[string]bool, bool) {
	set := make(map[string]bool, len(fields))
	for _, field := range fields {
		if set[field] {
			return nil, false
		}
		set[field] = true
	}
	return set, true
}

func SupportedOAuthScopes() []string {
	return append([]string(nil), supportedOAuthScopes...)
}
