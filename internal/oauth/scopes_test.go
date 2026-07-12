package oauth

import (
	"testing"

	"github.com/jbmopper/meristem/internal/access"
)

func TestOAuthScopeContractsMatchSealedProfiles(t *testing.T) {
	for _, tc := range []struct {
		profile access.ProviderAuthorityProfile
		want    string
	}{
		{access.ProviderOwnerTrackerReadV1, ScopeMCPRead},
		{access.ProviderDelegatedTreeReadV1, ScopeMCPRead},
		{access.ProviderOwnerTrackerWriteV1, ScopeMCPRead + " " + ScopeMCPTrackerWrite},
		{access.ProviderDelegatedTreeWriteV1, ScopeMCPRead + " " + ScopeMCPTrackerWrite},
	} {
		got, err := OAuthScopeForAuthorityProfile(tc.profile)
		if err != nil || got != tc.want {
			t.Fatalf("profile %s scope=%q err=%v want=%q", tc.profile, got, err, tc.want)
		}
		if got, err := normalizeScopeForProfile("", tc.profile); err != nil || got != tc.want {
			t.Fatalf("profile %s empty default=%q err=%v", tc.profile, got, err)
		}
	}
}

func TestRegistrationScopeAcceptsOnlyExactContracts(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
		ok   bool
	}{
		{"", ScopeMCPRead, true},
		{ScopeMCPRead, ScopeMCPRead, true},
		{ScopeMCPRead + " " + ScopeMCPTrackerWrite, ScopeMCPRead + " " + ScopeMCPTrackerWrite, true},
		{ScopeMCPTrackerWrite, "", false},
		{ScopeMCPTrackerWrite + " " + ScopeMCPRead, ScopeMCPRead + " " + ScopeMCPTrackerWrite, true},
		{ScopeMCPRead + " " + ScopeMCPRead, "", false},
		{ScopeMCPRead + " unknown", "", false},
	} {
		got, err := normalizeRegistrationScope(tc.raw)
		if (err == nil) != tc.ok || got != tc.want {
			t.Errorf("scope %q => %q err=%v; want %q ok=%v", tc.raw, got, err, tc.want, tc.ok)
		}
	}
}
