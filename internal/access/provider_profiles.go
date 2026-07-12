package access

import (
	"errors"
	"strings"

	"github.com/google/uuid"
)

// ProviderAuthorityProfile is a versioned, sealed scope bundle. OAuth grants
// persist the returned scopes; adding MCP tools later therefore cannot upgrade
// an already-issued read profile into a writer.
type ProviderAuthorityProfile string

const (
	ProviderOwnerTrackerReadV1   ProviderAuthorityProfile = "owner_tracker_read_v1"
	ProviderOwnerTrackerWriteV1  ProviderAuthorityProfile = "owner_tracker_write_v1"
	ProviderDelegatedTreeReadV1  ProviderAuthorityProfile = "delegated_tree_read_v1"
	ProviderDelegatedTreeWriteV1 ProviderAuthorityProfile = "delegated_tree_write_v1"
	providerProfileScopePrefix                            = "provider.profile:"
)

var ErrInvalidProviderAuthority = errors.New("access: invalid provider authority profile")

type ProviderAuthority struct {
	Profile ProviderAuthorityProfile
	Scopes  []string
}

// ReduceProviderAuthority deterministically expands one reviewed profile into
// the exact scopes stored on its OAuth actor token. It does not accept an
// arbitrary requested-scope list: changing authority requires selecting and
// approving another versioned profile.
func ReduceProviderAuthority(profile ProviderAuthorityProfile, treeRoot uuid.UUID) (ProviderAuthority, error) {
	marker := providerProfileScopePrefix + string(profile)
	var scopes []string
	switch profile {
	case ProviderOwnerTrackerReadV1:
		if treeRoot != uuid.Nil {
			return ProviderAuthority{}, ErrInvalidProviderAuthority
		}
		scopes = []string{marker, ScopeFeedRead, ScopeWorkItemsReadAll}
	case ProviderOwnerTrackerWriteV1:
		if treeRoot != uuid.Nil {
			return ProviderAuthority{}, ErrInvalidProviderAuthority
		}
		scopes = []string{marker, ScopeFeedRead, ScopeWorkItemsReadAll, ScopeWorkItemsTrackerWriteAll}
	case ProviderDelegatedTreeReadV1:
		if treeRoot == uuid.Nil {
			return ProviderAuthority{}, ErrInvalidProviderAuthority
		}
		scopes = []string{marker, ScopeFeedReadAssigned, ScopeWorkItemsRead, WorkItemTreeScope(treeRoot)}
	case ProviderDelegatedTreeWriteV1:
		if treeRoot == uuid.Nil {
			return ProviderAuthority{}, ErrInvalidProviderAuthority
		}
		scopes = []string{marker, ScopeFeedReadAssigned, ScopeWorkItemsRead, ScopeWorkItemsTrackerWrite, WorkItemTreeScope(treeRoot)}
	default:
		return ProviderAuthority{}, ErrInvalidProviderAuthority
	}
	return ProviderAuthority{Profile: profile, Scopes: scopes}, nil
}

func WorkItemTreeScope(root uuid.UUID) string {
	return scopeWorkItemsTreePrefix + root.String()
}

// ProviderAuthorityProfileFromScopes returns the single sealed profile marker.
// Missing, duplicated, or unknown markers fail closed.
func ProviderAuthorityProfileFromScopes(scopes []string) (ProviderAuthorityProfile, error) {
	var found ProviderAuthorityProfile
	var treeRoot uuid.UUID
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if strings.HasPrefix(scope, scopeWorkItemsTreePrefix) {
			candidate, err := uuid.Parse(strings.TrimPrefix(scope, scopeWorkItemsTreePrefix))
			if err != nil || candidate == uuid.Nil || treeRoot != uuid.Nil {
				return "", ErrInvalidProviderAuthority
			}
			treeRoot = candidate
		}
		if !strings.HasPrefix(scope, providerProfileScopePrefix) {
			continue
		}
		if found != "" {
			return "", ErrInvalidProviderAuthority
		}
		found = ProviderAuthorityProfile(strings.TrimPrefix(scope, providerProfileScopePrefix))
	}
	switch found {
	case ProviderOwnerTrackerReadV1, ProviderOwnerTrackerWriteV1, ProviderDelegatedTreeReadV1, ProviderDelegatedTreeWriteV1:
	default:
		return "", ErrInvalidProviderAuthority
	}
	expected, err := ReduceProviderAuthority(found, treeRoot)
	if err != nil || !sameScopeSet(scopes, expected.Scopes) {
		return "", ErrInvalidProviderAuthority
	}
	return found, nil
}

func sameScopeSet(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	want := make(map[string]bool, len(expected))
	for _, scope := range expected {
		want[scope] = true
	}
	for _, scope := range actual {
		scope = strings.TrimSpace(scope)
		if !want[scope] {
			return false
		}
		delete(want, scope)
	}
	return len(want) == 0
}
