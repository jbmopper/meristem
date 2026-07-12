package access

import (
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestProviderReadGrantCannotGainWritesWhenToolsAreEnabled(t *testing.T) {
	authority, err := ReduceProviderAuthority(ProviderOwnerTrackerReadV1, uuid.Nil)
	if err != nil {
		t.Fatal(err)
	}
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: authority.Scopes}
	for _, tool := range []string{
		"work_items.create", "work_items.spawn_child", "work_items.append_event",
		"work_items.update_metadata", "work_items.transition", "approvals.request",
		"approvals.decide", "connectors.http_request", "registry.activate_cultivar",
	} {
		if ToolVisible(actor, tool) {
			t.Errorf("read profile unexpectedly sees %s", tool)
		}
	}
	if !ToolVisible(actor, "feed.read") || !ToolVisible(actor, "work_items.list") || !ToolVisible(actor, "work_items.get") {
		t.Fatalf("read profile missing coordination reads: %v", authority.Scopes)
	}
}

func TestProviderTrackerWriteProfilesAreNarrow(t *testing.T) {
	for _, tc := range []struct {
		profile ProviderAuthorityProfile
		root    uuid.UUID
	}{
		{ProviderOwnerTrackerWriteV1, uuid.Nil},
		{ProviderDelegatedTreeWriteV1, uuid.New()},
	} {
		authority, err := ReduceProviderAuthority(tc.profile, tc.root)
		if err != nil {
			t.Fatal(err)
		}
		actor := domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: authority.Scopes}
		for _, tool := range []string{"work_items.spawn_child", "work_items.append_event", "work_items.update_metadata", "work_items.transition"} {
			if !ToolVisible(actor, tool) {
				t.Errorf("%s missing tracker tool %s", tc.profile, tool)
			}
		}
		for _, tool := range []string{"approvals.request", "approvals.decide", "connectors.http_request", "registry.activate_cultivar", "convergence.propose_checks"} {
			if ToolVisible(actor, tool) {
				t.Errorf("%s unexpectedly sees non-tracker tool %s", tc.profile, tool)
			}
		}
		if got, err := ProviderAuthorityProfileFromScopes(authority.Scopes); err != nil || got != tc.profile {
			t.Fatalf("profile round trip = %q, %v", got, err)
		}
	}
}

func TestReduceProviderAuthorityIsExactAndTreeBound(t *testing.T) {
	root := uuid.New()
	authority, err := ReduceProviderAuthority(ProviderDelegatedTreeReadV1, root)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(authority.Scopes, WorkItemTreeScope(root)) || slices.Contains(authority.Scopes, ScopeWorkItemsReadAll) {
		t.Fatalf("delegated scopes = %v", authority.Scopes)
	}
	if _, err := ReduceProviderAuthority(ProviderDelegatedTreeReadV1, uuid.Nil); err == nil {
		t.Fatal("delegated profile without tree should fail")
	}
	if _, err := ReduceProviderAuthority(ProviderOwnerTrackerReadV1, root); err == nil {
		t.Fatal("owner profile with tree should fail")
	}
	if _, err := ProviderAuthorityProfileFromScopes([]string{"provider.profile:future_v2"}); err == nil {
		t.Fatal("unknown profile marker should fail")
	}
	read, err := ReduceProviderAuthority(ProviderOwnerTrackerReadV1, uuid.Nil)
	if err != nil {
		t.Fatal(err)
	}
	escalated := append(append([]string{}, read.Scopes...), ScopeWorkItemsTrackerWriteAll)
	if _, err := ProviderAuthorityProfileFromScopes(escalated); err == nil {
		t.Fatal("read marker with an extra write scope should fail closed")
	}
}
