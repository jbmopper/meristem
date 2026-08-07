package access

import (
	"slices"
	"testing"
	"time"

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

func TestLocalAgentMCPProfileFromActor(t *testing.T) {
	revoked := time.Now()
	validID := uuid.New()
	for _, tc := range []struct {
		name    string
		actor   domain.Token
		marked  bool
		wantErr bool
	}{
		{name: "unmarked", actor: domain.Token{ID: validID, Source: domain.SourceAgent}, marked: false},
		{name: "valid", actor: domain.Token{ID: validID, Source: domain.SourceAgent, Scopes: []string{ScopeMCPLocalAgentProfileV1, ScopeFeedRead}}, marked: true},
		{name: "marker only remains valid but powerless", actor: domain.Token{ID: validID, Source: domain.SourceAgent, Scopes: []string{ScopeMCPLocalAgentProfileV1}}, marked: true},
		{name: "unknown", actor: domain.Token{ID: validID, Source: domain.SourceAgent, Scopes: []string{"mcp.profile:future_v2"}}, marked: true, wantErr: true},
		{name: "repeated", actor: domain.Token{ID: validID, Source: domain.SourceAgent, Scopes: []string{ScopeMCPLocalAgentProfileV1, ScopeMCPLocalAgentProfileV1}}, marked: true, wantErr: true},
		{name: "provider ambiguity", actor: domain.Token{ID: validID, Source: domain.SourceAgent, Scopes: []string{ScopeMCPLocalAgentProfileV1, providerProfileScopePrefix + string(ProviderOwnerTrackerReadV1)}}, marked: true, wantErr: true},
		{name: "nil id", actor: domain.Token{Source: domain.SourceAgent, Scopes: []string{ScopeMCPLocalAgentProfileV1}}, marked: true, wantErr: true},
		{name: "root", actor: domain.Token{ID: validID, IsRoot: true, Source: domain.SourceHuman, Scopes: []string{ScopeMCPLocalAgentProfileV1}}, marked: true, wantErr: true},
		{name: "human", actor: domain.Token{ID: validID, Source: domain.SourceHuman, Scopes: []string{ScopeMCPLocalAgentProfileV1}}, marked: true, wantErr: true},
		{name: "system", actor: domain.Token{ID: validID, Source: domain.SourceSystem, Scopes: []string{ScopeMCPLocalAgentProfileV1}}, marked: true, wantErr: true},
		{name: "revoked", actor: domain.Token{ID: validID, Source: domain.SourceAgent, Scopes: []string{ScopeMCPLocalAgentProfileV1}, RevokedAt: &revoked}, marked: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			marked, err := LocalAgentMCPProfileFromActor(tc.actor)
			if marked != tc.marked || (err != nil) != tc.wantErr {
				t.Fatalf("LocalAgentMCPProfileFromActor() = marked %v err %v, want marked %v err=%v", marked, err, tc.marked, tc.wantErr)
			}
		})
	}
}

func TestProviderAuthorityRejectsLocalMCPMarker(t *testing.T) {
	authority, err := ReduceProviderAuthority(ProviderOwnerTrackerReadV1, uuid.Nil)
	if err != nil {
		t.Fatal(err)
	}
	withLocal := append(append([]string(nil), authority.Scopes...), ScopeMCPLocalAgentProfileV1)
	if _, err := ProviderAuthorityProfileFromScopes(withLocal); err == nil {
		t.Fatal("sealed provider authority accepted a local MCP profile marker")
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
