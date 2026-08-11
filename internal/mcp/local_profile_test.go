package mcp

import (
	"context"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
)

func TestLocalMCPProfileSelectsScopeDerivedStdioSurface(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{
		ID:     uuid.New(),
		Source: domain.SourceAgent,
		Scopes: []string{access.ScopeMCPLocalAgentProfileV1, access.ScopeFeedRead},
	}
	result, rerr := s.gatedToolsList(actor)
	if rerr != nil {
		t.Fatalf("gatedToolsList: %+v", rerr)
	}
	body := result.(map[string]any)
	tools := body["tools"].([]toolDescriptor)
	if len(tools) != 1 || tools[0].Name != "feed.read" {
		t.Fatalf("local marker surface = %+v, want scope-derived feed.read only", tools)
	}
	profile, err := HTTPProfileForActor(actor)
	if err != nil || profile.Name() != LocalAgentHTTPProfile().Name() || profile.providerSafe() {
		t.Fatalf("HTTPProfileForActor = %#v, %v", profile, err)
	}
}

func TestLocalMCPProfileMarkerAloneGrantsNoBusinessTools(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	actor := domain.Token{
		ID:     uuid.New(),
		Source: domain.SourceAgent,
		Scopes: []string{access.ScopeMCPLocalAgentProfileV1},
	}
	result, rerr := s.gatedToolsList(actor)
	if rerr != nil {
		t.Fatalf("marker-only gatedToolsList: %+v", rerr)
	}
	body := result.(map[string]any)
	if tools := body["tools"].([]toolDescriptor); len(tools) != 0 {
		t.Fatalf("local profile marker granted business tools: %+v", tools)
	}
}

func TestListenerTaskProfileIsExactAndMarkerAloneFailsClosed(t *testing.T) {
	root := uuid.New()
	actorID := uuid.New()
	scopes, err := access.ListenerTaskMCPScopes(root)
	if err != nil {
		t.Fatal(err)
	}
	actor := domain.Token{ID: actorID, Source: domain.SourceAgent, Scopes: scopes}
	profile, marked, err := mcpProfileForActor(actor)
	if err != nil || !marked || profile.Name() != ListenerTaskHTTPProfile().Name() {
		t.Fatalf("listener task profile=%#v marked=%v err=%v", profile, marked, err)
	}
	if got, want := profile.allowedTools, toolSet("work_items.get", "work_items.get_assignment", "work_items.append_event"); !maps.Equal(got, want) {
		t.Fatalf("listener task tools=%v want=%v", got, want)
	}
	if !profile.providerSafe() {
		t.Fatal("listener task profile must use the closed response reducer")
	}
	markerOnly := domain.Token{ID: actorID, Source: domain.SourceAgent, Scopes: []string{access.ScopeMCPListenerTaskProfileV1}}
	if _, marked, err := mcpProfileForActor(markerOnly); !marked || err == nil {
		t.Fatalf("marker-only task profile marked=%v err=%v, want fail closed", marked, err)
	}
}

func TestListenerTaskServerInfoAttestsAuthenticatedActorOnly(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem", Version: "test"}, nil)
	if _, ok := s.serverInfo("test")["description"]; ok {
		t.Fatal("ordinary MCP server unexpectedly emitted listener task attestation")
	}
	actorID := uuid.New()
	s.actor = domain.Token{ID: actorID, Source: domain.SourceAgent}
	s.task = &ListenerTaskBinding{ExpectedActorID: actorID}
	info := s.serverInfo("test")
	if got, want := info["description"], listenerTaskAttestationPrefix+actorID.String(); got != want {
		t.Fatalf("task attestation=%v want=%q", got, want)
	}
}

func TestMalformedLocalMCPProfilesFailSharedDispatcher(t *testing.T) {
	s := New(Deps{}, ServerInfo{Name: "meristem-test", Version: "test"}, nil)
	revoked := time.Now()
	for _, actor := range []domain.Token{
		{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{"mcp.profile:future_v2", access.ScopeFeedRead}},
		{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{access.ScopeMCPLocalAgentProfileV1, access.ScopeMCPLocalAgentProfileV1, access.ScopeFeedRead}},
		{ID: uuid.New(), Source: domain.SourceHuman, Scopes: []string{access.ScopeMCPLocalAgentProfileV1, access.ScopeFeedRead}},
		{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{access.ScopeMCPLocalAgentProfileV1, access.ScopeFeedRead}, RevokedAt: &revoked},
	} {
		if _, rerr := s.gatedToolsList(actor); rerr == nil {
			t.Fatalf("malformed local actor was accepted: %+v", actor)
		}
		resp := s.HandleHTTPMessageWithOptions(
			context.Background(),
			[]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`),
			actor,
			HTTPOptions{},
		)
		if !strings.Contains(string(resp.Body), "invalid MCP actor profile") {
			t.Fatalf("HTTP list gate accepted malformed local actor: %+v body=%s", actor, resp.Body)
		}
	}
}

func TestUnmarkedHTTPActorRetainsProviderSafeFallback(t *testing.T) {
	actor := domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Scopes: []string{access.ScopeFeedRead, access.ScopeWorkItemsReadAll}}
	profile, err := HTTPProfileForActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name() != ProviderSafeReadHTTPProfile().Name() || !profile.restrictTools || !profile.providerSafe() {
		t.Fatalf("unmarked HTTP profile = %#v, want provider-safe fallback", profile)
	}
}
