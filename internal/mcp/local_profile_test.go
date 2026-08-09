package mcp

import (
	"context"
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
