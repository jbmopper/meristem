package cursorcli

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestRenderScaffoldIncludesWorkerContract(t *testing.T) {
	id := uuid.New()
	out, err := RenderScaffold(ScaffoldInput{
		WorkItem: domain.WorkItem{
			ID:                         id,
			Title:                      "Build the thing",
			State:                      domain.WorkItemPlanned,
			SuggestedConvergenceChecks: []string{"go test ./...", "go vet ./..."},
			HumanReviewStatus:          domain.HumanReviewApproved,
			CreatedAt:                  time.Unix(0, 0),
			UpdatedAt:                  time.Unix(0, 0),
		},
		Scope:        "Implement only the provider scaffold.",
		AllowedAreas: []string{"cmd/meristem", "internal/providers/cursorcli"},
		OutOfScope:   []string{"Launching Cursor processes"},
		TokenFile:    ".meristem/test-cursor.token",
		RepoRoot:     "/tmp/repo with spaces",
	})
	if err != nil {
		t.Fatalf("RenderScaffold: %v", err)
	}

	for _, want := range []string{
		"Provider: `cursor-cli`",
		"Model: `composer2`",
		"Assigned work_item: `" + id.String() + "`",
		"Human review status: `approved`",
		"- go test ./...",
		"- go vet ./...",
		"MERISTEM_MCP_TOOL_NAMES=cursor",
		"$(tr -d '\\n' < '.meristem/test-cursor.token')",
		"Do not introduce `agent_kind`",
		"Worker AGENTS.md Overlay",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("scaffold missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "mrs_") {
		t.Fatalf("scaffold appears to contain a bearer token:\n%s", out)
	}
}

func TestRenderScaffoldRequiresScopeAndAllowedArea(t *testing.T) {
	item := domain.WorkItem{ID: uuid.New(), Title: "x"}
	cases := []struct {
		name string
		in   ScaffoldInput
		want string
	}{
		{
			name: "missing scope",
			in:   ScaffoldInput{WorkItem: item, AllowedAreas: []string{"."}},
			want: "scope is required",
		},
		{
			name: "missing allowed area",
			in:   ScaffoldInput{WorkItem: item, Scope: "do it"},
			want: "at least one allowed area",
		},
		{
			name: "missing id",
			in: ScaffoldInput{
				WorkItem:     domain.WorkItem{},
				Scope:        "do it",
				AllowedAreas: []string{"."},
			},
			want: "work item id is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RenderScaffold(tc.in)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestRenderScaffoldDefaultsOutOfScopeAndHumanReview(t *testing.T) {
	out, err := RenderScaffold(ScaffoldInput{
		WorkItem:     domain.WorkItem{ID: uuid.New(), Title: "x"},
		Scope:        "do it",
		AllowedAreas: []string{"."},
	})
	if err != nil {
		t.Fatalf("RenderScaffold: %v", err)
	}
	for _, want := range []string{
		"Human review status: `waved_through`",
		"Secrets, unrelated refactors, and external writes without approval.",
		"No explicit checks are recorded",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("scaffold missing default %q:\n%s", want, out)
		}
	}
}
