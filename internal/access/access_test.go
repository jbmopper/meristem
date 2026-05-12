package access

import (
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestToolVisible_LegacyAndRootSeeExistingSurface(t *testing.T) {
	for _, actor := range []domain.Token{
		{ID: uuid.New(), Source: domain.SourceAgent},
		{ID: uuid.New(), Source: domain.SourceHuman, IsRoot: true},
	} {
		for _, tool := range []string{
			"inbox.capture",
			"feed.read",
			"deterministic_errors.list",
			"work_items.create",
			"work_items.transition",
		} {
			if !ToolVisible(actor, tool) {
				t.Fatalf("ToolVisible(%+v, %q) = false, want true", actor, tool)
			}
		}
	}
}

func TestToolVisible_ScopedWorkerSurface(t *testing.T) {
	root := uuid.New()
	actor := domain.Token{
		ID:     uuid.New(),
		Source: domain.SourceAgent,
		Scopes: []string{
			ScopeWorkItemsRead,
			ScopeWorkItemsWrite,
			ScopeFeedReadAssigned,
			scopeWorkItemsTreePrefix + root.String(),
		},
	}
	visible := []string{
		"feed.read",
		"work_items.list",
		"work_items.get",
		"work_items.spawn_child",
		"work_items.append_event",
		"work_items.update_metadata",
		"work_items.transition",
	}
	for _, tool := range visible {
		if !ToolVisible(actor, tool) {
			t.Errorf("ToolVisible(%q) = false, want true", tool)
		}
	}
	hidden := []string{
		"inbox.capture",
		"deterministic_errors.list",
		"deterministic_errors.get",
		"work_items.create",
	}
	for _, tool := range hidden {
		if ToolVisible(actor, tool) {
			t.Errorf("ToolVisible(%q) = true, want false", tool)
		}
	}
}

func TestToolVisible_ScopedToolsRequireUsableTree(t *testing.T) {
	root := uuid.New()
	tests := []struct {
		name   string
		scopes []string
		tool   string
	}{
		{
			name: "read_without_tree",
			scopes: []string{
				ScopeWorkItemsRead,
			},
			tool: "work_items.get",
		},
		{
			name: "write_without_tree",
			scopes: []string{
				ScopeWorkItemsWrite,
			},
			tool: "work_items.transition",
		},
		{
			name: "assigned_feed_without_tree",
			scopes: []string{
				ScopeFeedReadAssigned,
			},
			tool: "feed.read",
		},
		{
			name: "typo_does_not_fall_back_to_legacy",
			scopes: []string{
				"work_item.tree:" + root.String(),
				ScopeWorkItemsRead,
				ScopeFeedReadAssigned,
			},
			tool: "work_items.get",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actor := domain.Token{
				ID:     uuid.New(),
				Source: domain.SourceAgent,
				Scopes: tc.scopes,
			}
			if ToolVisible(actor, tc.tool) {
				t.Fatalf("ToolVisible(%q) = true, want false for scopes %v", tc.tool, tc.scopes)
			}
		})
	}
}

func TestWorkItemTreeRoots(t *testing.T) {
	a := uuid.New()
	b := uuid.New()
	got := workItemTreeRoots(domain.Token{Scopes: []string{
		" " + scopeWorkItemsTreePrefix + a.String() + " ",
		"work_items.tree:not-a-uuid",
		scopeWorkItemsTreePrefix + b.String(),
	}})
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("roots = %v, want [%s %s]", got, a, b)
	}
}
