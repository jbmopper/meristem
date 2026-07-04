package access

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
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

func TestToolVisible_PolicyProfileSwitchRequiresHumanNonRootAndScope(t *testing.T) {
	tests := []struct {
		name  string
		actor domain.Token
		want  bool
	}{
		{
			name:  "legacy_unscoped_human",
			actor: domain.Token{ID: uuid.New(), Source: domain.SourceHuman},
			want:  true,
		},
		{
			name:  "root_human_denied",
			actor: domain.Token{ID: uuid.New(), Source: domain.SourceHuman, IsRoot: true},
			want:  false,
		},
		{
			name:  "agent_denied",
			actor: domain.Token{ID: uuid.New(), Source: domain.SourceAgent},
			want:  false,
		},
		{
			name: "scoped_human_without_policy_scope_denied",
			actor: domain.Token{
				ID:     uuid.New(),
				Source: domain.SourceHuman,
				Scopes: []string{ScopeWorkItemsRead},
			},
			want: false,
		},
		{
			name: "scoped_human_with_policy_scope_allowed",
			actor: domain.Token{
				ID:     uuid.New(),
				Source: domain.SourceHuman,
				Scopes: []string{ScopePolicyProfileSwitch},
			},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToolVisible(tc.actor, "policy_profile.switch"); got != tc.want {
				t.Fatalf("ToolVisible(policy_profile.switch) = %t, want %t", got, tc.want)
			}
		})
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
		"backlog.readiness",
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

// TestFeedItemAnchorsCoverIncludedKinds forces every feed-included event
// kind to be classified in the anchor mapping, mirroring the
// Included/Excluded partition test in the feed package: adding a kind to
// feed.IncludedKinds without deciding its tree-visibility semantics fails
// here.
func TestFeedItemAnchorsCoverIncludedKinds(t *testing.T) {
	subject := uuid.New()
	payloadWorkItem := uuid.New()
	payloadHuman := uuid.New()
	payload := fmt.Appendf(nil,
		`{"work_item_id":%q,"human_work_item_id":%q,"parent_id":%q,"child_id":%q}`,
		payloadWorkItem, payloadHuman, payloadWorkItem, payloadHuman)

	subjectAnchored := []uuid.UUID{subject}
	payloadAnchored := []uuid.UUID{payloadWorkItem, payloadHuman}

	classification := map[string]struct {
		subjectKind string
		want        []uuid.UUID
	}{
		domain.EventMessageCaptured:            {domain.SubjectMessage, payloadAnchored},
		domain.EventWorkItemCreated:            {domain.SubjectWorkItem, subjectAnchored},
		domain.EventWorkItemTransitioned:       {domain.SubjectWorkItem, subjectAnchored},
		domain.EventWorkItemEventAppended:      {domain.SubjectWorkItem, subjectAnchored},
		domain.EventWorkItemRelationAdded:      {domain.SubjectWorkItem, []uuid.UUID{subject, payloadWorkItem, payloadHuman}},
		domain.EventWorkItemMetadataUpdated:    {domain.SubjectWorkItem, subjectAnchored},
		domain.EventSignalReceived:             {domain.SubjectSignal, payloadAnchored},
		domain.EventDeterministicErrorReported: {domain.SubjectDeterministicError, nil},
		domain.EventDeterministicErrorMasked:   {domain.SubjectDeterministicError, nil},
		domain.EventDeterministicErrorUnmasked: {domain.SubjectDeterministicError, nil},
		domain.EventEscalationRequested:        {domain.SubjectEscalation, payloadAnchored},
		domain.EventSubactorGrantRequested:     {domain.SubjectSubactorGrant, payloadAnchored},
		domain.EventSubactorGrantGranted:       {domain.SubjectSubactorGrant, payloadAnchored},
		domain.EventSubactorGrantDenied:        {domain.SubjectSubactorGrant, payloadAnchored},
		domain.EventSubactorGrantEscalated:     {domain.SubjectSubactorGrant, payloadAnchored},
		domain.EventPatienceBreached:           {domain.SubjectWorkItem, subjectAnchored},
		domain.EventConvergenceVerdictRecorded: {domain.SubjectConvergence, subjectAnchored},
		// System-wide owner posture, not tree content: no anchor, dropped
		// from tree-scoped feeds; visible to feed.read.
		domain.EventPolicyProfileSwitched: {domain.SubjectPolicyProfile, nil},
	}

	for _, kind := range feed.IncludedKinds {
		spec, ok := classification[kind]
		if !ok {
			t.Errorf("feed kind %q has no anchor classification; decide its tree-visibility semantics in feedItemAnchors and record it here", kind)
			continue
		}
		got := feedItemAnchors(feed.Item{
			Kind:        kind,
			SubjectKind: spec.subjectKind,
			SubjectID:   subject,
			Payload:     payload,
		})
		if len(got) != len(spec.want) {
			t.Errorf("feedItemAnchors(%s) = %v, want %v", kind, got, spec.want)
			continue
		}
		for i := range got {
			if got[i] != spec.want[i] {
				t.Errorf("feedItemAnchors(%s)[%d] = %s, want %s", kind, i, got[i], spec.want[i])
			}
		}
	}
}

func TestFeedItemAnchorsToleratesMalformedPayload(t *testing.T) {
	got := feedItemAnchors(feed.Item{
		Kind:        domain.EventEscalationRequested,
		SubjectKind: domain.SubjectEscalation,
		SubjectID:   uuid.New(),
		Payload:     []byte(`not json`),
	})
	if len(got) != 0 {
		t.Fatalf("malformed payload must yield no anchors (fail closed), got %v", got)
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
