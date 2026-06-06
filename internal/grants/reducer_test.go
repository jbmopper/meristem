package grants

import (
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
)

func TestReduceGrantsReadOnlySameTreeSubset(t *testing.T) {
	root := uuid.New()
	decision := Reduce(Request{
		Parent:            agentToken(root, access.ScopeWorkItemsRead, access.ScopeFeedReadAssigned),
		Template:          TemplateSameTreeReadProgress,
		RequestedSource:   domain.SourceAgent,
		RequestedTreeRoot: root,
		TreeRelation:      TreeSame,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
	})

	if decision.Disposition != DispositionGrant {
		t.Fatalf("disposition = %s, want grant: %s", decision.Disposition, decision.Reason)
	}
	want := map[string]bool{
		access.ScopeFeedReadAssigned:       true,
		access.ScopeWorkItemsRead:          true,
		"work_items.tree:" + root.String(): true,
	}
	if len(decision.Scopes) != len(want) {
		t.Fatalf("scopes = %v, want %v", decision.Scopes, want)
	}
	for _, scope := range decision.Scopes {
		if !want[scope] {
			t.Fatalf("unexpected scope %q in %v", scope, decision.Scopes)
		}
	}
}

func TestReduceEscalatesWriteGrantWithoutApproval(t *testing.T) {
	root := uuid.New()
	decision := Reduce(Request{
		Parent:            agentToken(root, access.ScopeWorkItemsRead, access.ScopeWorkItemsWrite, access.ScopeFeedReadAssigned),
		Template:          TemplateSameTreeWorker,
		RequestedSource:   domain.SourceAgent,
		RequestedTreeRoot: root,
		TreeRelation:      TreeSame,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
	})

	if decision.Disposition != DispositionEscalate {
		t.Fatalf("disposition = %s, want escalate", decision.Disposition)
	}
	if decision.Reason == "" {
		t.Fatal("expected escalation reason")
	}
}

func TestReduceGrantsWriteGrantWithApprovalAndSubset(t *testing.T) {
	root := uuid.New()
	decision := Reduce(Request{
		Parent:            agentToken(root, access.ScopeWorkItemsRead, access.ScopeWorkItemsWrite, access.ScopeFeedReadAssigned),
		Template:          TemplateSameTreeWorker,
		RequestedSource:   domain.SourceAgent,
		RequestedTreeRoot: root,
		TreeRelation:      TreeDescendant,
		HumanReviewStatus: domain.HumanReviewApproved,
	})

	if decision.Disposition != DispositionGrant {
		t.Fatalf("disposition = %s, want grant: %s", decision.Disposition, decision.Reason)
	}
}

func TestReduceEscalatesOutsideTree(t *testing.T) {
	root := uuid.New()
	decision := Reduce(Request{
		Parent:            agentToken(root, access.ScopeWorkItemsRead, access.ScopeFeedReadAssigned),
		Template:          TemplateSameTreeReadProgress,
		RequestedSource:   domain.SourceAgent,
		RequestedTreeRoot: uuid.New(),
		TreeRelation:      TreeOutside,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
	})

	if decision.Disposition != DispositionEscalate {
		t.Fatalf("disposition = %s, want escalate", decision.Disposition)
	}
}

func TestReduceDeniesUnknownTemplate(t *testing.T) {
	root := uuid.New()
	decision := Reduce(Request{
		Parent:            agentToken(root, access.ScopeWorkItemsRead, access.ScopeFeedReadAssigned),
		Template:          Template("anything"),
		RequestedSource:   domain.SourceAgent,
		RequestedTreeRoot: root,
		TreeRelation:      TreeSame,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
	})

	if decision.Disposition != DispositionDeny {
		t.Fatalf("disposition = %s, want deny", decision.Disposition)
	}
}

func TestReduceEscalatesLegacyUnscopedParent(t *testing.T) {
	root := uuid.New()
	decision := Reduce(Request{
		Parent: domain.Token{
			ID:     uuid.New(),
			Source: domain.SourceAgent,
		},
		Template:          TemplateSameTreeReadProgress,
		RequestedSource:   domain.SourceAgent,
		RequestedTreeRoot: root,
		TreeRelation:      TreeSame,
		HumanReviewStatus: domain.HumanReviewWavedThrough,
	})

	if decision.Disposition != DispositionEscalate {
		t.Fatalf("disposition = %s, want escalate", decision.Disposition)
	}
}

func TestReduceEscalatesLogsOrApprovalAuthority(t *testing.T) {
	root := uuid.New()
	cases := []Request{
		{
			Parent:             agentToken(root, access.ScopeWorkItemsRead, access.ScopeFeedReadAssigned),
			Template:           TemplateSameTreeReadProgress,
			RequestedSource:    domain.SourceAgent,
			RequestedTreeRoot:  root,
			RequestedScopes:    []string{access.ScopeWorkItemsRead, access.ScopeFeedReadAssigned, "logs.read", "work_items.tree:" + root.String()},
			TreeRelation:       TreeSame,
			HumanReviewStatus:  domain.HumanReviewWavedThrough,
			RequestedLogsScope: true,
		},
		{
			Parent:            agentToken(root, access.ScopeWorkItemsRead, access.ScopeFeedReadAssigned),
			Template:          TemplateSameTreeReadProgress,
			RequestedSource:   domain.SourceAgent,
			RequestedTreeRoot: root,
			TreeRelation:      TreeSame,
			HumanReviewStatus: domain.HumanReviewWavedThrough,
			ApprovalAuthority: true,
		},
	}
	for _, req := range cases {
		decision := Reduce(req)
		if decision.Disposition != DispositionEscalate {
			t.Fatalf("disposition = %s, want escalate for %+v", decision.Disposition, req)
		}
	}
}

func agentToken(root uuid.UUID, scopes ...string) domain.Token {
	scopes = append(scopes, "work_items.tree:"+root.String())
	return domain.Token{
		ID:     uuid.New(),
		Source: domain.SourceAgent,
		Scopes: scopes,
	}
}
