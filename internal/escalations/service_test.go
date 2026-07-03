package escalations

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestRequestRejectsMissingWorkItemID(t *testing.T) {
	s := NewService(nil, nil)
	_, err := s.Request(context.Background(), RequestInput{
		Reason: "needs human",
		Actor:  testActor(),
	})
	if err == nil || !strings.Contains(err.Error(), "work_item_id is required") {
		t.Fatalf("expected work_item_id error, got %v", err)
	}
}

func TestRequestRejectsBlankReason(t *testing.T) {
	s := NewService(nil, nil)
	_, err := s.Request(context.Background(), RequestInput{
		WorkItemID: uuid.New(),
		Reason:     " \t\n",
		Actor:      testActor(),
	})
	if err == nil || !strings.Contains(err.Error(), "reason is required") {
		t.Fatalf("expected reason error, got %v", err)
	}
}

func TestDeterministicIDsAreStable(t *testing.T) {
	workItemID := uuid.New()
	a := deterministicEscalationID(workItemID, "needs human", "summary")
	b := deterministicEscalationID(workItemID, "needs human", "summary")
	if a != b {
		t.Fatalf("escalation id is not stable: %s vs %s", a, b)
	}
	if deterministicHumanWorkItemID(a) != deterministicHumanWorkItemID(b) {
		t.Fatalf("human work item id is not stable")
	}
	if c := deterministicEscalationID(workItemID, "other", "summary"); c == a {
		t.Fatalf("expected reason to separate escalation ids")
	}
}

func testActor() domain.Token {
	return domain.Token{ID: uuid.New(), Source: domain.SourceAgent, Name: "agent"}
}
