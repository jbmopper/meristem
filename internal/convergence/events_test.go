package convergence

import (
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

func TestVerdictEventSpecUsesWorkItemSubject(t *testing.T) {
	workItemID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	actorID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	red, err := Run(MajorityVote{SignalKind: "grader.pass"}, []Signal{
		{Kind: "grader.pass", Pass: boolp(true)},
	}, 4)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	spec, err := VerdictEventSpec(domain.SourceSystem, &actorID, workItemID, red)
	if err != nil {
		t.Fatalf("VerdictEventSpec: %v", err)
	}
	if spec.SubjectKind != domain.SubjectConvergence {
		t.Fatalf("subject kind = %q, want %q", spec.SubjectKind, domain.SubjectConvergence)
	}
	if spec.SubjectID != workItemID {
		t.Fatalf("subject id = %s, want work item id %s", spec.SubjectID, workItemID)
	}
	if spec.Kind != domain.EventConvergenceVerdictRecorded {
		t.Fatalf("kind = %q, want %q", spec.Kind, domain.EventConvergenceVerdictRecorded)
	}
	if spec.Source != domain.SourceSystem {
		t.Fatalf("source = %q, want system", spec.Source)
	}
	if spec.ActorTokenID == nil || *spec.ActorTokenID != actorID {
		t.Fatalf("actor token id = %v, want %s", spec.ActorTokenID, actorID)
	}
	payload, ok := spec.Payload.(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T, want map[string]any", spec.Payload)
	}
	if payload["attempt"] != 4 {
		t.Fatalf("attempt = %v, want 4", payload["attempt"])
	}
}

func TestVerdictEventSpecValidatesReduction(t *testing.T) {
	if _, err := VerdictEventSpec(domain.SourceSystem, nil, uuid.New(), Reduction{}); err == nil {
		t.Fatal("zero reduction should be rejected")
	}
	if _, err := VerdictEventSpec(domain.Source("bogus"), nil, uuid.New(), Reduction{}); err == nil {
		t.Fatal("invalid source should be rejected")
	}
	if _, err := VerdictEventSpec(domain.SourceSystem, nil, uuid.Nil, Reduction{}); err == nil {
		t.Fatal("nil work_item_id should be rejected")
	}
}
