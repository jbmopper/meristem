package idempotency

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestSubjectIDIsStableForSameRequestAndLabel(t *testing.T) {
	tokenID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	ctx := withRequest(context.Background(), Request{
		TokenID:     tokenID,
		Scope:       "POST /v1/work-items",
		Key:         "request-1",
		RequestHash: []byte("same-body"),
	})

	first, ok := SubjectID(ctx, "work_item")
	if !ok {
		t.Fatal("expected subject id")
	}
	second, ok := SubjectID(ctx, "work_item")
	if !ok {
		t.Fatal("expected second subject id")
	}
	if first != second {
		t.Fatalf("subject id drifted: %s != %s", first, second)
	}
}

func TestSubjectIDSeparatesLabelsAndRequestBodies(t *testing.T) {
	tokenID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	base := Request{
		TokenID:     tokenID,
		Scope:       "POST /v1/inbox/messages",
		Key:         "request-1",
		RequestHash: []byte("same-body"),
	}
	messageID, ok := SubjectID(withRequest(context.Background(), base), "inbox.message")
	if !ok {
		t.Fatal("expected message subject id")
	}
	workItemID, ok := SubjectID(withRequest(context.Background(), base), "inbox.work_item")
	if !ok {
		t.Fatal("expected work item subject id")
	}
	if messageID == workItemID {
		t.Fatal("different labels should not collapse")
	}

	base.RequestHash = []byte("different-body")
	differentBodyID, ok := SubjectID(withRequest(context.Background(), base), "inbox.message")
	if !ok {
		t.Fatal("expected different-body subject id")
	}
	if messageID == differentBodyID {
		t.Fatal("different request bodies should not collapse")
	}
}

func TestSubjectIDRequiresIdempotencyContext(t *testing.T) {
	if id, ok := SubjectID(context.Background(), "work_item"); ok {
		t.Fatalf("expected no id without context, got %s", id)
	}
}
