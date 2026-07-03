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

func TestEventDiscriminator(t *testing.T) {
	tokenID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	base := Request{
		TokenID:     tokenID,
		Scope:       "MCP:work_items.transition",
		Key:         "key-1",
		RequestHash: []byte("body"),
	}

	if disc, ok := EventDiscriminator(context.Background()); ok || disc != "" {
		t.Fatalf("expected no discriminator outside idempotency context, got %q ok=%v", disc, ok)
	}

	first, ok := EventDiscriminator(withRequest(context.Background(), base))
	if !ok || first == "" {
		t.Fatal("expected discriminator under idempotency context")
	}
	retry, _ := EventDiscriminator(withRequest(context.Background(), base))
	if retry != first {
		t.Fatalf("discriminator must be stable across retries: %q != %q", retry, first)
	}

	otherKey := base
	otherKey.Key = "key-2"
	second, _ := EventDiscriminator(withRequest(context.Background(), otherKey))
	if second == first {
		t.Fatalf("distinct keys must yield distinct discriminators: %q", first)
	}

	otherBody := base
	otherBody.RequestHash = []byte("different-body")
	sameAction, _ := EventDiscriminator(withRequest(context.Background(), otherBody))
	if sameAction != first {
		t.Fatalf("request hash must not influence the discriminator (conflicts are the idempotency layer's job): %q != %q", sameAction, first)
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
