package worker

// Append-only remediation of historical string-shaped inners (c9dd47d5,
// follow-up to the e5a975cb boundary). The reducer's interpretation rules are
// pinned here: a first-accepted payload_shape.remediated annotation speaks
// ONLY for an inner that direct decoding could not read; originals always
// win over annotations; later annotations never flip an interpretation; and
// review verdicts are never remediated. Original events are immutable
// throughout — every remediation is a new, remediator-attributed event.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/app"
	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/workitems"
)

// rawAppendedEventID computes the deterministic event id appendRawWorkItemEvent
// produces for payload, so tests can reference the source event the way a real
// remediator would (by immutable event id).
func rawAppendedEventID(t *testing.T, itemID uuid.UUID, payload any) uuid.UUID {
	t.Helper()
	id, err := events.DeterministicID(events.Spec{
		SubjectKind: domain.SubjectWorkItem,
		SubjectID:   itemID,
		Kind:        domain.EventWorkItemEventAppended,
		Source:      domain.SourceSystem,
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("deterministic id: %v", err)
	}
	return id
}

func TestPayloadRemediationInterpretationIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "remediation-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	systemTok, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "remediation-worker", Source: domain.SourceSystem, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	remediator, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "remediator", Source: domain.SourceAgent, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create remediator token: %v", err)
	}
	service := workitems.NewService(pool, writer)

	newItem := func(title string, checks []string) domain.WorkItem {
		item, err := service.Create(ctx, workitems.CreateInput{
			Title: title, State: domain.WorkItemRunning,
			SuggestedConvergenceChecks: checks,
			HumanReviewStatus:          domain.HumanReviewWavedThrough,
			Actor:                      systemTok.Token,
		})
		if err != nil {
			t.Fatalf("create %s: %v", title, err)
		}
		return item
	}
	annotate := func(itemID, sourceEventID uuid.UUID, parsed map[string]any) {
		if err := service.AppendEvent(ctx, itemID, workitems.PayloadShapeRemediatedInnerKind, map[string]any{
			"source_event_id": sourceEventID.String(),
			"parsed":          parsed,
		}, remediator.Token); err != nil {
			t.Fatalf("append remediation: %v", err)
		}
	}

	// Item A: unrecoverable string signal, remediated → converges.
	itemA := newItem("remediated unrecoverable signal", []string{"remedy-a"})
	badPayloadA := map[string]any{"inner_kind": "checklist.item:remedy-a", "inner": `{"pass": true`}
	appendRawWorkItemEvent(t, ctx, pool, writer, systemTok.Token, itemA.ID, badPayloadA)
	annotate(itemA.ID, rawAppendedEventID(t, itemA.ID, badPayloadA), map[string]any{"pass": true})

	// Item B: two annotations for one source; the FIRST (pass=false) wins, so
	// the item must stay running despite the later pass=true.
	itemB := newItem("first-accepted annotation wins", []string{"remedy-b"})
	badPayloadB := map[string]any{"inner_kind": "checklist.item:remedy-b", "inner": `{"pass": maybe`}
	appendRawWorkItemEvent(t, ctx, pool, writer, systemTok.Token, itemB.ID, badPayloadB)
	sourceB := rawAppendedEventID(t, itemB.ID, badPayloadB)
	annotate(itemB.ID, sourceB, map[string]any{"pass": false})
	annotate(itemB.ID, sourceB, map[string]any{"pass": true})

	// Item C: the string RECOVERS via universal legacy decoding (valid JSON
	// object) saying pass=false; an annotation claiming pass=true must not
	// override the original.
	itemC := newItem("original recovery outranks annotation", []string{"remedy-c"})
	recoveredPayloadC := map[string]any{"inner_kind": "checklist.item:remedy-c", "inner": `{"pass": false}`}
	appendRawWorkItemEvent(t, ctx, pool, writer, systemTok.Token, itemC.ID, recoveredPayloadC)
	annotate(itemC.ID, rawAppendedEventID(t, itemC.ID, recoveredPayloadC), map[string]any{"pass": true})

	// Item D: a string-encoded review VERDICT cannot be remediated — the
	// typed, attributed verdict seam is excluded, so the item stays running
	// and the unusable verdict remains durable evidence.
	itemD := newItem("verdict is never remediated", []string{"event:review.verdict_recorded"})
	verdictPayloadD := map[string]any{"inner_kind": workitems.ReviewVerdictInnerKind, "inner": `{"verdict": "accepted"`}
	appendRawWorkItemEvent(t, ctx, pool, writer, systemTok.Token, itemD.ID, verdictPayloadD)
	annotate(itemD.ID, rawAppendedEventID(t, itemD.ID, verdictPayloadD), map[string]any{"verdict": "accepted"})

	// Item E (REM-B1): an annotation appended BEFORE its source event is a
	// pre-seeded prediction, not a historical interpretation — deterministic
	// event ids make the future id computable, so the ordering fence is the
	// only thing standing between annotation and forgery-by-anticipation.
	// The pre-seeded annotation must produce no signal even after the source
	// arrives.
	itemE := newItem("pre-source annotation is void", []string{"remedy-e"})
	badPayloadE := map[string]any{"inner_kind": "checklist.item:remedy-e", "inner": `{"pass": tru`}
	annotate(itemE.ID, rawAppendedEventID(t, itemE.ID, badPayloadE), map[string]any{"pass": true})
	appendRawWorkItemEvent(t, ctx, pool, writer, systemTok.Token, itemE.ID, badPayloadE)

	// Item F: same pre-seeding, but a second annotation lands AFTER the
	// source: the pre-source copy stays void and the post-source one is the
	// first ACCEPTED interpretation, so the item converges.
	itemF := newItem("post-source annotation still speaks", []string{"remedy-f"})
	badPayloadF := map[string]any{"inner_kind": "checklist.item:remedy-f", "inner": `{"pass": als`}
	sourceF := rawAppendedEventID(t, itemF.ID, badPayloadF)
	annotate(itemF.ID, sourceF, map[string]any{"pass": false})
	appendRawWorkItemEvent(t, ctx, pool, writer, systemTok.Token, itemF.ID, badPayloadF)
	annotate(itemF.ID, sourceF, map[string]any{"pass": true})

	budgets := Budgets{ByState: map[domain.WorkItemState]time.Duration{domain.WorkItemCaptured: time.Hour}}
	now := time.Now()
	w, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time { return now })
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}
	if _, err := w.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce: %v", err)
	}

	assertState := func(item domain.WorkItem, want domain.WorkItemState, why string) {
		t.Helper()
		got, err := service.Get(ctx, item.ID)
		if err != nil {
			t.Fatalf("get %s: %v", item.Title, err)
		}
		if got.State != want {
			t.Errorf("%s: state = %s, want %s (%s)", item.Title, got.State, want, why)
		}
	}
	assertState(itemA, domain.WorkItemDone, "valid remediation must satisfy the unrecoverable signal")
	assertState(itemB, domain.WorkItemRunning, "the FIRST annotation (pass=false) must win deterministically")
	assertState(itemC, domain.WorkItemRunning, "the recovered original (pass=false) must outrank the annotation")
	assertState(itemD, domain.WorkItemRunning, "a review verdict must never be satisfied through remediation")
	assertState(itemE, domain.WorkItemRunning, "a pre-source annotation must never produce a signal (REM-B1)")
	assertState(itemF, domain.WorkItemDone, "the first POST-source annotation is the first accepted interpretation")
}

func TestPayloadRemediationMalformedAnnotationIsEvidenceIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationPool(t)
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	writer := app.NewEventWriter()
	authSvc := auth.NewService(pool, writer)
	root, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "malformed-remediation-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	systemTok, err := authSvc.CreateToken(ctx, auth.CreateTokenInput{Name: "malformed-remediation-worker", Source: domain.SourceSystem, Actor: &root.Token})
	if err != nil {
		t.Fatalf("create system token: %v", err)
	}
	service := workitems.NewService(pool, writer)
	item, err := service.Create(ctx, workitems.CreateInput{
		Title: "malformed annotation", State: domain.WorkItemRunning,
		SuggestedConvergenceChecks: []string{"never-satisfied"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      systemTok.Token,
	})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	// Missing `parsed`: must be ignored as an interpretation AND leave
	// exactly one durable evidence row, idempotent across passes.
	if err := service.AppendEvent(ctx, item.ID, workitems.PayloadShapeRemediatedInnerKind, map[string]any{
		"source_event_id": uuid.New().String(),
	}, systemTok.Token); err != nil {
		t.Fatalf("append malformed annotation: %v", err)
	}

	budgets := Budgets{ByState: map[domain.WorkItemState]time.Duration{domain.WorkItemCaptured: time.Hour}}
	now := time.Now()
	w, err := New(pool, writer, budgets, &systemTok.Token.ID, func() time.Time { return now })
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}
	for pass := 1; pass <= 2; pass++ {
		if _, err := w.ScanOnce(ctx); err != nil {
			t.Fatalf("ScanOnce pass %d: %v", pass, err)
		}
		if got := countEventsByKind(t, ctx, pool, domain.EventDeterministicErrorReported); got != 1 {
			t.Fatalf("pass %d: deterministic evidence rows = %d, want exactly 1", pass, got)
		}
	}
}
