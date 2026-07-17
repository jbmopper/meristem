package workitems

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
)

// The Projector contract: applying the EXACT same event again is a no-op
// (replay/rebuild), while a DIFFERENT event claiming the same lifecycle key
// fails loudly (round-1 finding: the review_launch projectors violated
// both halves).
func TestReviewLaunchProjectorsReplayIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newProvisionStack(t, ctx, 3600)
	item, job := s.admitReviewChild(t, ctx, "projector replay child", "rep1111")
	res := s.provision(t, ctx, item, job, 4)
	if err := s.svc.RecordReviewLaunchHandle(ctx, ReviewLaunchHandleInput{
		WorkItemID: item.ID, RoundSeq: res.RoundSeq, Attempt: job.Attempts,
		AssignmentEventID: res.Assignment.AssignmentEventID,
		Pid:               8181, Pgid: 8181, StartToken: "starttime:8181",
	}, s.issuer); err != nil {
		t.Fatalf("handle: %v", err)
	}

	loadEvent := func(kind string) domain.Event {
		t.Helper()
		var ev domain.Event
		var payload []byte
		var actor *uuid.UUID
		if err := s.pool.QueryRow(ctx, `
			SELECT id, subject_kind, subject_id, kind, payload, occurred_at, actor_token_id, source
			FROM events
			WHERE subject_id = $1 AND kind = $2
			ORDER BY seq DESC LIMIT 1
		`, item.ID, kind).Scan(&ev.ID, &ev.SubjectKind, &ev.SubjectID, &ev.Kind, &payload, &ev.OccurredAt, &actor, &ev.Source); err != nil {
			t.Fatalf("load %s event: %v", kind, err)
		}
		ev.ActorTokenID = actor
		ev.Payload = json.RawMessage(payload)
		return ev
	}
	// Exact replay of each event through its projector is a no-op.
	reserved := loadEvent(domain.EventReviewLaunchReserved)
	handled := loadEvent(domain.EventReviewLaunchHandleRecorded)
	for name, run := range map[string]func() error{
		"reserved replay": func() error {
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if err := (reviewLaunchReservedProjector{}).Apply(ctx, tx, reserved); err != nil {
				return err
			}
			return tx.Commit(ctx)
		},
		"handle replay": func() error {
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if err := (reviewLaunchHandleProjector{}).Apply(ctx, tx, handled); err != nil {
				return err
			}
			return tx.Commit(ctx)
		},
	} {
		if err := run(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}

	// A DIFFERENT event claiming the same lifecycle key fails loudly.
	conflicting := reserved
	conflicting.ID = uuid.New()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := (reviewLaunchReservedProjector{}).Apply(ctx, tx, conflicting); err == nil {
		t.Fatal("distinct reservation event on an existing key must fail")
	}
	_ = tx.Rollback(ctx)

	// Malformed payload versions fail closed.
	badVersion := reserved
	badVersion.ID = uuid.New()
	var decoded map[string]any
	if err := json.Unmarshal(reserved.Payload.(json.RawMessage), &decoded); err != nil {
		t.Fatal(err)
	}
	decoded["payload_version"] = 99
	reEncoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	badVersion.Payload = json.RawMessage(reEncoded)
	tx, err = s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := (reviewLaunchReservedProjector{}).Apply(ctx, tx, badVersion); err == nil {
		t.Fatal("unsupported payload_version must fail")
	}
	_ = tx.Rollback(ctx)

	// Resolved replay: finish the launch, then re-apply its exact event.
	if err := s.svc.ResolveReviewLaunch(ctx, s.auth, ResolveReviewLaunchInput{
		WorkItemID: item.ID, RoundSeq: res.RoundSeq, Attempt: job.Attempts,
		Outcome: ReviewLaunchSucceeded,
	}, s.issuer); err != nil {
		t.Fatalf("succeed: %v", err)
	}
	resolved := loadEvent(domain.EventReviewLaunchResolved)
	tx, err = s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := (reviewLaunchResolvedProjector{}).Apply(ctx, tx, resolved); err != nil {
		t.Fatalf("resolved replay: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var state string
	var updatedEvent uuid.UUID
	if err := s.pool.QueryRow(ctx, `
		SELECT state, updated_event_id FROM review_launch
		WHERE work_item_id = $1 AND round_seq = $2 AND attempt = $3
	`, item.ID, res.RoundSeq, job.Attempts).Scan(&state, &updatedEvent); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if state != "succeeded" || updatedEvent != resolved.ID {
		t.Fatalf("row after replay = %s/%s, want succeeded/%s", state, updatedEvent, resolved.ID)
	}
}
