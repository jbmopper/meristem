package workitems

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

func TestAssignmentMigrationBackfillsNonterminalItemFromCreatedEvent(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewPool(t, "assignment_migration_nonterminal")
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("initial migrate: %v", err)
	}

	authRegistry := projections.NewRegistry()
	auth.RegisterProjectors(authRegistry)
	authWriter := events.NewWriter(authRegistry)
	authService := auth.NewService(pool, authWriter)
	rootResult, err := authService.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-migration-root", IsRoot: true, Source: domain.SourceHuman})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	actorResult, err := authService.CreateToken(ctx, auth.CreateTokenInput{Name: "assignment-migration-actor", Source: domain.SourceAgent, Actor: &rootResult.Token})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}
	if err := storage.MigrateDown(ctx, pool, nil); err != nil {
		t.Fatalf("roll back assignment migration: %v", err)
	}

	legacyRegistry := projections.NewRegistry()
	legacyRegistry.Register(preAssignmentCreatedProjector{})
	legacyRegistry.Register(transitionedProjector{})
	writer := events.NewWriter(legacyRegistry)
	itemID := uuid.New()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	createdID, _, err := writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: itemID,
		Kind: domain.EventWorkItemCreated, Source: actorResult.Token.Source, ActorTokenID: &actorResult.Token.ID,
		Payload: map[string]any{
			"title": "pre-assignment nonterminal", "state": domain.WorkItemCaptured,
			"suggested_convergence_checks": []string{"migration remains replay-honest"},
			"human_review_status":          domain.HumanReviewWavedThrough,
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append created: %v", err)
	}
	var latestTransitionID uuid.UUID
	for i, transition := range []struct {
		from domain.WorkItemState
		to   domain.WorkItemState
	}{{domain.WorkItemCaptured, domain.WorkItemTriaged}, {domain.WorkItemTriaged, domain.WorkItemPlanned}} {
		latestTransitionID, _, err = writer.Append(ctx, tx, events.Spec{
			SubjectKind: domain.SubjectWorkItem, SubjectID: itemID,
			Kind: domain.EventWorkItemTransitioned, Source: actorResult.Token.Source, ActorTokenID: &actorResult.Token.ID,
			Discriminator: "pre-assignment-transition-" + string(rune('0'+i)),
			Payload:       map[string]any{"from": transition.from, "to": transition.to, "reason": "legacy fixture"},
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("append transition %d: %v", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("apply assignment migration: %v", err)
	}

	var stateEventID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT state_event_id FROM work_item_assignment_state WHERE work_item_id=$1`, itemID).Scan(&stateEventID); err != nil {
		t.Fatalf("read backfilled assignment state: %v", err)
	}
	if stateEventID != createdID {
		t.Fatalf("nonterminal state_event_id = %s, want created event %s (latest transition was %s)", stateEventID, createdID, latestTransitionID)
	}
}

// preAssignmentCreatedProjector models the work_item.created projection just
// before migration 0035 introduced the permanent assignment placeholder.
type preAssignmentCreatedProjector struct{}

func (preAssignmentCreatedProjector) Kind() string { return domain.EventWorkItemCreated }

func (preAssignmentCreatedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		Title                      string                   `json:"title"`
		Body                       string                   `json:"body"`
		State                      domain.WorkItemState     `json:"state"`
		SuggestedConvergenceChecks []string                 `json:"suggested_convergence_checks"`
		HumanReviewStatus          domain.HumanReviewStatus `json:"human_review_status"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	checksJSON, err := marshalSuggestedConvergenceChecks(payload.SuggestedConvergenceChecks)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO work_items (
			id, title, body, state, suggested_convergence_checks,
			human_review_status, created_by, created_at, state_entered_at, updated_at
		) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $8, $8)
	`, event.SubjectID, payload.Title, payload.Body, payload.State, checksJSON,
		payload.HumanReviewStatus, event.ActorTokenID, event.OccurredAt)
	return err
}
