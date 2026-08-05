package workitems

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/auth"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/projections"
	"github.com/jbmopper/meristem/internal/storage"
	"github.com/jbmopper/meristem/internal/testutil/pgtest"
)

// migrateDownTo rolls migrations back until version is the newest applied one.
//
// These tests reconstruct the pre-0035 and pre-0036 database states an older
// binary would present at upgrade time. That is a property of *which*
// migrations are applied, not of how many MigrateDown calls it takes to get
// there: counting rollbacks silently retargets the moment any later migration
// lands, and the guarded-upgrade assertions then pass or fail against the
// wrong file. Naming the version keeps the fixture pinned to its intent.
func migrateDownTo(t *testing.T, ctx context.Context, pool *pgxpool.Pool, version int64) {
	t.Helper()
	for {
		var head int64
		if err := pool.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&head); err != nil {
			t.Fatalf("read schema_migrations head: %v", err)
		}
		if head == version {
			return
		}
		if head < version {
			t.Fatalf("schema_migrations head is %d, already below target %d", head, version)
		}
		if err := storage.MigrateDown(ctx, pool, nil); err != nil {
			t.Fatalf("roll back migration %d: %v", head, err)
		}
	}
}

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
	migrateDownTo(t, ctx, pool, 34)

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

func TestTerminalAddresseeMigrationBackfillsActiveEpochOnly(t *testing.T) {
	ctx := context.Background()
	pool, writer, _, holder, closer := newAssignmentTestStack(t, ctx)
	svc := NewService(pool, writer)

	activeAtTerminal := createClaimableItem(t, ctx, svc, holder, "terminal addressee migration active")
	if _, err := svc.Claim(ctx, activeAtTerminal.ID, holder); err != nil {
		t.Fatalf("claim active terminal fixture: %v", err)
	}
	if _, err := svc.Transition(ctx, activeAtTerminal.ID, domain.WorkItemDone, "pre-0036 active terminal", closer); err != nil {
		t.Fatalf("terminalize active fixture: %v", err)
	}
	activeEntry := loadLatestAssignmentEvent(t, ctx, pool, activeAtTerminal.ID, domain.EventWorkItemTransitioned)

	releasedBeforeTerminal := createClaimableItem(t, ctx, svc, holder, "terminal addressee migration released")
	if _, err := svc.Claim(ctx, releasedBeforeTerminal.ID, holder); err != nil {
		t.Fatalf("claim released fixture: %v", err)
	}
	if _, err := svc.Yield(ctx, releasedBeforeTerminal.ID, holder); err != nil {
		t.Fatalf("yield released fixture: %v", err)
	}
	if _, err := svc.Transition(ctx, releasedBeforeTerminal.ID, domain.WorkItemDone, "pre-0036 released terminal", closer); err != nil {
		t.Fatalf("terminalize released fixture: %v", err)
	}

	terminalAtCreate, err := svc.Create(ctx, CreateInput{
		Title: "terminal addressee migration created terminal", State: domain.WorkItemDone,
		SuggestedConvergenceChecks: []string{"created terminal stays unaddressed"},
		HumanReviewStatus:          domain.HumanReviewWavedThrough,
		Actor:                      closer,
	})
	if err != nil {
		t.Fatalf("create terminal fixture: %v", err)
	}
	createdEntry := loadLatestAssignmentEvent(t, ctx, pool, terminalAtCreate.ID, domain.EventWorkItemCreated)
	if _, err := svc.Transition(ctx, terminalAtCreate.ID, domain.WorkItemDone, "pre-0036 created terminal no-op", closer); err != nil {
		t.Fatalf("append created-terminal no-op: %v", err)
	}
	createdNoop := loadLatestAssignmentEvent(t, ctx, pool, terminalAtCreate.ID, domain.EventWorkItemTransitioned)

	legacyMissingFrom := createClaimableItem(t, ctx, svc, holder, "terminal addressee migration legacy missing from")
	if _, err := svc.Claim(ctx, legacyMissingFrom.ID, holder); err != nil {
		t.Fatalf("claim legacy missing-from fixture: %v", err)
	}

	// Dropping 0036 erases only the new projection column. The events and
	// 0035 lifecycle rows left behind are exactly the state an older binary
	// would present at guarded upgrade time.
	migrateDownTo(t, ctx, pool, 35)
	// Legacy producers omitted payload.from. Append both a terminal entry and a
	// terminal same-state no-op in that historical shape, then fold the exact
	// 0035 pointers in the same transaction. The no-op must not become a second
	// terminal entry during the 0036 backfill.
	legacyWriter := events.NewWriter(projections.NewRegistry())
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	activeNoopID, _, err := legacyWriter.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: activeAtTerminal.ID,
		Kind: domain.EventWorkItemTransitioned, Source: closer.Source, ActorTokenID: &closer.ID,
		Discriminator: "legacy-terminal-noop-missing-from",
		Payload: map[string]any{
			"to": domain.WorkItemDone, "reason": "legacy terminal no-op without from",
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append legacy missing-from terminal no-op: %v", err)
	}
	legacyMissingFromEventID, _, err := legacyWriter.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: legacyMissingFrom.ID,
		Kind: domain.EventWorkItemTransitioned, Source: closer.Source, ActorTokenID: &closer.ID,
		Discriminator: "legacy-terminal-missing-from",
		Payload: map[string]any{
			"to": domain.WorkItemDone, "reason": "legacy terminal payload without from",
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append legacy missing-from transition: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE work_items AS work_item
		SET state='done',
		    state_reason='legacy terminal no-op without from',
		    state_entered_at=event.occurred_at,
		    updated_at=event.occurred_at
		FROM events AS event
		WHERE work_item.id=$1 AND event.id=$2
	`, activeAtTerminal.ID, activeNoopID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("fold legacy missing-from terminal no-op work item: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE work_item_assignment_state AS assignment_state
		SET state_event_id=event.id,
		    state_event_seq=event.seq,
		    updated_at=event.occurred_at
		FROM events AS event
		WHERE assignment_state.work_item_id=$1 AND event.id=$2
	`, activeAtTerminal.ID, activeNoopID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("fold legacy missing-from terminal no-op assignment: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE work_items AS work_item
		SET state='done',
		    state_reason='legacy terminal payload without from',
		    state_entered_at=event.occurred_at,
		    updated_at=event.occurred_at
		FROM events AS event
		WHERE work_item.id=$1 AND event.id=$2
	`, legacyMissingFrom.ID, legacyMissingFromEventID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("fold legacy missing-from work item: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE work_item_assignment_state AS assignment_state
		SET holder_token_id=NULL,
		    mode=NULL,
		    assignment_event_id=NULL,
		    claimed_at=NULL,
		    expires_at=NULL,
		    last_release_reason='done',
		    terminal_state='done',
		    state_event_id=event.id,
		    state_event_seq=event.seq,
		    updated_at=event.occurred_at
		FROM events AS event
		WHERE assignment_state.work_item_id=$1 AND event.id=$2
	`, legacyMissingFrom.ID, legacyMissingFromEventID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("fold legacy missing-from assignment: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit legacy missing-from fixture: %v", err)
	}
	activeNoop := loadAssignmentEvent(t, ctx, pool, activeNoopID)
	if activeNoop.ID == activeEntry.ID {
		t.Fatal("active terminal no-op reused entering event identity")
	}
	// Reconstruct the exact 0035 projector outcome. Before this fix, every
	// terminal same-state transition advanced the assignment pointer even
	// though it was a lifecycle no-op. The 0036 up migration must repair T2
	// back to the entering-terminal event T1 (or the terminal created event).
	for _, legacy := range []struct {
		workItemID uuid.UUID
		event      domain.Event
	}{
		{workItemID: terminalAtCreate.ID, event: createdNoop},
	} {
		if _, err := pool.Exec(ctx, `
			UPDATE work_item_assignment_state
			SET state_event_id=$2, state_event_seq=$3, updated_at=$4
			WHERE work_item_id=$1
		`, legacy.workItemID, legacy.event.ID, legacy.event.Seq, legacy.event.OccurredAt); err != nil {
			t.Fatalf("construct 0035 terminal pointer for %s: %v", legacy.workItemID, err)
		}
	}
	for _, legacy := range []struct {
		workItemID uuid.UUID
		wantEvent  uuid.UUID
	}{
		{workItemID: activeAtTerminal.ID, wantEvent: activeNoop.ID},
		{workItemID: terminalAtCreate.ID, wantEvent: createdNoop.ID},
	} {
		var got uuid.UUID
		if err := pool.QueryRow(ctx, `SELECT state_event_id FROM work_item_assignment_state WHERE work_item_id=$1`, legacy.workItemID).Scan(&got); err != nil {
			t.Fatalf("read constructed 0035 pointer for %s: %v", legacy.workItemID, err)
		}
		if got != legacy.wantEvent {
			t.Fatalf("constructed 0035 pointer for %s = %s, want no-op %s", legacy.workItemID, got, legacy.wantEvent)
		}
	}
	if err := storage.Migrate(ctx, pool, nil); err != nil {
		t.Fatalf("reapply terminal addressee migration: %v", err)
	}

	var activeAddressee uuid.UUID
	var activeStateEvent uuid.UUID
	var activeStateSeq int64
	var activeUpdatedAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT terminal_addressee_token_id, state_event_id,
		       state_event_seq, updated_at
		FROM work_item_assignment_state WHERE work_item_id=$1
	`, activeAtTerminal.ID).Scan(&activeAddressee, &activeStateEvent, &activeStateSeq, &activeUpdatedAt); err != nil {
		t.Fatalf("read active backfill: %v", err)
	}
	if activeAddressee != holder.ID || activeStateEvent != activeEntry.ID ||
		activeStateSeq != activeEntry.Seq || !activeUpdatedAt.Equal(activeEntry.OccurredAt) {
		t.Fatalf("active backfill = addressee %s event %s seq %d updated %s, want %s/%s/%d/%s", activeAddressee, activeStateEvent, activeStateSeq, activeUpdatedAt, holder.ID, activeEntry.ID, activeEntry.Seq, activeEntry.OccurredAt)
	}

	var legacyAddressee uuid.UUID
	var legacyStateEvent uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT terminal_addressee_token_id, state_event_id
		FROM work_item_assignment_state WHERE work_item_id=$1
	`, legacyMissingFrom.ID).Scan(&legacyAddressee, &legacyStateEvent); err != nil {
		t.Fatalf("read legacy missing-from backfill: %v", err)
	}
	if legacyAddressee != holder.ID || legacyStateEvent != legacyMissingFromEventID {
		t.Fatalf("legacy missing-from backfill = addressee %s event %s, want %s/%s", legacyAddressee, legacyStateEvent, holder.ID, legacyMissingFromEventID)
	}

	var releasedAddressee *uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT terminal_addressee_token_id
		FROM work_item_assignment_state WHERE work_item_id=$1
	`, releasedBeforeTerminal.ID).Scan(&releasedAddressee); err != nil {
		t.Fatalf("read released backfill: %v", err)
	}
	if releasedAddressee != nil {
		t.Fatalf("released backfill addressee = %s, want NULL", *releasedAddressee)
	}

	var createdAddressee *uuid.UUID
	var createdStateEvent uuid.UUID
	var createdStateSeq int64
	var createdUpdatedAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT terminal_addressee_token_id, state_event_id,
		       state_event_seq, updated_at
		FROM work_item_assignment_state WHERE work_item_id=$1
	`, terminalAtCreate.ID).Scan(&createdAddressee, &createdStateEvent, &createdStateSeq, &createdUpdatedAt); err != nil {
		t.Fatalf("read created-terminal backfill: %v", err)
	}
	if createdAddressee != nil || createdStateEvent != createdEntry.ID ||
		createdStateSeq != createdEntry.Seq || !createdUpdatedAt.Equal(createdEntry.OccurredAt) {
		t.Fatalf("created-terminal backfill = addressee %v event %s seq %d updated %s, want NULL/%s/%d/%s", createdAddressee, createdStateEvent, createdStateSeq, createdUpdatedAt, createdEntry.ID, createdEntry.Seq, createdEntry.OccurredAt)
	}
}

func TestTerminalAddresseeMigrationRejectsInvalidTerminalHistory(t *testing.T) {
	t.Run("terminal state change", func(t *testing.T) {
		ctx := context.Background()
		pool, writer, _, holder, closer := newAssignmentTestStack(t, ctx)
		svc := NewService(pool, writer)
		item := createClaimableItem(t, ctx, svc, holder, "ambiguous terminal history")
		if _, err := svc.Claim(ctx, item.ID, holder); err != nil {
			t.Fatalf("claim fixture: %v", err)
		}
		if _, err := svc.Transition(ctx, item.ID, domain.WorkItemDone, "first terminal entry", closer); err != nil {
			t.Fatalf("terminalize fixture: %v", err)
		}
		migrateDownTo(t, ctx, pool, 35)

		// Model corrupted pre-0036 history without running today's fail-closed
		// projector: a later event attempts to change one terminal into another.
		legacyWriter := events.NewWriter(projections.NewRegistry())
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		invalidTransitionID, _, err := legacyWriter.Append(ctx, tx, events.Spec{
			SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
			Kind: domain.EventWorkItemTransitioned, Source: closer.Source, ActorTokenID: &closer.ID,
			Discriminator: "invalid-terminal-state-change",
			Payload: map[string]any{
				"from": domain.WorkItemDone, "to": domain.WorkItemFailed,
				"reason": "invalid legacy history fixture",
			},
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("append invalid legacy history: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE work_items AS work_item
			SET state='failed',
			    state_reason='invalid legacy history fixture',
			    state_entered_at=event.occurred_at,
			    updated_at=event.occurred_at
			FROM events AS event
			WHERE work_item.id=$1 AND event.id=$2
		`, item.ID, invalidTransitionID); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("fold invalid terminal work item fixture: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE work_item_assignment_state AS assignment_state
			SET terminal_state='failed',
			    state_event_id=event.id,
			    state_event_seq=event.seq,
			    updated_at=event.occurred_at
			FROM events AS event
			WHERE assignment_state.work_item_id=$1 AND event.id=$2
		`, item.ID, invalidTransitionID); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("fold invalid terminal assignment fixture: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		err = storage.Migrate(ctx, pool, nil)
		if err == nil || !strings.Contains(err.Error(), "transition history mismatch") ||
			!strings.Contains(err.Error(), "prior_state=done") || !strings.Contains(err.Error(), "result_state=failed") {
			t.Fatalf("terminal state-change migration error = %v", err)
		}
		assertMigrationVersionAbsent(t, ctx, pool, 36)
	})

	t.Run("entry state disagrees with projection", func(t *testing.T) {
		ctx := context.Background()
		pool, writer, _, holder, closer := newAssignmentTestStack(t, ctx)
		svc := NewService(pool, writer)
		item := createClaimableItem(t, ctx, svc, holder, "terminal state mismatch")
		if _, err := svc.Transition(ctx, item.ID, domain.WorkItemDone, "terminal entry done", closer); err != nil {
			t.Fatalf("terminalize fixture: %v", err)
		}
		migrateDownTo(t, ctx, pool, 35)
		// Simulate pre-upgrade projection drift. The migration must abort rather
		// than silently bind a done event to a failed terminal sentinel.
		if _, err := pool.Exec(ctx, `
			UPDATE work_item_assignment_state
			SET terminal_state='failed'
			WHERE work_item_id=$1
		`, item.ID); err != nil {
			t.Fatalf("prepare mismatched projection fixture: %v", err)
		}
		err := storage.Migrate(ctx, pool, nil)
		if err == nil || !strings.Contains(err.Error(), "work_item_state=done") || !strings.Contains(err.Error(), "assignment_terminal_state=failed") {
			t.Fatalf("mismatched migration error = %v", err)
		}
		assertMigrationVersionAbsent(t, ctx, pool, 36)
	})

	t.Run("terminal work item has no terminal sentinel", func(t *testing.T) {
		ctx := context.Background()
		pool, writer, _, holder, closer := newAssignmentTestStack(t, ctx)
		svc := NewService(pool, writer)
		item := createClaimableItem(t, ctx, svc, holder, "missing terminal sentinel")
		if _, err := svc.Transition(ctx, item.ID, domain.WorkItemDone, "terminal entry done", closer); err != nil {
			t.Fatalf("terminalize fixture: %v", err)
		}
		migrateDownTo(t, ctx, pool, 35)
		// A terminal work_item with a nonterminal assignment sentinel cannot be
		// repaired safely by guessing. The guarded cutover must reject it before
		// deriving an address from history.
		if _, err := pool.Exec(ctx, `
			UPDATE work_item_assignment_state
			SET terminal_state=NULL,
			    last_release_reason=NULL
			WHERE work_item_id=$1
		`, item.ID); err != nil {
			t.Fatalf("prepare missing sentinel fixture: %v", err)
		}
		err := storage.Migrate(ctx, pool, nil)
		if err == nil || !strings.Contains(err.Error(), "work_item_state=done") || !strings.Contains(err.Error(), "assignment_terminal_state=<NULL>") {
			t.Fatalf("missing sentinel migration error = %v", err)
		}
		assertMigrationVersionAbsent(t, ctx, pool, 36)
	})

	t.Run("work item is missing assignment placeholder", func(t *testing.T) {
		ctx := context.Background()
		pool, writer, _, holder, _ := newAssignmentTestStack(t, ctx)
		item := createClaimableItem(t, ctx, NewService(pool, writer), holder, "missing assignment placeholder")
		migrateDownTo(t, ctx, pool, 35)
		if _, err := pool.Exec(ctx, `DELETE FROM work_item_assignment_state WHERE work_item_id=$1`, item.ID); err != nil {
			t.Fatalf("remove assignment placeholder fixture: %v", err)
		}
		err := storage.Migrate(ctx, pool, nil)
		if err == nil || !strings.Contains(err.Error(), "assignment placeholder missing") {
			t.Fatalf("missing placeholder migration error = %v", err)
		}
		assertMigrationVersionAbsent(t, ctx, pool, 36)
	})

	t.Run("nonterminal projection disagrees with latest lifecycle", func(t *testing.T) {
		ctx := context.Background()
		pool, writer, _, holder, _ := newAssignmentTestStack(t, ctx)
		item := createClaimableItem(t, ctx, NewService(pool, writer), holder, "nonterminal lifecycle drift")
		migrateDownTo(t, ctx, pool, 35)
		if _, err := pool.Exec(ctx, `UPDATE work_items SET state='planned' WHERE id=$1`, item.ID); err != nil {
			t.Fatalf("prepare nonterminal lifecycle drift fixture: %v", err)
		}
		err := storage.Migrate(ctx, pool, nil)
		if err == nil || !strings.Contains(err.Error(), "lifecycle projection mismatch") ||
			!strings.Contains(err.Error(), "work_item_state=planned") || !strings.Contains(err.Error(), "latest_result_state=running") {
			t.Fatalf("nonterminal lifecycle mismatch error = %v", err)
		}
		assertMigrationVersionAbsent(t, ctx, pool, 36)
	})

	t.Run("latest assigned control is invalid", func(t *testing.T) {
		for _, tc := range []struct {
			name          string
			assigneeValue string
			wantError     string
		}{
			{name: "blank", assigneeValue: "   ", wantError: "missing assignee_token_id"},
			{name: "invalid uuid", assigneeValue: "not-a-uuid", wantError: "invalid input syntax for type uuid"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := migrateMalformedPriorAssignedFixture(t, tc.assigneeValue)
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("malformed assigned history error = %v, want %q", err, tc.wantError)
				}
			})
		}
	})
}

func migrateMalformedPriorAssignedFixture(t *testing.T, assigneeValue string) error {
	t.Helper()
	ctx := context.Background()
	pool, writer, _, actor, _ := newAssignmentTestStack(t, ctx)
	item := createClaimableItem(t, ctx, NewService(pool, writer), actor, "malformed prior assigned control")
	migrateDownTo(t, ctx, pool, 35)

	legacyWriter := events.NewWriter(projections.NewRegistry())
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := legacyWriter.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventWorkItemAssigned, Source: actor.Source, ActorTokenID: &actor.ID,
		Discriminator: "malformed-prior-assigned-control",
		Payload: map[string]any{
			"payload_version": 1, "assignee_token_id": assigneeValue, "mode": domain.WorkItemAssignmentClaim,
		},
	}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append malformed assigned control: %v", err)
	}
	terminalEventID, _, err := legacyWriter.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectWorkItem, SubjectID: item.ID,
		Kind: domain.EventWorkItemTransitioned, Source: actor.Source, ActorTokenID: &actor.ID,
		Discriminator: "terminal-after-malformed-assigned-control",
		Payload: map[string]any{
			"from": domain.WorkItemRunning, "to": domain.WorkItemDone,
			"reason": "terminal after malformed assigned control",
		},
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append terminal transition fixture: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE work_items AS work_item
		SET state='done',
		    state_reason='terminal after malformed assigned control',
		    state_entered_at=event.occurred_at,
		    updated_at=event.occurred_at
		FROM events AS event
		WHERE work_item.id=$1 AND event.id=$2
	`, item.ID, terminalEventID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("fold terminal work item fixture: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE work_item_assignment_state AS assignment_state
		SET last_release_reason='done',
		    terminal_state='done',
		    state_event_id=event.id,
		    state_event_seq=event.seq,
		    updated_at=event.occurred_at
		FROM events AS event
		WHERE assignment_state.work_item_id=$1 AND event.id=$2
	`, item.ID, terminalEventID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("fold terminal assignment fixture: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit malformed assigned fixture: %v", err)
	}

	err = storage.Migrate(ctx, pool, nil)
	if err != nil {
		assertMigrationVersionAbsent(t, ctx, pool, 36)
	}
	return err
}

func assertMigrationVersionAbsent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, version int64) {
	t.Helper()
	var present bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&present); err != nil {
		t.Fatalf("read migration version %d: %v", version, err)
	}
	if present {
		t.Fatalf("failed migration %d was recorded as applied", version)
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
