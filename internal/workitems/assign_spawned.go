package workitems

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
)

// ErrSpawnAssigneeInvalid reports a spawn assignment naming an assignee token
// that does not exist, is revoked, is root, or is not an agent identity.
var ErrSpawnAssigneeInvalid = errors.New("workitems: spawn assignee must be a live non-root agent token")

// AssignSpawned atomically binds a freshly provisioned reviewer identity to a
// work item at spawn time — mode=spawn, "born assigned" (ee916614 slice 2).
//
// Claim is the assignee's own act; AssignSpawned is the spawner's. A
// dedicated non-root system executor appends the assignment on the assignee's
// behalf BEFORE the reviewing mind starts, so an admitted review child is
// never observable unbound and the volunteer race cannot begin. Everything
// else mirrors Claim exactly: the work_items → assignment-placeholder lock
// order, the cultivar-xylem lease resolution, incumbent expiry released in
// the same transaction, an idempotent same-assignee success, and a typed
// ClaimHeldError for a live different holder — no takeover path exists.
//
// The assignee must be a live non-root agent token: the spawner binds the
// scoped per-session identity it just provisioned (3818efed), never itself
// and never a human.
func (s *Service) AssignSpawned(ctx context.Context, id uuid.UUID, assigneeTokenID uuid.UUID, actor domain.Token) (domain.WorkItemAssignment, error) {
	if actor.ID == uuid.Nil || actor.Source != domain.SourceSystem || actor.IsRoot {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: spawn assignment requires a dedicated non-root system actor", ErrInvalidRequest)
	}
	if assigneeTokenID == uuid.Nil {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: assignee token id is required", ErrInvalidRequest)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	item, err := scanWorkItemForUpdate(ctx, tx, id)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if err := claimableWorkItem(item); err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if err := validateSpawnAssignee(ctx, tx, assigneeTokenID); err != nil {
		return domain.WorkItemAssignment{}, err
	}
	state, err := scanAssignmentStateForUpdate(ctx, tx, id)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	observedAt, err := readAssignmentClock(ctx, tx)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}

	if state.Assignment != nil && state.Assignment.ExpiresAt.After(observedAt) {
		if state.Assignment.HolderTokenID == assigneeTokenID && state.Assignment.Mode == domain.WorkItemAssignmentSpawn {
			if err := tx.Commit(ctx); err != nil {
				return domain.WorkItemAssignment{}, err
			}
			return *state.Assignment, nil
		}
		return domain.WorkItemAssignment{}, &ClaimHeldError{
			HolderTokenID:     state.Assignment.HolderTokenID,
			AssignmentEventID: state.Assignment.AssignmentEventID,
			ExpiresAt:         state.Assignment.ExpiresAt,
		}
	}
	if state.Assignment != nil {
		if _, err := s.appendAssignmentReleaseInTx(ctx, tx, *state.Assignment, domain.AssignmentReleaseExpired, "", observedAt, actor); err != nil {
			return domain.WorkItemAssignment{}, err
		}
		state, err = scanAssignmentState(ctx, tx, id, false)
		if err != nil {
			return domain.WorkItemAssignment{}, err
		}
	}

	lease, leaseSource, err := s.resolveClaimLease(ctx, tx, id)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	// Same two-observation clock discipline as Claim: incumbent expiry was
	// decided on the first read; lease birth is observed immediately before
	// the append so a lock wait cannot pre-consume the new lease.
	claimedAt, err := readAssignmentClock(ctx, tx)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	assignment, budgetErr, err := s.appendAssignmentInTx(ctx, tx, item, actor, assigneeTokenID, domain.WorkItemAssignmentSpawn, lease, leaseSource, claimedAt, state.StateEventID)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if budgetErr != nil {
		if err := tx.Commit(ctx); err != nil {
			return domain.WorkItemAssignment{}, err
		}
		return domain.WorkItemAssignment{}, budgetErr
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItemAssignment{}, err
	}
	return assignment, nil
}

// ErrVerdictNotFromBoundReviewer refuses a review verdict appended by anyone
// other than the item's currently bound reviewer. Release the binding (yield,
// expiry, or terminal transition) and rebind before another identity may
// record a verdict.
var ErrVerdictNotFromBoundReviewer = errors.New("workitems: a bound reviewer holds this item; only its verdict is accepted while the binding is active")

// ErrVerdictStaleGeneration refuses a verdict whose assignment_event_id names
// a superseded or expired binding generation: an old reviewer process must
// not land a verdict after lease expiry, transfer, or reassignment.
var ErrVerdictStaleGeneration = errors.New("workitems: review verdict names a stale assignment generation")

// ErrVerdictGenerationRequired refuses a verdict that names no binding
// generation while the item is bound: a bound reviewer must cite the exact
// work_item.assigned event that authorizes it.
var ErrVerdictGenerationRequired = errors.New("workitems: review verdict must name its assignment_event_id while a binding is active")

// ErrVerdictBindingRequired refuses verdicts on an item whose binding was
// released and not rebound: an item that uses bindings has no verdict
// authority in the gap, so a stale process cannot slip through it.
var ErrVerdictBindingRequired = errors.New("workitems: this item's reviewer binding was released; rebind before recording a verdict")

// requireVerdictAuthority is the verdict-authority gate (ee916614 slice 2),
// enforced here in the one canonical domain operation REST, MCP, and worker
// paths share. Authority is fenced to the binding GENERATION, not just the
// current holder:
//
//   - active binding: only the holder may record a verdict, and it must cite
//     the exact current assignment_event_id — a process outliving a
//     same-holder rebind still fences out on the generation.
//   - released or expired, not rebound: no verdict authority exists in the
//     gap; fail closed until a rebind.
//   - never bound (or the pre-assignment placeholder is absent entirely):
//     legacy latest-verdict-wins is unchanged, and a generation claim on an
//     unbound item is refused as stale rather than ignored.
//
// The convergence reducer stays untouched and replay-pure — history that
// predates bindings reduces exactly as before — because this live gate
// guarantees no unauthorized verdict enters the log while bindings are in
// use. The assignment row is read under lock so a concurrent release or
// claim serializes against the verdict.
func (s *Service) requireVerdictAuthority(ctx context.Context, tx pgx.Tx, id uuid.UUID, actor domain.Token, claimedGeneration uuid.UUID) error {
	state, err := scanAssignmentStateForUpdate(ctx, tx, id)
	if err != nil {
		if errors.Is(err, ErrAssignmentStateMissing) {
			return nil
		}
		return err
	}

	if state.Assignment != nil {
		now, err := readAssignmentClock(ctx, tx)
		if err != nil {
			return err
		}
		if state.Assignment.ExpiresAt.After(now) {
			if state.Assignment.HolderTokenID != actor.ID {
				return fmt.Errorf("%w: holder=%s actor=%s expires_at=%s", ErrVerdictNotFromBoundReviewer, state.Assignment.HolderTokenID, actor.ID, state.Assignment.ExpiresAt.Format(time.RFC3339))
			}
			if claimedGeneration == uuid.Nil {
				return fmt.Errorf("%w: current generation is %s", ErrVerdictGenerationRequired, state.Assignment.AssignmentEventID)
			}
			if claimedGeneration != state.Assignment.AssignmentEventID {
				return fmt.Errorf("%w: named %s, current is %s", ErrVerdictStaleGeneration, claimedGeneration, state.Assignment.AssignmentEventID)
			}
			return nil
		}
		// The recorded binding has expired and nothing rebound: authority
		// ended with the lease.
		return fmt.Errorf("%w: binding %s expired at %s", ErrVerdictBindingRequired, state.Assignment.AssignmentEventID, state.Assignment.ExpiresAt.Format(time.RFC3339))
	}

	if state.LastReleaseReason != nil {
		return fmt.Errorf("%w: last release reason %q", ErrVerdictBindingRequired, *state.LastReleaseReason)
	}
	// Never bound: legacy latest-verdict-wins — but a generation claim that
	// cannot possibly be current is refused, never ignored.
	if claimedGeneration != uuid.Nil {
		return fmt.Errorf("%w: named %s on an item with no binding", ErrVerdictStaleGeneration, claimedGeneration)
	}
	return nil
}

// validateSpawnAssignee requires the assignee to be a live non-root agent
// token, locked for the transaction so a concurrent revocation cannot race
// the binding.
func validateSpawnAssignee(ctx context.Context, tx pgx.Tx, assigneeTokenID uuid.UUID) error {
	var (
		isRoot    bool
		source    string
		revokedAt *time.Time
	)
	err := tx.QueryRow(ctx, `
		SELECT is_root, source, revoked_at FROM tokens WHERE id = $1 FOR UPDATE
	`, assigneeTokenID).Scan(&isRoot, &source, &revokedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: token %s not found", ErrSpawnAssigneeInvalid, assigneeTokenID)
		}
		return fmt.Errorf("workitems: read spawn assignee token: %w", err)
	}
	if isRoot || revokedAt != nil || source != string(domain.SourceAgent) {
		return fmt.Errorf("%w: token %s (root=%t revoked=%t source=%s)", ErrSpawnAssigneeInvalid, assigneeTokenID, isRoot, revokedAt != nil, source)
	}
	return nil
}
