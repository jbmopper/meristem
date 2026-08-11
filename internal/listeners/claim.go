package listeners

// The listener-bound claim (LCP3-R1-B1): ONE canonical domain operation, ONE
// transaction. The registration row is locked FIRST, so everything the
// generic work-item claim cannot know — retirement, the current credential
// binding, the policy revision in force, demand eligibility under that
// revision, the actor's own claim authority, and listener capacity — is
// revalidated against durable state that cannot change under the claim. Two
// supervisors for one listener serialize on the registration lock, so
// max_concurrent_assignments is enforced even across different work items.
// The resulting assignment event and projection carry the listener, demand
// event, and policy revision, making restart and completion generation-bound
// to the LISTENER rather than just the token.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/jobqueue"
	"github.com/jbmopper/meristem/internal/workitems"
)

var (
	// ErrDemandNotEligible refuses a claim whose demand no longer matches the
	// listener's CURRENT policy (or was never eligible demand). Pure conflict.
	ErrDemandNotEligible = errors.New("listeners: demand is not eligible under the current policy")
	// ErrListenerAtCapacity refuses a claim that would exceed the listener's
	// max_concurrent_assignments. Pure conflict.
	ErrListenerAtCapacity = errors.New("listeners: listener is at max concurrent assignments")
)

// ClaimDemandInput names the demand and the caller's observed policy
// revision. The revision fence makes a stale supervisor's claim a PURE
// conflict: if administration replaced the policy after the snapshot, the
// old lens cannot admit new work.
type ClaimDemandInput struct {
	DemandEventID         uuid.UUID
	ObservedPolicyEventID *uuid.UUID
	Actor                 domain.Token
}

// ClaimDemand is the supervisor's ONLY claim path. Selection may be
// optimistic; this operation is not.
func (s *Service) ClaimDemand(ctx context.Context, listenerID uuid.UUID, in ClaimDemandInput) (domain.WorkItemAssignment, error) {
	if in.Actor.ID == uuid.Nil {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if in.DemandEventID == uuid.Nil {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: demand_event_id is required", ErrInvalidRequest)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Registration lock: retirement, rebinding, policy revision, and
	// capacity are all judged under this lock — a concurrent admin action or
	// sibling supervisor serializes here.
	reg, err := scanRegistrationForUpdate(ctx, tx, listenerID)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if reg.RetiredAt != nil {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: %s", ErrRetired, listenerID)
	}
	if reg.PrincipalTokenID != in.Actor.ID {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: claim requires the listener's currently bound principal", ErrNotAuthorized)
	}
	if reg.Policy == nil || reg.PolicyEventID == nil {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: listener has no established base policy", ErrNotAuthorized)
	}
	if in.ObservedPolicyEventID == nil || *in.ObservedPolicyEventID != *reg.PolicyEventID {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: observed=%v current=%v", ErrStalePolicy, in.ObservedPolicyEventID, reg.PolicyEventID)
	}

	// Demand must STILL be eligible under the revision just locked.
	env, err := s.demandEnvelope(ctx, tx, in.DemandEventID)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	binding := workitems.ClaimBinding{
		ListenerID:    listenerID,
		DemandEventID: in.DemandEventID,
		PolicyEventID: *reg.PolicyEventID,
	}
	_, exactRetry, err := s.workItems.ExistingExactClaimInTx(ctx, tx, env.WorkItemID, in.Actor, binding)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if !exactRetry {
		if err := validateCurrentDemand(ctx, tx, in.DemandEventID, env.WorkItemID); err != nil {
			return domain.WorkItemAssignment{}, err
		}
	}
	if !EligibleDemand(reg.Policy, env) {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: demand %s", ErrDemandNotEligible, in.DemandEventID)
	}

	// The actor's OWN authority over the item — the same reducer that
	// filtered the candidate listing — is revalidated at claim time and
	// FENCED INSIDE this transaction (LCP3-R2-B1): the tree-membership read
	// runs through the open tx, never a nested pool acquire, so the decision
	// shares the claim's snapshot and a saturated pool cannot deadlock. A
	// listener registration never grants authority (design: "it grants no
	// authority: authorization stays with the bound token's own scopes").
	if err := s.access.CanWriteWorkItemIn(ctx, tx, in.Actor, env.WorkItemID); err != nil {
		if errors.Is(err, access.ErrDenied) {
			return domain.WorkItemAssignment{}, fmt.Errorf("%w: actor lacks claim authority over work item %s", ErrNotAuthorized, env.WorkItemID)
		}
		return domain.WorkItemAssignment{}, err
	}

	// Capacity under the registration lock. An expired-but-unswept lease
	// still counts: it is released by the claim reduction or the worker, not
	// by wall-clock passage here.
	var active int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM work_item_assignment_state
		WHERE listener_id = $1 AND holder_token_id IS NOT NULL AND work_item_id <> $2`,
		listenerID, env.WorkItemID).Scan(&active); err != nil {
		return domain.WorkItemAssignment{}, fmt.Errorf("listeners: count active assignments: %w", err)
	}
	if active >= reg.Policy.MaxConcurrentAssignments {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: %d active of %d", ErrListenerAtCapacity, active, reg.Policy.MaxConcurrentAssignments)
	}
	if exactRetry {
		// The first exact-binding check is intentionally before current-demand
		// validation so a retry can return ownership after the assignee advances
		// lifecycle state. Recheck its wall-clock lease immediately before the
		// final reduction: if it expired while policy/authority/capacity were
		// evaluated, it is no longer an idempotent return. Validate the demand
		// before ClaimInTx is allowed to release and mint replacement ownership.
		existing, stillExact, err := s.workItems.ExistingExactClaimInTx(ctx, tx, env.WorkItemID, in.Actor, binding)
		if err != nil {
			return domain.WorkItemAssignment{}, err
		}
		if stillExact {
			if err := tx.Commit(ctx); err != nil {
				return domain.WorkItemAssignment{}, err
			}
			return existing, nil
		}
		if err := validateCurrentDemand(ctx, tx, in.DemandEventID, env.WorkItemID); err != nil {
			return domain.WorkItemAssignment{}, err
		}
	}

	assignment, budgetErr, err := s.workItems.ClaimInTx(ctx, tx, env.WorkItemID, in.Actor, &binding)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if budgetErr != nil {
		return domain.WorkItemAssignment{}, budgetErr
	}
	return assignment, nil
}

// validateCurrentDemand closes the optimistic-selection race at the claim
// boundary. A supervisor may have selected a demand before a lifecycle
// transition or before a newer dispatch payload generation was appended; it
// must not turn that stale immutable fact into a fresh assignment.
//
// The work-item row is locked so lifecycle state and state_entered_at cannot
// change between this reduction and ClaimInTx's assignment append. events.seq
// is compared only within this node's event log: a work item's authoritative
// events are appended by its home node, and seq is deliberately not a fleet-
// global ordering primitive.
func validateCurrentDemand(ctx context.Context, tx pgx.Tx, demandEventID, workItemID uuid.UUID) error {
	var (
		currentState     string
		currentEnteredAt time.Time
	)
	err := tx.QueryRow(ctx, `
		SELECT state, state_entered_at
		FROM work_items
		WHERE id = $1
		FOR UPDATE`, workItemID).Scan(&currentState, &currentEnteredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: demand %s names missing work item %s", ErrDemandNotEligible, demandEventID, workItemID)
	}
	if err != nil {
		return fmt.Errorf("listeners: lock demand work item: %w", err)
	}

	identity, err := jobqueue.ResolveDispatchIdentity(ctx, tx, demandEventID)
	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, jobqueue.ErrInvalidDispatchDemand) {
		return fmt.Errorf("%w: demand %s has invalid state-entry identity", ErrInvalidRequest, demandEventID)
	}
	if err != nil {
		return fmt.Errorf("listeners: resolve demand identity: %w", err)
	}
	if identity.WorkItemID != workItemID {
		return fmt.Errorf("%w: demand %s names a different work item", ErrInvalidRequest, demandEventID)
	}
	currentEntry, err := jobqueue.ResolveCurrentStateEntry(ctx, tx, workItemID)
	if err != nil {
		return fmt.Errorf("listeners: resolve current state entry: %w", err)
	}
	if string(currentEntry.State) != currentState || !currentEntry.OccurredAt.Equal(currentEnteredAt) {
		return fmt.Errorf("%w: work item %s lifecycle projection disagrees with its event log", ErrDemandNotEligible, workItemID)
	}
	latest, err := jobqueue.LatestValidDispatch(ctx, tx, workItemID)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: work item %s has no current dispatch demand", ErrDemandNotEligible, workItemID)
	}
	if errors.Is(err, jobqueue.ErrInvalidDispatchDemand) {
		return fmt.Errorf("%w: work item %s has a malformed latest dispatch demand", ErrDemandNotEligible, workItemID)
	}
	if err != nil {
		return fmt.Errorf("listeners: resolve latest demand: %w", err)
	}
	if latest.ID != demandEventID {
		return fmt.Errorf("%w: demand %s was superseded by %s", ErrDemandNotEligible, demandEventID, latest.ID)
	}
	if identity.StateEntryID != currentEntry.ID && !jobqueue.CausallyAdmitsDemand(currentEntry, demandEventID) {
		return fmt.Errorf("%w: demand %s belongs to state entry %s; current entry is %s", ErrDemandNotEligible, demandEventID, identity.StateEntryID, currentEntry.ID)
	}
	return nil
}
