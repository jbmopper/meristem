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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
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
	if !EligibleDemand(reg.Policy, env) {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: demand %s", ErrDemandNotEligible, in.DemandEventID)
	}

	// The actor's OWN authority over the item — the same reducer that
	// filtered the candidate listing — is revalidated at claim time. A
	// listener registration never grants authority (design: "it grants no
	// authority: authorization stays with the bound token's own scopes").
	if err := s.access.CanWriteWorkItem(ctx, in.Actor, env.WorkItemID); err != nil {
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

	binding := workitems.ClaimBinding{
		ListenerID:    listenerID,
		DemandEventID: in.DemandEventID,
		PolicyEventID: *reg.PolicyEventID,
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
