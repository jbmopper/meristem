package workitems

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/safety"
)

var (
	ErrClaimUnavailable       = errors.New("workitems: claim unavailable")
	ErrClaimHeld              = errors.New("workitems: claim held by another token")
	ErrAssignmentNotFound     = errors.New("workitems: no active assignment")
	ErrAssignmentNotHeld      = errors.New("workitems: assignment is held by another token")
	ErrAssignmentStateMissing = errors.New("workitems: assignment-state projection missing")
	// ErrStaleAssignmentGeneration refuses a yield naming an assignment event
	// that is not the CURRENT generation. A delayed yield from a released
	// epoch must never close a newer lease — even one held by the same token
	// after reacquiring (review finding LCP-B1).
	ErrStaleAssignmentGeneration = errors.New("workitems: yield names a stale assignment generation")
)

// ClaimHeldError reports the current holder without granting a takeover. It
// unwraps to ErrClaimHeld so transports can map the conflict while retaining
// holder identity for a useful response.
type ClaimHeldError struct {
	HolderTokenID     uuid.UUID
	AssignmentEventID uuid.UUID
	ExpiresAt         time.Time
}

func (e *ClaimHeldError) Error() string {
	return fmt.Sprintf("%v: holder_token_id=%s assignment_event_id=%s expires_at=%s", ErrClaimHeld, e.HolderTokenID, e.AssignmentEventID, e.ExpiresAt.UTC().Format(time.RFC3339Nano))
}

func (e *ClaimHeldError) Unwrap() error { return ErrClaimHeld }

type assignmentState struct {
	Assignment    *domain.WorkItemAssignment
	StateEventID  uuid.UUID
	StateEventSeq int64
	UpdatedAt     time.Time
}

// ClaimBinding attributes a claim to the listener-bound operation that
// authorized it: the listener registration, the durable demand event, and
// the policy revision in force. The three travel together (all-or-none) and
// land on both the assignment event payload and the projection, so restart
// derivation is generation-bound to the listener rather than just the token.
type ClaimBinding struct {
	ListenerID    uuid.UUID
	DemandEventID uuid.UUID
	PolicyEventID uuid.UUID
}

func bindingField(binding *ClaimBinding, pick func(ClaimBinding) uuid.UUID) *uuid.UUID {
	if binding == nil {
		return nil
	}
	v := pick(*binding)
	if v == uuid.Nil {
		return nil
	}
	return &v
}

// Claim atomically checks and appends a mode=claim assignment. The global
// lock order is work_items first, then its permanent assignment-state row.
// That placeholder makes the empty state lockable, so two first claimers
// cannot both observe absence and append.
//
// An unexpired same-holder claim is an idempotent success. A different holder
// is returned as a typed conflict; no takeover path exists. An expired epoch
// is released as expired in the same transaction before the fresh claim.
func (s *Service) Claim(ctx context.Context, id uuid.UUID, actor domain.Token) (domain.WorkItemAssignment, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	assignment, budgetErr, err := s.ClaimInTx(ctx, tx, id, actor, nil)
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

// ClaimInTx is the full claim reduction inside the CALLER'S transaction —
// the composition seam for the listener-bound claim operation, which locks
// and revalidates the listener registration in the same transaction before
// delegating here. The caller owns commit/rollback; a non-nil budgetErr must
// still be committed (the exhaustion event is recorded) and then surfaced.
func (s *Service) ClaimInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, actor domain.Token, binding *ClaimBinding) (domain.WorkItemAssignment, error, error) {
	if actor.ID == uuid.Nil {
		return domain.WorkItemAssignment{}, nil, fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if actor.IsRoot {
		return domain.WorkItemAssignment{}, nil, fmt.Errorf("%w: root token is mint/revoke-only", ErrClaimUnavailable)
	}
	item, err := scanWorkItemForUpdate(ctx, tx, id)
	if err != nil {
		return domain.WorkItemAssignment{}, nil, err
	}
	if err := claimableWorkItem(item); err != nil {
		return domain.WorkItemAssignment{}, nil, err
	}
	state, err := scanAssignmentStateForUpdate(ctx, tx, id)
	if err != nil {
		return domain.WorkItemAssignment{}, nil, err
	}
	observedAt, err := readAssignmentClock(ctx, tx)
	if err != nil {
		return domain.WorkItemAssignment{}, nil, err
	}

	if state.Assignment != nil && state.Assignment.ExpiresAt.After(observedAt) {
		if state.Assignment.HolderTokenID == actor.ID && state.Assignment.Mode == domain.WorkItemAssignmentClaim &&
			sameClaimBinding(state.Assignment, binding) {
			return *state.Assignment, nil, nil
		}
		return domain.WorkItemAssignment{}, nil, &ClaimHeldError{
			HolderTokenID:     state.Assignment.HolderTokenID,
			AssignmentEventID: state.Assignment.AssignmentEventID,
			ExpiresAt:         state.Assignment.ExpiresAt,
		}
	}
	if state.Assignment != nil {
		if _, err := s.appendAssignmentReleaseInTx(ctx, tx, *state.Assignment, domain.AssignmentReleaseExpired, "", observedAt, actor); err != nil {
			return domain.WorkItemAssignment{}, nil, err
		}
		state, err = scanAssignmentState(ctx, tx, id, false)
		if err != nil {
			return domain.WorkItemAssignment{}, nil, err
		}
	}

	lease, leaseSource, err := s.resolveClaimLease(ctx, tx, id)
	if err != nil {
		return domain.WorkItemAssignment{}, nil, err
	}
	// Lease birth is a second DB-clock observation immediately before the
	// append path. The earlier observation decides incumbent expiry; keeping
	// it separate prevents lease/policy resolution after a lock wait from
	// consuming the replacement's short lease before it is even recorded.
	claimedAt, err := readAssignmentClock(ctx, tx)
	if err != nil {
		return domain.WorkItemAssignment{}, nil, err
	}
	return s.appendClaimInTx(ctx, tx, item, actor, lease, leaseSource, claimedAt, state.StateEventID, binding)
}

// sameClaimBinding decides whether an existing unexpired same-holder claim is
// THIS logical claim (idempotent success) or a different one (held conflict).
// An unbound retry matches an unbound claim; a listener-bound retry matches
// only an assignment attributed to the SAME listener — the same token backing
// two listener registrations must not treat one listener's lease as the
// other's.
func sameClaimBinding(existing *domain.WorkItemAssignment, binding *ClaimBinding) bool {
	if binding == nil {
		return existing.ListenerID == nil
	}
	return existing.ListenerID != nil && *existing.ListenerID == binding.ListenerID
}

// Yield releases the caller's active assignment. Yield is holder-only AND
// generation-fenced: the caller names the exact work_item.assigned event it
// intends to release, so a delayed stale yield appends nothing even when the
// same token has since reacquired the item under a new lease. Yield is the
// sole voluntary release reason; terminal transitions and the worker own
// done and expired respectively.
func (s *Service) Yield(ctx context.Context, id uuid.UUID, assignmentEventID uuid.UUID, actor domain.Token) (domain.WorkItemAssignment, error) {
	if actor.ID == uuid.Nil {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if actor.IsRoot {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: root token is mint/revoke-only", ErrClaimUnavailable)
	}
	if assignmentEventID == uuid.Nil {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: assignment_event_id is required", ErrInvalidRequest)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanWorkItemForUpdate(ctx, tx, id); err != nil {
		return domain.WorkItemAssignment{}, err
	}
	state, err := scanAssignmentStateForUpdate(ctx, tx, id)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if state.Assignment == nil {
		return domain.WorkItemAssignment{}, ErrAssignmentNotFound
	}
	if state.Assignment.HolderTokenID != actor.ID {
		return domain.WorkItemAssignment{}, ErrAssignmentNotHeld
	}
	if state.Assignment.AssignmentEventID != assignmentEventID {
		return domain.WorkItemAssignment{}, fmt.Errorf("%w: current=%s named=%s",
			ErrStaleAssignmentGeneration, state.Assignment.AssignmentEventID, assignmentEventID)
	}
	assignment := *state.Assignment
	releasedAt, err := readAssignmentClock(ctx, tx)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if _, err := s.appendAssignmentReleaseInTx(ctx, tx, assignment, domain.AssignmentReleaseYield, "", releasedAt, actor); err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItemAssignment{}, err
	}
	return assignment, nil
}

// GetAssignment returns the current event-projected holder. Expired-but-
// unswept rows remain visible until an expired release event is appended:
// wall-clock passage alone does not mutate truth. Claim opportunistically
// expires such a row, while the worker performs the ordinary sweep.
func (s *Service) GetAssignment(ctx context.Context, id uuid.UUID) (domain.WorkItemAssignment, error) {
	state, err := scanAssignmentState(ctx, s.pool, id, false)
	if err != nil {
		return domain.WorkItemAssignment{}, err
	}
	if state.Assignment == nil {
		return domain.WorkItemAssignment{}, ErrAssignmentNotFound
	}
	return *state.Assignment, nil
}

// ListAssignmentsForHolder is the restart-derivation read (listener control
// plane, slice 3): the active assignments a principal currently holds,
// ordered deterministically by (claimed_at, work_item_id). A restarted
// supervisor derives IDLE/FOCUSED from this projection — never from process
// memory. Expiry stays worker-owned: an expired-but-unreleased lease is still
// returned here (the holder resumes FOCUSED and observes the release), so a
// supervisor cannot silently double-claim during the expiry window.
func (s *Service) ListAssignmentsForHolder(ctx context.Context, holder uuid.UUID) ([]domain.WorkItemAssignment, error) {
	if holder == uuid.Nil {
		return nil, fmt.Errorf("workitems: holder token id is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT work_item_id, mode, assignment_event_id, claimed_at, expires_at, updated_at,
		       listener_id, demand_event_id, policy_event_id
		FROM work_item_assignment_state
		WHERE holder_token_id = $1
		ORDER BY claimed_at, work_item_id`, holder)
	if err != nil {
		return nil, fmt.Errorf("workitems: list assignments for holder: %w", err)
	}
	defer rows.Close()
	var out []domain.WorkItemAssignment
	for rows.Next() {
		assignment := domain.WorkItemAssignment{HolderTokenID: holder}
		var (
			mode                                   string
			listenerID, demandEventID, policyEvent pgtype.UUID
		)
		if err := rows.Scan(&assignment.WorkItemID, &mode, &assignment.AssignmentEventID,
			&assignment.ClaimedAt, &assignment.ExpiresAt, &assignment.UpdatedAt,
			&listenerID, &demandEventID, &policyEvent); err != nil {
			return nil, fmt.Errorf("workitems: scan holder assignment: %w", err)
		}
		assignment.Mode = domain.WorkItemAssignmentMode(mode)
		if listenerID.Valid {
			v := uuid.UUID(listenerID.Bytes)
			assignment.ListenerID = &v
		}
		if demandEventID.Valid {
			v := uuid.UUID(demandEventID.Bytes)
			assignment.DemandEventID = &v
		}
		if policyEvent.Valid {
			v := uuid.UUID(policyEvent.Bytes)
			assignment.PolicyEventID = &v
		}
		out = append(out, assignment)
	}
	return out, rows.Err()
}

// ExpireAssignment is the worker-owned cleanup seam. It revalidates one
// candidate under the global work_item -> assignment-state lock order and
// releases only when the exact persisted lease is due. The projection makes
// restart recovery independent of process memory.
func (s *Service) ExpireAssignment(ctx context.Context, id uuid.UUID, actor domain.Token) (bool, error) {
	if actor.ID == uuid.Nil || actor.IsRoot || actor.Source != domain.SourceSystem {
		return false, fmt.Errorf("%w: assignment expiry requires a dedicated system actor", ErrInvalidRequest)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanWorkItemForUpdate(ctx, tx, id); err != nil {
		return false, err
	}
	state, err := scanAssignmentStateForUpdate(ctx, tx, id)
	if err != nil {
		return false, err
	}
	databaseNow, err := readAssignmentClock(ctx, tx)
	if err != nil {
		return false, err
	}
	if state.Assignment == nil || !assignmentDue(state.Assignment.ExpiresAt, databaseNow.UTC()) {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if _, err := s.appendAssignmentReleaseInTx(ctx, tx, *state.Assignment, domain.AssignmentReleaseExpired, "", databaseNow.UTC(), actor); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func assignmentDue(expiresAt, databaseNow time.Time) bool {
	return !expiresAt.After(databaseNow)
}

func readAssignmentClock(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("workitems: read assignment database clock: %w", err)
	}
	return now.UTC(), nil
}

func claimableWorkItem(item domain.WorkItem) error {
	if item.State.Terminal() || item.State == domain.WorkItemBlocked || item.State == domain.WorkItemAwaitingApproval {
		return fmt.Errorf("%w: work item state %s is not claimable", ErrClaimUnavailable, item.State)
	}
	if item.HumanReviewStatus == domain.HumanReviewBlocked {
		return fmt.Errorf("%w: human review is blocked", ErrClaimUnavailable)
	}
	return nil
}

func (s *Service) resolveClaimLease(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID) (time.Duration, string, error) {
	meta, err := workItemLaunchMetadata(ctx, tx, workItemID)
	if err != nil {
		return 0, "", err
	}
	if cultivarRef := strings.TrimSpace(meta.Cultivar); cultivarRef != "" {
		xylem, resolvedRef, err := cultivarXylemForRefInTx(ctx, tx, cultivarRef)
		if err != nil {
			return 0, "", err
		}
		if xylem.MaxWallSeconds <= 0 {
			return 0, "", fmt.Errorf("%w: cultivar %s has non-positive xylem.max_wall_seconds", ErrInvalidRequest, cultivarRef)
		}
		lease := boundedClaimLeaseSeconds(xylem.MaxWallSeconds)
		return lease, "cultivar:" + resolvedRef + ":xylem.max_wall_seconds", nil
	}
	profileName := safety.ProfileSteady
	var storedProfile string
	err = tx.QueryRow(ctx, `SELECT name FROM active_policy_profile WHERE singleton`).Scan(&storedProfile)
	switch {
	case err == nil:
		profileName = storedProfile
	case errors.Is(err, pgx.ErrNoRows):
		// No switch event: steady is the event-sourced read default.
	default:
		return 0, "", fmt.Errorf("workitems: resolve active claim policy profile: %w", err)
	}
	policy, err := safety.ProfileByName(profileName)
	if err != nil {
		// Match policyprofile.Active's fail-closed compatibility behavior for a
		// newer stored name read by an older binary.
		profileName = safety.ProfileSteady
		policy, _ = safety.ProfileByName(profileName)
	}
	lease := boundedClaimLease(policy.PatienceBudgets[domain.WorkItemRunning])
	if lease <= 0 {
		return 0, "", fmt.Errorf("%w: active policy profile %s has no positive running patience budget", ErrInvalidRequest, profileName)
	}
	return lease, "policy_profile:" + profileName + ":running", nil
}

// cultivarXylemForRefInTx resolves the immutable xylem payload through the
// caller's transaction. Claim holds row locks while resolving leases and event
// budgets; opening another pool connection here deadlocks when MaxConns=1 and
// can starve a saturated pool.
func cultivarXylemForRefInTx(ctx context.Context, tx pgx.Tx, cultivarRef string) (registry.Xylem, string, error) {
	name, version, err := registry.ParseCultivarRef(cultivarRef)
	if err != nil {
		return registry.Xylem{}, "", err
	}
	var rawXylem []byte
	resolvedVersion := version
	if version == 0 {
		err = tx.QueryRow(ctx, `SELECT version, xylem FROM cultivars WHERE name = $1`, name).Scan(&resolvedVersion, &rawXylem)
	} else {
		err = tx.QueryRow(ctx, `
			SELECT payload->'xylem'
			FROM events
			WHERE subject_kind = $1
			  AND kind = $2
			  AND payload->>'name' = $3
			  AND (payload->>'version')::integer = $4
			ORDER BY seq DESC
			LIMIT 1
		`, domain.SubjectCultivar, domain.EventCultivarDefined, name, version).Scan(&rawXylem)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return registry.Xylem{}, "", fmt.Errorf("%w: no cultivar named %s", registry.ErrUnknownCultivar, cultivarRef)
		}
		return registry.Xylem{}, "", fmt.Errorf("workitems: resolve cultivar %s in transaction: %w", cultivarRef, err)
	}
	var xylem registry.Xylem
	if err := json.Unmarshal(rawXylem, &xylem); err != nil {
		return registry.Xylem{}, "", fmt.Errorf("workitems: decode cultivar %s xylem: %w", cultivarRef, err)
	}
	return xylem, fmt.Sprintf("%s@%d", name, resolvedVersion), nil
}

func boundedClaimLease(lease time.Duration) time.Duration {
	if lease > safety.MaxPatienceBudget {
		return safety.MaxPatienceBudget
	}
	return lease
}

func boundedClaimLeaseSeconds(seconds int) time.Duration {
	maxSeconds := int64(safety.MaxPatienceBudget / time.Second)
	if int64(seconds) > maxSeconds {
		return safety.MaxPatienceBudget
	}
	return time.Duration(seconds) * time.Second
}

func (s *Service) appendClaimInTx(ctx context.Context, tx pgx.Tx, item domain.WorkItem, actor domain.Token, lease time.Duration, leaseSource string, claimedAt time.Time, predecessorEventID uuid.UUID, binding *ClaimBinding) (domain.WorkItemAssignment, error, error) {
	if lease <= 0 || lease > safety.MaxPatienceBudget || lease%time.Second != 0 {
		return domain.WorkItemAssignment{}, nil, fmt.Errorf("%w: claim lease must be positive whole seconds and <= %s", ErrInvalidRequest, safety.MaxPatienceBudget)
	}
	if predecessorEventID == uuid.Nil {
		return domain.WorkItemAssignment{}, nil, fmt.Errorf("%w: assignment predecessor event is required", ErrAssignmentStateMissing)
	}
	discriminator := eventDiscriminator(ctx)
	if discriminator == "" {
		discriminator = "assignment_state:" + predecessorEventID.String()
	}
	spec := events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     item.ID,
		Kind:          domain.EventWorkItemAssigned,
		Source:        sourceForActor(actor),
		ActorTokenID:  &actor.ID,
		Discriminator: discriminator,
		Payload: assignedPayload{
			PayloadVersion:  assignmentPayloadVersion,
			AssigneeTokenID: actor.ID,
			Mode:            domain.WorkItemAssignmentClaim,
			LeaseSeconds:    int64(lease / time.Second),
			LeaseSource:     leaseSource,
			ClaimedAt:       claimedAt,
			ExpiresAt:       claimedAt.Add(lease),
			ListenerID:      bindingField(binding, func(b ClaimBinding) uuid.UUID { return b.ListenerID }),
			DemandEventID:   bindingField(binding, func(b ClaimBinding) uuid.UUID { return b.DemandEventID }),
			PolicyEventID:   bindingField(binding, func(b ClaimBinding) uuid.UUID { return b.PolicyEventID }),
		},
	}
	eventID, err := events.DeterministicID(spec)
	if err != nil {
		return domain.WorkItemAssignment{}, nil, err
	}
	exhausted, budgetErr, err := s.appendWorkItemEventWithRateBudget(ctx, tx, item, spec, "", actor)
	if err != nil {
		return domain.WorkItemAssignment{}, nil, err
	}
	if exhausted {
		return domain.WorkItemAssignment{}, budgetErr, nil
	}
	state, err := scanAssignmentState(ctx, tx, item.ID, false)
	if err != nil {
		return domain.WorkItemAssignment{}, nil, err
	}
	if state.Assignment == nil || state.Assignment.AssignmentEventID != eventID {
		return domain.WorkItemAssignment{}, nil, fmt.Errorf("workitems: work_item.assigned projector did not materialize exact event %s", eventID)
	}
	return *state.Assignment, nil, nil
}

func (s *Service) appendAssignmentReleaseInTx(ctx context.Context, tx pgx.Tx, assignment domain.WorkItemAssignment, reason domain.AssignmentReleaseReason, terminalState domain.WorkItemState, releasedAt time.Time, actor domain.Token) (bool, error) {
	payload := assignmentReleasedPayload{
		PayloadVersion:    assignmentPayloadVersion,
		AssignmentEventID: assignment.AssignmentEventID,
		AssigneeTokenID:   assignment.HolderTokenID,
		Mode:              assignment.Mode,
		Reason:            reason,
		TerminalState:     terminalState,
		ReleasedAt:        releasedAt,
	}
	_, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    assignment.WorkItemID,
		Kind:         domain.EventWorkItemAssignmentReleased,
		Source:       sourceForActor(actor),
		ActorTokenID: &actor.ID,
		Payload:      payload,
	})
	if err != nil {
		return false, err
	}
	if !fresh {
		return false, fmt.Errorf("%w: work_item.assignment_released unexpectedly deduped", ErrUnexpectedEventDedupe)
	}
	return true, nil
}

func scanAssignmentStateForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (assignmentState, error) {
	return scanAssignmentState(ctx, tx, id, true)
}

func scanAssignmentState(ctx context.Context, q queryer, id uuid.UUID, forUpdate bool) (assignmentState, error) {
	query := `
		SELECT holder_token_id, mode, assignment_event_id, claimed_at, expires_at,
		       listener_id, demand_event_id, policy_event_id,
		       state_event_id, state_event_seq, updated_at
		FROM work_item_assignment_state
		WHERE work_item_id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var (
		holder          pgtype.UUID
		mode            pgtype.Text
		assignmentEvent pgtype.UUID
		claimedAt       pgtype.Timestamptz
		expiresAt       pgtype.Timestamptz
		listenerID      pgtype.UUID
		demandEventID   pgtype.UUID
		policyEventID   pgtype.UUID
		stateEvent      uuid.UUID
		state           assignmentState
	)
	if err := q.QueryRow(ctx, query, id).Scan(
		&holder, &mode, &assignmentEvent, &claimedAt, &expiresAt,
		&listenerID, &demandEventID, &policyEventID,
		&stateEvent, &state.StateEventSeq, &state.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return assignmentState{}, ErrAssignmentStateMissing
		}
		return assignmentState{}, fmt.Errorf("workitems: scan assignment state: %w", err)
	}
	state.StateEventID = stateEvent
	activeFields := []bool{holder.Valid, mode.Valid, assignmentEvent.Valid, claimedAt.Valid, expiresAt.Valid}
	activeCount := 0
	for _, valid := range activeFields {
		if valid {
			activeCount++
		}
	}
	if activeCount == 0 {
		return state, nil
	}
	if activeCount != len(activeFields) {
		return assignmentState{}, fmt.Errorf("workitems: incomplete active assignment projection for %s", id)
	}
	assignment := domain.WorkItemAssignment{
		WorkItemID:        id,
		HolderTokenID:     uuid.UUID(holder.Bytes),
		Mode:              domain.WorkItemAssignmentMode(mode.String),
		AssignmentEventID: uuid.UUID(assignmentEvent.Bytes),
		ClaimedAt:         claimedAt.Time,
		ExpiresAt:         expiresAt.Time,
		UpdatedAt:         state.UpdatedAt,
	}
	if listenerID.Valid {
		v := uuid.UUID(listenerID.Bytes)
		assignment.ListenerID = &v
	}
	if demandEventID.Valid {
		v := uuid.UUID(demandEventID.Bytes)
		assignment.DemandEventID = &v
	}
	if policyEventID.Valid {
		v := uuid.UUID(policyEventID.Bytes)
		assignment.PolicyEventID = &v
	}
	if !assignment.Mode.Valid() {
		return assignmentState{}, fmt.Errorf("workitems: invalid assignment mode %q for %s", assignment.Mode, id)
	}
	state.Assignment = &assignment
	return state, nil
}
