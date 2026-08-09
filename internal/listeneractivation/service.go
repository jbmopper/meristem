// Package listeneractivation owns durable, restart-safe delivery state for
// listener adapters. It never contacts an external application. A supervisor
// first records a finite dispatch lease here, then invokes one adapter, then
// records the adapter's structural receipt.
package listeneractivation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

const (
	PayloadVersion       = 1
	MaxDispatches        = 3
	MaxReconciliations   = 3
	DispatchLease        = 60 * time.Second
	AcceptedLease        = 30 * time.Minute
	RetryDelay           = 20 * time.Second
	maxBindingGeneration = 200

	// ReasonAdapterTargetBusy is expected backpressure from a bound app task
	// that is still executing another turn. It remains retryable until the
	// assignment lease/patience ends and does not consume the adapter-failure
	// budget; no admission has been attempted at this point.
	ReasonAdapterTargetBusy = "adapter_target_busy"
)

var (
	ErrInvalidRequest     = errors.New("listeneractivation: invalid request")
	ErrNotFound           = errors.New("listeneractivation: not found")
	ErrNotAuthorized      = errors.New("listeneractivation: not authorized")
	ErrStaleState         = errors.New("listeneractivation: stale state")
	ErrNoActiveAssignment = errors.New("listeneractivation: no matching active assignment")
)

type State string

const (
	StateRequested   State = "requested"
	StateDispatching State = "dispatching"
	StateAccepted    State = "accepted"
	StateCompleted   State = "completed"
	StateFailed      State = "failed"
	StateAmbiguous   State = "ambiguous"
)

type DispatchMode string

const (
	ModeDispatch  DispatchMode = "dispatch"
	ModeReconcile DispatchMode = "reconcile"
)

type Action string

const (
	ActionDispatch  Action = "dispatch"
	ActionReconcile Action = "reconcile"
	ActionWait      Action = "wait"
	ActionTerminal  Action = "terminal"
)

type Activation struct {
	ID                 uuid.UUID
	ListenerID         uuid.UUID
	WorkItemID         uuid.UUID
	AssignmentEventID  uuid.UUID
	DemandEventID      uuid.UUID
	Attempt            int
	AdapterKind        string
	BindingGeneration  string
	State              State
	DispatchMode       DispatchMode
	ConsumerGeneration string
	LeaseExpiresAt     *time.Time
	DispatchCount      int
	ReconcileCount     int
	NextRetryAt        *time.Time
	LastReason         string
	LastOutcomeEventID uuid.UUID
	StateEventID       uuid.UUID
	StateEventSeq      int64
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type Service struct {
	pool          *pgxpool.Pool
	writer        *events.Writer
	dispatchLease time.Duration
	acceptedLease time.Duration
	retryDelay    time.Duration
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{
		pool: pool, writer: writer,
		dispatchLease: DispatchLease, acceptedLease: AcceptedLease, retryDelay: RetryDelay,
	}
}

type EnsureInput struct {
	ListenerID        uuid.UUID
	AssignmentEventID uuid.UUID
	BindingGeneration string
	Attempt           int
	Actor             domain.Token
}

func (s *Service) Ensure(ctx context.Context, in EnsureInput) (Activation, error) {
	if in.ListenerID == uuid.Nil || in.AssignmentEventID == uuid.Nil {
		return Activation{}, fmt.Errorf("%w: listener_id and assignment_event_id are required", ErrInvalidRequest)
	}
	if in.Attempt == 0 {
		in.Attempt = 1
	}
	if in.Attempt < 1 {
		return Activation{}, fmt.Errorf("%w: attempt must be positive", ErrInvalidRequest)
	}
	binding, err := normalizeBindingGeneration(in.BindingGeneration)
	if err != nil {
		return Activation{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Activation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	adapterKind, err := requirePrincipal(ctx, tx, in.ListenerID, in.Actor, true)
	if err != nil {
		return Activation{}, err
	}
	var workItemID, holderID, listenerID, demandEventID uuid.UUID
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT work_item_id, holder_token_id, listener_id, demand_event_id, expires_at
		FROM work_item_assignment_state
		WHERE assignment_event_id = $1
		FOR UPDATE
	`, in.AssignmentEventID).Scan(&workItemID, &holderID, &listenerID, &demandEventID, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Activation{}, ErrNoActiveAssignment
	}
	if err != nil {
		return Activation{}, fmt.Errorf("listeneractivation: read assignment: %w", err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return Activation{}, err
	}
	if holderID != in.Actor.ID || listenerID != in.ListenerID || !expiresAt.After(now) {
		return Activation{}, ErrNoActiveAssignment
	}
	id := ActivationID(in.AssignmentEventID, binding, in.Attempt)
	payload := map[string]any{
		"payload_version":     PayloadVersion,
		"listener_id":         in.ListenerID,
		"work_item_id":        workItemID,
		"assignment_event_id": in.AssignmentEventID,
		"demand_event_id":     demandEventID,
		"attempt":             in.Attempt,
		"adapter_kind":        adapterKind,
		"binding_generation":  binding,
		"assignee_token_id":   in.Actor.ID,
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectListenerActivation, SubjectID: id,
		Kind: domain.EventListenerActivationRequested, Source: in.Actor.Source,
		ActorTokenID: &in.Actor.ID, Payload: payload,
	}); err != nil {
		return Activation{}, err
	}
	out, err := scanActivation(ctx, tx, id, false)
	if err != nil {
		return Activation{}, err
	}
	if out.ListenerID != in.ListenerID || out.AssignmentEventID != in.AssignmentEventID || out.BindingGeneration != binding || out.Attempt != in.Attempt {
		return Activation{}, fmt.Errorf("%w: deterministic activation identity collision", ErrInvalidRequest)
	}
	if err := tx.Commit(ctx); err != nil {
		return Activation{}, err
	}
	return out, nil
}

type BeginInput struct {
	ActivationID       uuid.UUID
	ConsumerGeneration string
	Actor              domain.Token
}

type BeginResult struct {
	Activation Activation
	Action     Action
}

func (s *Service) Begin(ctx context.Context, in BeginInput) (BeginResult, error) {
	consumer, err := normalizeBindingGeneration(in.ConsumerGeneration)
	if err != nil {
		return BeginResult{}, err
	}
	listenerID, err := s.activationListenerID(ctx, in.ActivationID)
	if err != nil {
		return BeginResult{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return BeginResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := requirePrincipal(ctx, tx, listenerID, in.Actor, true); err != nil {
		return BeginResult{}, err
	}
	current, err := scanActivation(ctx, tx, in.ActivationID, true)
	if err != nil {
		return BeginResult{}, err
	}
	if err := requireCurrentAssignment(ctx, tx, current, in.Actor); err != nil {
		return BeginResult{}, err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return BeginResult{}, err
	}
	if (current.State == StateDispatching || current.State == StateAccepted) && current.LeaseExpiresAt != nil && current.LeaseExpiresAt.After(now) {
		if err := tx.Commit(ctx); err != nil {
			return BeginResult{}, err
		}
		return BeginResult{Activation: current, Action: ActionWait}, nil
	}
	if current.State == StateCompleted ||
		(current.State == StateFailed && current.DispatchCount >= MaxDispatches && current.LastReason != ReasonAdapterTargetBusy) ||
		(current.State == StateAmbiguous && current.ReconcileCount >= MaxReconciliations) {
		if err := tx.Commit(ctx); err != nil {
			return BeginResult{}, err
		}
		return BeginResult{Activation: current, Action: ActionTerminal}, nil
	}
	if current.NextRetryAt != nil && current.NextRetryAt.After(now) {
		if err := tx.Commit(ctx); err != nil {
			return BeginResult{}, err
		}
		return BeginResult{Activation: current, Action: ActionWait}, nil
	}
	mode := ModeDispatch
	if current.State == StateDispatching || current.State == StateAccepted {
		current, err = s.appendOutcomeInTx(ctx, tx, current, StateAmbiguous, in.Actor, "dispatch lease expired before a durable terminal receipt", now, nil)
		if err != nil {
			return BeginResult{}, err
		}
		mode = ModeReconcile
	} else if current.State == StateAmbiguous {
		mode = ModeReconcile
	}
	leaseExpires := now.Add(s.dispatchLease)
	_, _, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectListenerActivation, SubjectID: current.ID,
		Kind: domain.EventListenerActivationDispatching, Source: in.Actor.Source,
		ActorTokenID:  &in.Actor.ID,
		Discriminator: "activation_state:" + current.StateEventID.String(),
		Payload: map[string]any{
			"payload_version": PayloadVersion, "from": current.State,
			"mode": mode, "consumer_generation": consumer,
			"lease_expires_at":  leaseExpires,
			"assignee_token_id": in.Actor.ID, "work_item_id": current.WorkItemID,
		},
	})
	if err != nil {
		return BeginResult{}, err
	}
	out, err := scanActivation(ctx, tx, current.ID, false)
	if err != nil {
		return BeginResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return BeginResult{}, err
	}
	action := ActionDispatch
	if mode == ModeReconcile {
		action = ActionReconcile
	}
	return BeginResult{Activation: out, Action: action}, nil
}

type ReceiptInput struct {
	ActivationID         uuid.UUID
	ObservedStateEventID uuid.UUID
	ConsumerGeneration   string
	Outcome              State
	Reason               string
	Actor                domain.Token
}

var allowedReceiptReasons = map[string]bool{
	"turn_admitted":               true,
	"reconciled_in_progress_turn": true,
	"reconciled_completed_turn":   true,
	"reconciled_terminal_turn":    true,
	"turn_completed":              true,
	"turn_terminal_failure":       true,
	"adapter_start_failed":        true,
	"adapter_retryable_failure":   true,
	"adapter_outcome_ambiguous":   true,
	"adapter_protocol_invalid":    true,
	ReasonAdapterTargetBusy:       true,
}

func (s *Service) RecordReceipt(ctx context.Context, in ReceiptInput) (Activation, error) {
	if in.ObservedStateEventID == uuid.Nil {
		return Activation{}, fmt.Errorf("%w: observed_state_event_id is required", ErrInvalidRequest)
	}
	consumer, err := normalizeBindingGeneration(in.ConsumerGeneration)
	if err != nil {
		return Activation{}, err
	}
	switch in.Outcome {
	case StateAccepted, StateCompleted, StateFailed, StateAmbiguous:
	default:
		return Activation{}, fmt.Errorf("%w: invalid receipt outcome %q", ErrInvalidRequest, in.Outcome)
	}
	in.Reason = strings.TrimSpace(in.Reason)
	if !allowedReceiptReasons[in.Reason] {
		return Activation{}, fmt.Errorf("%w: receipt reason is not in the structural allowlist", ErrInvalidRequest)
	}
	listenerID, err := s.activationListenerID(ctx, in.ActivationID)
	if err != nil {
		return Activation{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Activation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := requirePrincipal(ctx, tx, listenerID, in.Actor, true); err != nil {
		return Activation{}, err
	}
	current, err := scanActivation(ctx, tx, in.ActivationID, true)
	if err != nil {
		return Activation{}, err
	}
	if current.StateEventID != in.ObservedStateEventID {
		return Activation{}, fmt.Errorf("%w: observed=%s current=%s", ErrStaleState, in.ObservedStateEventID, current.StateEventID)
	}
	if current.ConsumerGeneration != consumer || (current.State != StateDispatching && current.State != StateAccepted) {
		return Activation{}, fmt.Errorf("%w: receipt does not own current dispatch generation", ErrStaleState)
	}
	if current.State == StateAccepted && in.Outcome == StateAccepted {
		return Activation{}, fmt.Errorf("%w: duplicate accepted receipt", ErrStaleState)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return Activation{}, err
	}
	var next *time.Time
	if in.Outcome == StateFailed && (current.DispatchCount < MaxDispatches || in.Reason == ReasonAdapterTargetBusy) {
		v := now.Add(s.retryDelay)
		next = &v
	}
	if in.Outcome == StateAmbiguous && current.ReconcileCount < MaxReconciliations {
		v := now.Add(s.retryDelay)
		next = &v
	}
	out, err := s.appendOutcomeInTx(ctx, tx, current, in.Outcome, in.Actor, in.Reason, now, next)
	if err != nil {
		return Activation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Activation{}, err
	}
	return out, nil
}

func (s *Service) appendOutcomeInTx(ctx context.Context, tx pgx.Tx, current Activation, outcome State, actor domain.Token, reason string, now time.Time, next *time.Time) (Activation, error) {
	kinds := map[State]string{
		StateAccepted:  domain.EventListenerActivationAccepted,
		StateCompleted: domain.EventListenerActivationCompleted,
		StateFailed:    domain.EventListenerActivationFailed,
		StateAmbiguous: domain.EventListenerActivationAmbiguous,
	}
	payload := map[string]any{
		"payload_version": PayloadVersion, "from": current.State,
		"reason": reason, "assignee_token_id": actor.ID,
		"work_item_id": current.WorkItemID,
	}
	if outcome == StateAccepted {
		payload["lease_expires_at"] = now.Add(s.acceptedLease)
	}
	if next != nil {
		payload["next_retry_at"] = *next
	}
	_, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind: domain.SubjectListenerActivation, SubjectID: current.ID,
		Kind: kinds[outcome], Source: actor.Source, ActorTokenID: &actor.ID,
		Discriminator: "activation_state:" + current.StateEventID.String(), Payload: payload,
	})
	if err != nil {
		return Activation{}, err
	}
	return scanActivation(ctx, tx, current.ID, false)
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Activation, error) {
	return scanActivation(ctx, s.pool, id, false)
}

func (s *Service) GetForAssignment(ctx context.Context, assignmentEventID uuid.UUID, bindingGeneration string, attempt int) (Activation, error) {
	binding, err := normalizeBindingGeneration(bindingGeneration)
	if err != nil {
		return Activation{}, err
	}
	if attempt == 0 {
		attempt = 1
	}
	return s.Get(ctx, ActivationID(assignmentEventID, binding, attempt))
}

func ActivationID(assignmentEventID uuid.UUID, bindingGeneration string, attempt int) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("meristem|listener_activation|%s|%s|%d", assignmentEventID, bindingGeneration, attempt)))
}

func (s *Service) activationListenerID(ctx context.Context, activationID uuid.UUID) (uuid.UUID, error) {
	if activationID == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: activation_id is required", ErrInvalidRequest)
	}
	var listenerID uuid.UUID
	if err := s.pool.QueryRow(ctx, `SELECT listener_id FROM listener_activations WHERE id=$1`, activationID).Scan(&listenerID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrNotFound
		}
		return uuid.Nil, err
	}
	return listenerID, nil
}

func requirePrincipal(ctx context.Context, tx pgx.Tx, listenerID uuid.UUID, actor domain.Token, lock bool) (string, error) {
	if actor.ID == uuid.Nil || actor.IsRoot || actor.RevokedAt != nil {
		return "", ErrNotAuthorized
	}
	query := `SELECT principal_token_id, provider, retired_at FROM listener_registrations WHERE id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	var principal uuid.UUID
	var provider string
	var retired pgtype.Timestamptz
	if err := tx.QueryRow(ctx, query, listenerID).Scan(&principal, &provider, &retired); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	if retired.Valid || principal != actor.ID {
		return "", ErrNotAuthorized
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" || len(provider) > 64 || strings.ContainsAny(provider, "\r\n\t ") {
		return "", fmt.Errorf("%w: listener provider must name one adapter kind", ErrInvalidRequest)
	}
	return provider, nil
}

func requireCurrentAssignment(ctx context.Context, tx pgx.Tx, activation Activation, actor domain.Token) error {
	var holder, listener, assignment pgtype.UUID
	var expires pgtype.Timestamptz
	err := tx.QueryRow(ctx, `
		SELECT holder_token_id, listener_id, assignment_event_id, expires_at
		FROM work_item_assignment_state WHERE work_item_id=$1 FOR UPDATE
	`, activation.WorkItemID).Scan(&holder, &listener, &assignment, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoActiveAssignment
	}
	if err != nil {
		return err
	}
	if !holder.Valid || !listener.Valid || !assignment.Valid || !expires.Valid {
		return ErrNoActiveAssignment
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	if uuid.UUID(holder.Bytes) != actor.ID || uuid.UUID(listener.Bytes) != activation.ListenerID || uuid.UUID(assignment.Bytes) != activation.AssignmentEventID || !expires.Time.After(now) {
		return ErrNoActiveAssignment
	}
	return nil
}

func normalizeBindingGeneration(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxBindingGeneration || strings.ContainsAny(value, "\x00\r\n") {
		return "", fmt.Errorf("%w: binding/consumer generation must be 1-%d visible bytes", ErrInvalidRequest, maxBindingGeneration)
	}
	return value, nil
}

type queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func scanActivation(ctx context.Context, q queryer, id uuid.UUID, forUpdate bool) (Activation, error) {
	query := `
		SELECT id, listener_id, work_item_id, assignment_event_id, demand_event_id,
		       attempt, adapter_kind, binding_generation, state, dispatch_mode,
		       consumer_generation, lease_expires_at, dispatch_count,
		       reconcile_count, next_retry_at, last_reason,
		       last_outcome_event_id, state_event_id, state_event_seq,
		       created_at, updated_at
		FROM listener_activations WHERE id=$1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var out Activation
	var mode, consumer pgtype.Text
	var lease, retry pgtype.Timestamptz
	if err := q.QueryRow(ctx, query, id).Scan(
		&out.ID, &out.ListenerID, &out.WorkItemID, &out.AssignmentEventID,
		&out.DemandEventID, &out.Attempt, &out.AdapterKind,
		&out.BindingGeneration, &out.State, &mode, &consumer, &lease,
		&out.DispatchCount, &out.ReconcileCount, &retry, &out.LastReason,
		&out.LastOutcomeEventID, &out.StateEventID, &out.StateEventSeq,
		&out.CreatedAt, &out.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Activation{}, ErrNotFound
		}
		return Activation{}, err
	}
	if mode.Valid {
		out.DispatchMode = DispatchMode(mode.String)
	}
	if consumer.Valid {
		out.ConsumerGeneration = consumer.String
	}
	if lease.Valid {
		v := lease.Time
		out.LeaseExpiresAt = &v
	}
	if retry.Valid {
		v := retry.Time
		out.NextRetryAt = &v
	}
	return out, nil
}
