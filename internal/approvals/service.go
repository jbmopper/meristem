// Package approvals owns the default-deny decision primitive for external
// side effects. Approval rows are projections of approval.* events; work item
// lifecycle movement is recorded as normal work_item.transitioned events in
// the same transaction as the approval fact that caused it.
package approvals

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
)

var (
	ErrNotFound           = errors.New("approvals: not found")
	ErrActorRequired      = errors.New("approvals: actor token is required")
	ErrInvalidRequest     = errors.New("approvals: invalid request")
	ErrHumanDecisionToken = errors.New("approvals: decision requires a human non-root token")
	ErrSeparationOfDuties = errors.New("approvals: requesting token cannot decide the same approval")
	ErrAlreadyDecided     = errors.New("approvals: approval already decided")
	ErrInvalidDecision    = errors.New("approvals: invalid decision")
)

type Status string

const (
	StatusPending  Status = "pending"
	StatusApproved Status = "approved"
	StatusDenied   Status = "denied"
	StatusExpired  Status = "expired"
)

type Decision string

const (
	DecisionApproved Decision = "approved"
	DecisionDenied   Decision = "denied"
	DecisionExpired  Decision = "expired"
)

type Approval struct {
	ID              uuid.UUID       `json:"id"`
	WorkItemID      uuid.UUID       `json:"work_item_id"`
	Status          Status          `json:"status"`
	Summary         string          `json:"summary"`
	Request         json.RawMessage `json:"request"`
	RequestedBy     *uuid.UUID      `json:"requested_by,omitempty"`
	RequestedSource domain.Source   `json:"requested_source"`
	CreatedAt       time.Time       `json:"created_at"`
	ExpiresAt       time.Time       `json:"expires_at"`
	DecidedBy       *uuid.UUID      `json:"decided_by,omitempty"`
	DecisionSource  *domain.Source  `json:"decision_source,omitempty"`
	DecidedAt       *time.Time      `json:"decided_at,omitempty"`
	Decision        *Decision       `json:"decision,omitempty"`
	DecisionReason  string          `json:"decision_reason"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
	clock  func() time.Time
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return NewServiceWithClock(pool, writer, nil)
}

func NewServiceWithClock(pool *pgxpool.Pool, writer *events.Writer, clock func() time.Time) *Service {
	if clock == nil {
		clock = time.Now
	}
	return &Service{pool: pool, writer: writer, clock: clock}
}

type CreateInput struct {
	ApprovalID uuid.UUID
	WorkItemID uuid.UUID
	Summary    string
	Request    any
	ExpiresIn  time.Duration
	Actor      domain.Token
}

type DecisionInput struct {
	ApprovalID uuid.UUID
	Decision   Decision
	Reason     string
	Actor      domain.Token
}

type ExpireInput struct {
	ApprovalID uuid.UUID
	Reason     string
	Actor      domain.Token
}

type Result struct {
	Approval Approval
	Fresh    bool
	EventID  uuid.UUID
}

func (s *Service) Create(ctx context.Context, in CreateInput) (Result, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	approvalID, eventID, fresh, err := s.CreateInTx(ctx, tx, in)
	if err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	approval, err := s.Get(ctx, approvalID)
	if err != nil {
		return Result{}, err
	}
	return Result{Approval: approval, Fresh: fresh, EventID: eventID}, nil
}

func (s *Service) CreateInTx(ctx context.Context, tx pgx.Tx, in CreateInput) (approvalID uuid.UUID, eventID uuid.UUID, fresh bool, err error) {
	if in.Actor.ID == uuid.Nil {
		return uuid.Nil, uuid.Nil, false, ErrActorRequired
	}
	if in.WorkItemID == uuid.Nil {
		return uuid.Nil, uuid.Nil, false, fmt.Errorf("%w: work_item_id is required", ErrInvalidRequest)
	}
	summary := strings.TrimSpace(in.Summary)
	if summary == "" {
		return uuid.Nil, uuid.Nil, false, fmt.Errorf("%w: summary is required", ErrInvalidRequest)
	}
	expiresIn := in.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = time.Hour
	}
	expiresAt := s.clock().UTC().Add(expiresIn).Truncate(time.Microsecond)
	request := in.Request
	if request == nil {
		request = map[string]any{}
	}
	approvalID = in.ApprovalID
	if approvalID == uuid.Nil {
		approvalID = newSubjectID(ctx, "approval")
	}

	item, err := scanWorkItem(ctx, tx, in.WorkItemID)
	if err != nil {
		return uuid.Nil, uuid.Nil, false, err
	}
	if item.State.Terminal() {
		return uuid.Nil, uuid.Nil, false, fmt.Errorf("%w: cannot request approval for terminal work item %s", ErrInvalidRequest, in.WorkItemID)
	}

	eventID, fresh, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectApproval,
		SubjectID:    approvalID,
		Kind:         domain.EventApprovalCreated,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload: map[string]any{
			"work_item_id": in.WorkItemID,
			"summary":      summary,
			"request":      request,
			"expires_at":   expiresAt.Format(time.RFC3339Nano),
		},
	})
	if err != nil {
		return uuid.Nil, uuid.Nil, false, err
	}
	if item.State != domain.WorkItemAwaitingApproval {
		if _, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:   domain.SubjectWorkItem,
			SubjectID:     in.WorkItemID,
			Kind:          domain.EventWorkItemTransitioned,
			Source:        sourceForActor(in.Actor),
			ActorTokenID:  &in.Actor.ID,
			Discriminator: eventDiscriminator(ctx),
			Payload: map[string]any{
				"from":   item.State,
				"to":     domain.WorkItemAwaitingApproval,
				"reason": "approval requested: " + summary,
			},
		}); err != nil {
			return uuid.Nil, uuid.Nil, false, err
		}
	}
	return approvalID, eventID, fresh, nil
}

func (s *Service) Decide(ctx context.Context, in DecisionInput) (Result, error) {
	if in.Actor.ID == uuid.Nil {
		return Result{}, ErrActorRequired
	}
	if in.Actor.IsRoot || in.Actor.Source != domain.SourceHuman {
		return Result{}, ErrHumanDecisionToken
	}
	if in.ApprovalID == uuid.Nil {
		return Result{}, fmt.Errorf("approvals: approval_id is required")
	}
	if in.Decision != DecisionApproved && in.Decision != DecisionDenied {
		return Result{}, ErrInvalidDecision
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, err := scanApprovalForUpdate(ctx, tx, in.ApprovalID)
	if err != nil {
		return Result{}, err
	}
	if current.RequestedBy != nil && *current.RequestedBy == in.Actor.ID {
		return Result{}, ErrSeparationOfDuties
	}
	if current.Status != StatusPending {
		if current.Decision != nil && *current.Decision == in.Decision {
			return Result{Approval: current, Fresh: false}, nil
		}
		return Result{}, ErrAlreadyDecided
	}

	reason := strings.TrimSpace(in.Reason)
	eventID, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectApproval,
		SubjectID:     in.ApprovalID,
		Kind:          domain.EventApprovalDecided,
		Source:        sourceForActor(in.Actor),
		ActorTokenID:  &in.Actor.ID,
		Discriminator: eventDiscriminator(ctx),
		Payload: map[string]any{
			"work_item_id": current.WorkItemID,
			"decision":     string(in.Decision),
			"reason":       reason,
		},
	})
	if err != nil {
		return Result{}, err
	}
	to := domain.WorkItemRunning
	stateReason := "approval_approved"
	if in.Decision == DecisionDenied {
		to = domain.WorkItemFailed
		stateReason = "approval_denied"
	}
	if err := appendApprovalTransition(ctx, tx, s.writer, in.Actor, current.WorkItemID, to, stateReason, eventDiscriminator(ctx)); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	approval, err := s.Get(ctx, in.ApprovalID)
	if err != nil {
		return Result{}, err
	}
	return Result{Approval: approval, Fresh: fresh, EventID: eventID}, nil
}

func (s *Service) Expire(ctx context.Context, in ExpireInput) (Result, error) {
	if in.Actor.ID == uuid.Nil {
		return Result{}, ErrActorRequired
	}
	if in.ApprovalID == uuid.Nil {
		return Result{}, fmt.Errorf("approvals: approval_id is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, err := scanApprovalForUpdate(ctx, tx, in.ApprovalID)
	if err != nil {
		return Result{}, err
	}
	if current.Status != StatusPending {
		if current.Status == StatusExpired {
			return Result{Approval: current, Fresh: false}, nil
		}
		return Result{}, ErrAlreadyDecided
	}
	reason := strings.TrimSpace(in.Reason)
	if reason == "" {
		reason = "approval_expired"
	}
	eventID, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectApproval,
		SubjectID:     in.ApprovalID,
		Kind:          domain.EventApprovalExpired,
		Source:        sourceForActor(in.Actor),
		ActorTokenID:  &in.Actor.ID,
		Discriminator: eventDiscriminator(ctx),
		Payload: map[string]any{
			"work_item_id": current.WorkItemID,
			"reason":       reason,
		},
	})
	if err != nil {
		return Result{}, err
	}
	if err := appendApprovalTransition(ctx, tx, s.writer, in.Actor, current.WorkItemID, domain.WorkItemBlocked, "approval_expired", eventDiscriminator(ctx)); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	approval, err := s.Get(ctx, in.ApprovalID)
	if err != nil {
		return Result{}, err
	}
	return Result{Approval: approval, Fresh: fresh, EventID: eventID}, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Approval, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, work_item_id, status, summary, request, requested_by, requested_source,
		       created_at, expires_at, decided_by, decision_source, decided_at, decision,
		       decision_reason, updated_at
		FROM approvals
		WHERE id = $1
	`, id)
	return scanApproval(row)
}

func (s *Service) ListForWorkItem(ctx context.Context, workItemID uuid.UUID) ([]Approval, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, work_item_id, status, summary, request, requested_by, requested_source,
		       created_at, expires_at, decided_by, decision_source, decided_at, decision,
		       decision_reason, updated_at
		FROM approvals
		WHERE work_item_id = $1
		ORDER BY updated_at DESC, id DESC
	`, workItemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Approval
	for rows.Next() {
		item, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type workItemRow struct {
	ID    uuid.UUID
	State domain.WorkItemState
}

func scanWorkItem(ctx context.Context, tx pgx.Tx, id uuid.UUID) (workItemRow, error) {
	var row workItemRow
	var state string
	err := tx.QueryRow(ctx, `SELECT id, state FROM work_items WHERE id = $1 FOR UPDATE`, id).Scan(&row.ID, &state)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workItemRow{}, ErrNotFound
		}
		return workItemRow{}, err
	}
	row.State = domain.WorkItemState(state)
	return row, nil
}

func scanApprovalForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Approval, error) {
	row := tx.QueryRow(ctx, `
		SELECT id, work_item_id, status, summary, request, requested_by, requested_source,
		       created_at, expires_at, decided_by, decision_source, decided_at, decision,
		       decision_reason, updated_at
		FROM approvals
		WHERE id = $1
		FOR UPDATE
	`, id)
	return scanApproval(row)
}

func scanApproval(row pgx.Row) (Approval, error) {
	var (
		item            Approval
		status          string
		requestedBy     uuid.NullUUID
		requestedSource string
		decidedBy       uuid.NullUUID
		decisionSource  sql.NullString
		decidedAt       sql.NullTime
		decision        sql.NullString
	)
	err := row.Scan(
		&item.ID,
		&item.WorkItemID,
		&status,
		&item.Summary,
		&item.Request,
		&requestedBy,
		&requestedSource,
		&item.CreatedAt,
		&item.ExpiresAt,
		&decidedBy,
		&decisionSource,
		&decidedAt,
		&decision,
		&item.DecisionReason,
		&item.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Approval{}, ErrNotFound
		}
		return Approval{}, err
	}
	item.Status = Status(status)
	if requestedBy.Valid {
		id := requestedBy.UUID
		item.RequestedBy = &id
	}
	item.RequestedSource = domain.Source(requestedSource)
	if decidedBy.Valid {
		id := decidedBy.UUID
		item.DecidedBy = &id
	}
	if decisionSource.Valid {
		source := domain.Source(decisionSource.String)
		item.DecisionSource = &source
	}
	if decidedAt.Valid {
		t := decidedAt.Time
		item.DecidedAt = &t
	}
	if decision.Valid {
		dec := Decision(decision.String)
		item.Decision = &dec
	}
	if len(item.Request) == 0 {
		item.Request = json.RawMessage(`{}`)
	}
	return item, nil
}

func appendApprovalTransition(ctx context.Context, tx pgx.Tx, writer *events.Writer, actor domain.Token, workItemID uuid.UUID, to domain.WorkItemState, reason, discriminator string) error {
	item, err := scanWorkItem(ctx, tx, workItemID)
	if err != nil {
		return err
	}
	if item.State == to {
		return nil
	}
	if !domain.CanTransition(item.State, to) {
		return fmt.Errorf("approvals: invalid work item transition from %s to %s", item.State, to)
	}
	_, _, err = writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     workItemID,
		Kind:          domain.EventWorkItemTransitioned,
		Source:        sourceForActor(actor),
		ActorTokenID:  &actor.ID,
		Discriminator: discriminator,
		Payload: map[string]any{
			"from":   item.State,
			"to":     to,
			"reason": reason,
		},
	})
	return err
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}

func eventDiscriminator(ctx context.Context) string {
	disc, _ := idempotency.EventDiscriminator(ctx)
	return disc
}

func newSubjectID(ctx context.Context, label string) uuid.UUID {
	if id, ok := idempotency.SubjectID(ctx, label); ok {
		return id
	}
	return uuid.New()
}
