// Package escalations records deterministic handoffs to the human operator.
//
// Escalation is not a side channel. A reducer or worker asks for human
// attention by appending an escalation event and by creating a normal
// human-visible work_item in the same transaction.
package escalations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

var ErrNotFound = errors.New("escalations: work_item not found")

type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

type RequestInput struct {
	WorkItemID uuid.UUID
	Reason     string
	Summary    string
	Actor      domain.Token
}

type RequestResult struct {
	EscalationID    uuid.UUID
	HumanWorkItemID uuid.UUID
	Fresh           bool
}

func (s *Service) Request(ctx context.Context, in RequestInput) (RequestResult, error) {
	if in.WorkItemID == uuid.Nil {
		return RequestResult{}, fmt.Errorf("escalations: work_item_id is required")
	}
	if strings.TrimSpace(in.Reason) == "" {
		return RequestResult{}, fmt.Errorf("escalations: reason is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RequestResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.RequestInTx(ctx, tx, in)
	if err != nil {
		return RequestResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RequestResult{}, err
	}
	return result, nil
}

// RequestInTx records a human escalation in a caller-owned transaction.
func (s *Service) RequestInTx(ctx context.Context, tx pgx.Tx, in RequestInput) (RequestResult, error) {
	if in.WorkItemID == uuid.Nil {
		return RequestResult{}, fmt.Errorf("escalations: work_item_id is required")
	}
	reason := strings.TrimSpace(in.Reason)
	if reason == "" {
		return RequestResult{}, fmt.Errorf("escalations: reason is required")
	}
	summary := strings.TrimSpace(in.Summary)
	if summary == "" {
		summary = reason
	}

	parent, err := scanWorkItem(ctx, tx, in.WorkItemID)
	if err != nil {
		return RequestResult{}, err
	}

	escalationID := deterministicEscalationID(in.WorkItemID, reason, summary)
	humanWorkItemID := deterministicHumanWorkItemID(escalationID)
	if existing, ok, err := existingEscalation(ctx, tx, escalationID); err != nil {
		return RequestResult{}, err
	} else if ok {
		return RequestResult{EscalationID: escalationID, HumanWorkItemID: existing, Fresh: false}, nil
	}
	actorID := &in.Actor.ID
	source := sourceForActor(in.Actor)
	title := "Human attention: " + parent.Title

	escalationEventID, fresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectEscalation,
		SubjectID:    escalationID,
		Kind:         domain.EventEscalationRequested,
		Source:       source,
		ActorTokenID: actorID,
		Payload: map[string]any{
			"work_item_id":        in.WorkItemID,
			"human_work_item_id":  humanWorkItemID,
			"reason":              reason,
			"summary":             summary,
			"origin_state":        parent.State,
			"origin_state_reason": parent.StateReason,
		},
	})
	if err != nil {
		return RequestResult{}, err
	}
	if escalationEventID == uuid.Nil {
		return RequestResult{}, fmt.Errorf("escalations: failed to append escalation event")
	}

	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    humanWorkItemID,
		Kind:         domain.EventWorkItemCreated,
		Source:       source,
		ActorTokenID: actorID,
		Payload: map[string]any{
			"title":                        title,
			"body":                         humanWorkItemBody(parent, reason, summary),
			"state":                        domain.WorkItemCaptured,
			"suggested_convergence_checks": []string{"human_response_recorded"},
			"human_review_status":          domain.HumanReviewBlocked,
		},
	}); err != nil {
		return RequestResult{}, err
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    in.WorkItemID,
		Kind:         domain.EventWorkItemRelationAdded,
		Source:       source,
		ActorTokenID: actorID,
		Payload: map[string]any{
			"parent_id": in.WorkItemID,
			"child_id":  humanWorkItemID,
		},
	}); err != nil {
		return RequestResult{}, err
	}
	if parent.State != domain.WorkItemBlocked {
		if _, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectWorkItem,
			SubjectID:    in.WorkItemID,
			Kind:         domain.EventWorkItemTransitioned,
			Source:       source,
			ActorTokenID: actorID,
			Payload: map[string]any{
				"from":   parent.State,
				"to":     domain.WorkItemBlocked,
				"reason": "human escalation requested: " + reason,
			},
		}); err != nil {
			return RequestResult{}, err
		}
	}

	return RequestResult{EscalationID: escalationID, HumanWorkItemID: humanWorkItemID, Fresh: fresh}, nil
}

type workItemRow struct {
	ID                         uuid.UUID
	Title                      string
	State                      domain.WorkItemState
	StateReason                *string
	SuggestedConvergenceChecks []string
	HumanReviewStatus          domain.HumanReviewStatus
}

func scanWorkItem(ctx context.Context, tx pgx.Tx, id uuid.UUID) (workItemRow, error) {
	var row workItemRow
	var checksJSON []byte
	err := tx.QueryRow(ctx, `
		SELECT id, title, state, state_reason, suggested_convergence_checks, human_review_status
		FROM work_items
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&row.ID, &row.Title, &row.State, &row.StateReason, &checksJSON, &row.HumanReviewStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workItemRow{}, ErrNotFound
		}
		return workItemRow{}, err
	}
	if len(checksJSON) > 0 {
		if err := json.Unmarshal(checksJSON, &row.SuggestedConvergenceChecks); err != nil {
			return workItemRow{}, fmt.Errorf("escalations: decode suggested_convergence_checks: %w", err)
		}
	}
	if row.SuggestedConvergenceChecks == nil {
		row.SuggestedConvergenceChecks = []string{}
	}
	return row, nil
}

func existingEscalation(ctx context.Context, tx pgx.Tx, escalationID uuid.UUID) (uuid.UUID, bool, error) {
	var payloadJSON []byte
	err := tx.QueryRow(ctx, `
		SELECT payload
		FROM events
		WHERE subject_kind = $1 AND subject_id = $2 AND kind = $3
	`, domain.SubjectEscalation, escalationID, domain.EventEscalationRequested).Scan(&payloadJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, false, nil
		}
		return uuid.Nil, false, err
	}
	var payload struct {
		HumanWorkItemID uuid.UUID `json:"human_work_item_id"`
	}
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return uuid.Nil, false, fmt.Errorf("escalations: decode existing escalation payload: %w", err)
	}
	if payload.HumanWorkItemID == uuid.Nil {
		return uuid.Nil, false, fmt.Errorf("escalations: existing escalation missing human_work_item_id")
	}
	return payload.HumanWorkItemID, true, nil
}

func deterministicEscalationID(workItemID uuid.UUID, reason string, summary string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"meristem",
		"escalation",
		workItemID.String(),
		reason,
		summary,
	}, "\x00")))
}

func deterministicHumanWorkItemID(escalationID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"meristem",
		"escalation",
		"human-work-item",
		escalationID.String(),
	}, "\x00")))
}

func humanWorkItemBody(parent workItemRow, reason string, summary string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Escalation requested for work_item %s.\n\n", parent.ID)
	fmt.Fprintf(&b, "Reason: %s\n\n", reason)
	if summary != reason {
		fmt.Fprintf(&b, "Summary: %s\n\n", summary)
	}
	fmt.Fprintf(&b, "Respond by appending a human decision or by moving the original work_item out of blocked once resolved.")
	return b.String()
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceSystem
}
