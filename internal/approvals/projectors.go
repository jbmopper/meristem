package approvals

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(createdProjector{})
	registry.Register(decidedProjector{})
	registry.Register(expiredProjector{})
}

type createdProjector struct{}

func (createdProjector) Kind() string { return domain.EventApprovalCreated }

func (createdProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectApproval {
		return fmt.Errorf("approval.created: expected subject_kind %q, got %q", domain.SubjectApproval, event.SubjectKind)
	}
	var payload struct {
		WorkItemID uuid.UUID       `json:"work_item_id"`
		Summary    string          `json:"summary"`
		Request    json.RawMessage `json:"request"`
		ExpiresAt  string          `json:"expires_at"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if payload.WorkItemID == uuid.Nil {
		return fmt.Errorf("approval.created: work_item_id is required")
	}
	if payload.Summary == "" {
		return fmt.Errorf("approval.created: summary is required")
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, payload.ExpiresAt)
	if err != nil {
		return fmt.Errorf("approval.created: parse expires_at: %w", err)
	}
	if len(payload.Request) == 0 || string(payload.Request) == "null" {
		payload.Request = []byte(`{}`)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO approvals (
			id, work_item_id, status, summary, request, requested_by,
			requested_source, created_at, expires_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $8)
		ON CONFLICT (id) DO NOTHING
	`, event.SubjectID, payload.WorkItemID, StatusPending, payload.Summary, payload.Request,
		event.ActorTokenID, event.Source, event.OccurredAt, expiresAt)
	return err
}

type decidedProjector struct{}

func (decidedProjector) Kind() string { return domain.EventApprovalDecided }

func (decidedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectApproval {
		return fmt.Errorf("approval.decided: expected subject_kind %q, got %q", domain.SubjectApproval, event.SubjectKind)
	}
	var payload struct {
		Decision string `json:"decision"`
		Reason   string `json:"reason"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	switch Decision(payload.Decision) {
	case DecisionApproved, DecisionDenied:
	default:
		return fmt.Errorf("approval.decided: invalid decision %q", payload.Decision)
	}
	_, err := tx.Exec(ctx, `
		UPDATE approvals
		SET status = $2,
		    decided_by = $3,
		    decision_source = $4,
		    decided_at = $5,
		    decision = $2,
		    decision_reason = $6,
		    updated_at = $5
		WHERE id = $1
	`, event.SubjectID, payload.Decision, event.ActorTokenID, event.Source, event.OccurredAt, payload.Reason)
	return err
}

type expiredProjector struct{}

func (expiredProjector) Kind() string { return domain.EventApprovalExpired }

func (expiredProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectApproval {
		return fmt.Errorf("approval.expired: expected subject_kind %q, got %q", domain.SubjectApproval, event.SubjectKind)
	}
	var payload struct {
		Reason string `json:"reason"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE approvals
		SET status = $2,
		    decided_by = $3,
		    decision_source = $4,
		    decided_at = $5,
		    decision = $2,
		    decision_reason = $6,
		    updated_at = $5
		WHERE id = $1
	`, event.SubjectID, DecisionExpired, event.ActorTokenID, event.Source, event.OccurredAt, payload.Reason)
	return err
}

func decodePayload(payload any, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
