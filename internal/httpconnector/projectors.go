package httpconnector

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(actionRequestedProjector{})
	registry.Register(actionApprovedProjector{})
	registry.Register(actionSentProjector{})
}

type actionRequestedProjector struct{}

func (actionRequestedProjector) Kind() string { return domain.EventHTTPConnectorActionRequested }

func (actionRequestedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectHTTPConnectorAction {
		return fmt.Errorf("http_connector.action_requested: expected subject_kind %q, got %q", domain.SubjectHTTPConnectorAction, event.SubjectKind)
	}
	var payload struct {
		WorkItemID uuid.UUID       `json:"work_item_id"`
		Mode       string          `json:"mode"`
		Method     string          `json:"method"`
		URL        string          `json:"url"`
		Request    json.RawMessage `json:"request"`
		ApprovalID uuid.UUID       `json:"approval_id"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if payload.WorkItemID == uuid.Nil {
		return fmt.Errorf("http_connector.action_requested: work_item_id is required")
	}
	mode := Mode(payload.Mode)
	if mode != ModeRead && mode != ModeWrite {
		return fmt.Errorf("http_connector.action_requested: invalid mode %q", payload.Mode)
	}
	status := StatusRequested
	var approvalID *uuid.UUID
	if mode == ModeWrite {
		if payload.ApprovalID == uuid.Nil {
			return fmt.Errorf("http_connector.action_requested: approval_id is required for writes")
		}
		status = StatusAwaitingApproval
		approvalID = &payload.ApprovalID
	}
	if len(payload.Request) == 0 || string(payload.Request) == "null" {
		payload.Request = []byte(`{}`)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO http_connector_actions (
			id, work_item_id, mode, method, url, request, status, approval_id,
			requested_by, source, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11, $11)
		ON CONFLICT (id) DO NOTHING
	`, event.SubjectID, payload.WorkItemID, payload.Mode, payload.Method, payload.URL, payload.Request,
		status, approvalID, event.ActorTokenID, event.Source, event.OccurredAt)
	return err
}

type actionApprovedProjector struct{}

func (actionApprovedProjector) Kind() string { return domain.EventHTTPConnectorActionApproved }

func (actionApprovedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectHTTPConnectorAction {
		return fmt.Errorf("http_connector.action_approved: expected subject_kind %q, got %q", domain.SubjectHTTPConnectorAction, event.SubjectKind)
	}
	var payload struct {
		WorkItemID uuid.UUID `json:"work_item_id"`
		ApprovalID uuid.UUID `json:"approval_id"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if payload.WorkItemID == uuid.Nil || payload.ApprovalID == uuid.Nil {
		return fmt.Errorf("http_connector.action_approved: work_item_id and approval_id are required")
	}
	raw, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}
	if len(raw) == 0 || string(raw) == "null" {
		raw = []byte(`{}`)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE http_connector_actions
		SET status = 'approved', updated_at = $2
		WHERE id = $1 AND status <> 'sent'
	`, event.SubjectID, event.OccurredAt); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, kind, action_id, state, payload, created_at, updated_at)
		VALUES ($1, 'http_connector.write', $2, 'pending', $3::jsonb, $4, $4)
		ON CONFLICT (id) DO NOTHING
	`, event.ID, event.SubjectID, raw, event.OccurredAt)
	return err
}

type actionSentProjector struct{}

func (actionSentProjector) Kind() string { return domain.EventHTTPConnectorActionSent }

func (actionSentProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectHTTPConnectorAction {
		return fmt.Errorf("http_connector.action_sent: expected subject_kind %q, got %q", domain.SubjectHTTPConnectorAction, event.SubjectKind)
	}
	var payload struct {
		ResponseStatus int    `json:"response_status"`
		ResponseBody   string `json:"response_body"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE http_connector_actions
		SET status = 'sent',
		    response_status = $2,
		    response_body = $3,
		    error = '',
		    updated_at = $4
		WHERE id = $1
	`, event.SubjectID, payload.ResponseStatus, payload.ResponseBody, event.OccurredAt)
	return err
}

func decodePayload(payload any, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
