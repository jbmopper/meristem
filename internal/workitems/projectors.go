package workitems

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
	registry.Register(createdProjector{})
	registry.Register(transitionedProjector{})
	registry.Register(eventAppendedProjector{})
	registry.Register(checksProposedProjector{})
	registry.Register(relationAddedProjector{})
	registry.Register(metadataUpdatedProjector{})
}

type createdProjector struct{}

func (createdProjector) Kind() string { return domain.EventWorkItemCreated }

func (createdProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		Title                      string                   `json:"title"`
		Body                       string                   `json:"body"`
		State                      string                   `json:"state"`
		SuggestedConvergenceChecks []string                 `json:"suggested_convergence_checks"`
		HumanReviewStatus          domain.HumanReviewStatus `json:"human_review_status"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if payload.Title == "" {
		return fmt.Errorf("work_item.created: title is required")
	}
	state := payload.State
	if state == "" {
		state = string(domain.WorkItemCaptured)
	}
	checks, err := normalizeSuggestedConvergenceChecks(payload.SuggestedConvergenceChecks)
	if err != nil {
		return err
	}
	checksJSON, err := marshalSuggestedConvergenceChecks(checks)
	if err != nil {
		return err
	}
	humanReview, err := normalizeHumanReviewStatus(payload.HumanReviewStatus)
	if err != nil {
		return err
	}
	// DO NOTHING on conflict for the same reason as
	// internal/auth/projectors.go: the events writer fires projectors only
	// on a fresh event-row insert, so a duplicate work_item id reaching
	// this statement means a real bug (two distinct work_item.created
	// events claiming the same subject_id) or a rebuild-time replay where
	// the projection table is empty. DO UPDATE would silently mask the
	// bug case.
	_, err = tx.Exec(ctx, `
		INSERT INTO work_items (
			id, title, body, state, suggested_convergence_checks,
			human_review_status, created_by, created_at, state_entered_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $8, $8)
		ON CONFLICT (id) DO NOTHING
	`, event.SubjectID, payload.Title, payload.Body, state, checksJSON, humanReview, event.ActorTokenID, event.OccurredAt)
	return err
}

type transitionedProjector struct{}

func (transitionedProjector) Kind() string { return domain.EventWorkItemTransitioned }

func (transitionedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		From   string `json:"from"`
		To     string `json:"to"`
		Reason string `json:"reason"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if payload.To == "" {
		return fmt.Errorf("work_item.transitioned: to is required")
	}
	_, err := tx.Exec(ctx, `
		UPDATE work_items
		SET state = $2,
		    state_reason = NULLIF($3, ''),
		    state_entered_at = CASE
		        WHEN NULLIF($5, '') IS NULL OR $5 <> $2 THEN $4
		        ELSE state_entered_at
		    END,
		    updated_at = $4
		WHERE id = $1
	`, event.SubjectID, payload.To, payload.Reason, event.OccurredAt, payload.From)
	return err
}

type eventAppendedProjector struct{}

func (eventAppendedProjector) Kind() string { return domain.EventWorkItemEventAppended }

func (eventAppendedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	_, err := tx.Exec(ctx, `UPDATE work_items SET updated_at = $2 WHERE id = $1`, event.SubjectID, event.OccurredAt)
	return err
}

type checksProposedProjector struct{}

func (checksProposedProjector) Kind() string { return domain.EventConvergenceChecksProposed }

func (checksProposedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectWorkItem {
		return fmt.Errorf("convergence.checks_proposed: expected subject_kind %q, got %q", domain.SubjectWorkItem, event.SubjectKind)
	}
	_, err := tx.Exec(ctx, `UPDATE work_items SET updated_at = $2 WHERE id = $1`, event.SubjectID, event.OccurredAt)
	return err
}

type relationAddedProjector struct{}

func (relationAddedProjector) Kind() string { return domain.EventWorkItemRelationAdded }

func (relationAddedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		ParentID uuid.UUID `json:"parent_id"`
		ChildID  uuid.UUID `json:"child_id"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if payload.ParentID == uuid.Nil || payload.ChildID == uuid.Nil {
		return fmt.Errorf("work_item.relation_added: parent_id and child_id are required")
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO work_item_relations (parent_id, child_id, created_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (parent_id, child_id) DO NOTHING
	`, payload.ParentID, payload.ChildID, event.OccurredAt)
	return err
}

type metadataUpdatedProjector struct{}

func (metadataUpdatedProjector) Kind() string { return domain.EventWorkItemMetadataUpdated }

func (metadataUpdatedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	var payload struct {
		To struct {
			SuggestedConvergenceChecks []string                 `json:"suggested_convergence_checks"`
			HumanReviewStatus          domain.HumanReviewStatus `json:"human_review_status"`
		} `json:"to"`
	}
	if err := decodePayload(event.Payload, &payload); err != nil {
		return err
	}
	checks, err := normalizeSuggestedConvergenceChecks(payload.To.SuggestedConvergenceChecks)
	if err != nil {
		return err
	}
	checksJSON, err := marshalSuggestedConvergenceChecks(checks)
	if err != nil {
		return err
	}
	humanReview, err := normalizeHumanReviewStatus(payload.To.HumanReviewStatus)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE work_items
		SET suggested_convergence_checks = $2::jsonb,
		    human_review_status = $3,
		    updated_at = $4
		WHERE id = $1
	`, event.SubjectID, checksJSON, humanReview, event.OccurredAt)
	return err
}

func decodePayload(payload any, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
