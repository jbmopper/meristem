package errorreporting

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

func RegisterProjectors(registry *projections.Registry) {
	registry.Register(reportedProjector{})
	registry.Register(maskedProjector{})
	registry.Register(unmaskedProjector{})
}

type reportedProjector struct{}

func (reportedProjector) Kind() string { return domain.EventDeterministicErrorReported }

func (reportedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectDeterministicError {
		return fmt.Errorf("deterministic_error.reported: expected subject_kind %q, got %q", domain.SubjectDeterministicError, event.SubjectKind)
	}
	payload, err := decodeReportedPayload(event.Payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO deterministic_errors (
			id, component, code, message, severity, details,
			reported_by, reported_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
		ON CONFLICT (id) DO NOTHING
	`, event.SubjectID, payload.Component, payload.Code, payload.Message, payload.Severity, []byte(payload.Details), event.ActorTokenID, event.OccurredAt)
	return err
}

type maskedProjector struct{}

func (maskedProjector) Kind() string { return domain.EventDeterministicErrorMasked }

func (maskedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectDeterministicError {
		return fmt.Errorf("deterministic_error.masked: expected subject_kind %q, got %q", domain.SubjectDeterministicError, event.SubjectKind)
	}
	reason, err := decodeReasonPayload(event.Payload, "deterministic_error.masked")
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE deterministic_errors
		SET masked = TRUE,
		    mask_reason = NULLIF($2, ''),
		    masked_by = $3,
		    masked_at = $4,
		    updated_at = $4
		WHERE id = $1
	`, event.SubjectID, reason, event.ActorTokenID, event.OccurredAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("deterministic_error.masked: no deterministic_errors row for %s", event.SubjectID)
	}
	return nil
}

type unmaskedProjector struct{}

func (unmaskedProjector) Kind() string { return domain.EventDeterministicErrorUnmasked }

func (unmaskedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectDeterministicError {
		return fmt.Errorf("deterministic_error.unmasked: expected subject_kind %q, got %q", domain.SubjectDeterministicError, event.SubjectKind)
	}
	if _, err := decodeReasonPayload(event.Payload, "deterministic_error.unmasked"); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE deterministic_errors
		SET masked = FALSE,
		    mask_reason = NULL,
		    masked_by = NULL,
		    masked_at = NULL,
		    updated_at = $2
		WHERE id = $1
	`, event.SubjectID, event.OccurredAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("deterministic_error.unmasked: no deterministic_errors row for %s", event.SubjectID)
	}
	return nil
}

type reportedPayload struct {
	Component string                            `json:"component"`
	Code      string                            `json:"code"`
	Message   string                            `json:"message"`
	Severity  domain.DeterministicErrorSeverity `json:"severity"`
	Details   json.RawMessage                   `json:"details"`
}

func decodeReportedPayload(raw any) (reportedPayload, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return reportedPayload{}, fmt.Errorf("deterministic_error.reported: marshal payload: %w", err)
	}
	var p reportedPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return reportedPayload{}, fmt.Errorf("deterministic_error.reported: unmarshal payload: %w", err)
	}
	switch {
	case p.Component == "":
		return reportedPayload{}, fmt.Errorf("deterministic_error.reported: component is required")
	case p.Code == "":
		return reportedPayload{}, fmt.Errorf("deterministic_error.reported: code is required")
	case p.Message == "":
		return reportedPayload{}, fmt.Errorf("deterministic_error.reported: message is required")
	case !p.Severity.Valid():
		return reportedPayload{}, fmt.Errorf("deterministic_error.reported: invalid severity %q", p.Severity)
	case len(p.Details) == 0 || !json.Valid(p.Details):
		return reportedPayload{}, fmt.Errorf("deterministic_error.reported: details must be valid JSON")
	}
	var details any
	if err := json.Unmarshal(p.Details, &details); err != nil {
		return reportedPayload{}, fmt.Errorf("deterministic_error.reported: details must be valid JSON: %w", err)
	}
	if _, ok := details.(map[string]any); !ok {
		return reportedPayload{}, fmt.Errorf("deterministic_error.reported: details must be a JSON object")
	}
	return p, nil
}

func decodeReasonPayload(raw any, kind string) (string, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return "", fmt.Errorf("%s: marshal payload: %w", kind, err)
	}
	var p struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return "", fmt.Errorf("%s: unmarshal payload: %w", kind, err)
	}
	return p.Reason, nil
}
