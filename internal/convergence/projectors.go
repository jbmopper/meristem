package convergence

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/projections"
)

// RegisterProjectors adds convergence projection writers to the application
// registry.
func RegisterProjectors(registry *projections.Registry) {
	registry.Register(verdictRecordedProjector{})
}

type verdictRecordedProjector struct{}

func (verdictRecordedProjector) Kind() string { return domain.EventConvergenceVerdictRecorded }

func (verdictRecordedProjector) Apply(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	if event.SubjectKind != domain.SubjectConvergence {
		return fmt.Errorf("convergence.verdict_recorded: expected subject_kind %q, got %q", domain.SubjectConvergence, event.SubjectKind)
	}
	if event.SubjectID == uuid.Nil {
		return fmt.Errorf("convergence.verdict_recorded: subject_id/work_item_id is required")
	}
	payload, err := decodeVerdictRecordedPayload(event.Payload)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO convergence_verdicts (
			event_id, work_item_id, reducer_identity, reducer_version,
			attempt, inputs_digest, disposition, reason, signals,
			actor_token_id, source, occurred_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12)
		ON CONFLICT (event_id) DO UPDATE SET
			work_item_id = EXCLUDED.work_item_id,
			reducer_identity = EXCLUDED.reducer_identity,
			reducer_version = EXCLUDED.reducer_version,
			attempt = EXCLUDED.attempt,
			inputs_digest = EXCLUDED.inputs_digest,
			disposition = EXCLUDED.disposition,
			reason = EXCLUDED.reason,
			signals = EXCLUDED.signals,
			actor_token_id = EXCLUDED.actor_token_id,
			source = EXCLUDED.source,
			occurred_at = EXCLUDED.occurred_at
	`, event.ID, event.SubjectID, payload.ReducerIdentity, payload.ReducerVersion,
		payload.Attempt, payload.InputsDigest, string(payload.Verdict.Disposition),
		payload.Verdict.Reason, []byte(payload.Signals), event.ActorTokenID,
		string(event.Source), event.OccurredAt)
	if err != nil {
		return fmt.Errorf("convergence.verdict_recorded: insert projection: %w", err)
	}
	return nil
}

type verdictRecordedPayload struct {
	ReducerIdentity string          `json:"reducer_identity"`
	ReducerVersion  int             `json:"reducer_version"`
	Attempt         int             `json:"attempt"`
	InputsDigest    string          `json:"inputs_digest"`
	Verdict         Verdict         `json:"verdict"`
	Signals         json.RawMessage `json:"signals"`
}

func decodeVerdictRecordedPayload(raw any) (verdictRecordedPayload, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: marshal payload: %w", err)
	}
	var p verdictRecordedPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: unmarshal payload: %w", err)
	}
	switch {
	case p.ReducerIdentity == "":
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: reducer_identity is required")
	case p.ReducerVersion < 1:
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: reducer_version must be >= 1")
	case p.Attempt < 1:
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: attempt must be >= 1")
	case p.InputsDigest == "":
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: inputs_digest is required")
	case !p.Verdict.Disposition.Valid():
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: invalid disposition %q", p.Verdict.Disposition)
	case p.Verdict.Reason == "":
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: verdict.reason is required")
	}
	if len(p.InputsDigest) != 64 {
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: inputs_digest must be 64 hex characters")
	}
	if _, err := hex.DecodeString(p.InputsDigest); err != nil {
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: inputs_digest must be hex: %w", err)
	}
	if len(p.Signals) == 0 || !json.Valid(p.Signals) {
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: signals must be valid JSON")
	}
	var signals []json.RawMessage
	if err := json.Unmarshal(p.Signals, &signals); err != nil {
		return verdictRecordedPayload{}, fmt.Errorf("convergence.verdict_recorded: signals must be a JSON array: %w", err)
	}
	return p, nil
}
