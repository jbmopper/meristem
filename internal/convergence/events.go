package convergence

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
)

// VerdictEventSpec returns the canonical event spec for a persisted
// convergence reduction. The subject_id is the judged work_item id; attempt is
// part of the payload, so the events writer's deterministic id naturally
// dedupes replay of the same reduction and separates genuine retries.
func VerdictEventSpec(source domain.Source, actorTokenID *uuid.UUID, workItemID uuid.UUID, reduction Reduction) (events.Spec, error) {
	if workItemID == uuid.Nil {
		return events.Spec{}, errors.New("convergence: work_item_id is required")
	}
	if !source.Valid() {
		return events.Spec{}, fmt.Errorf("convergence: source %q is invalid", source)
	}
	payload := reduction.EventPayload()
	if _, err := decodeVerdictRecordedPayload(payload); err != nil {
		return events.Spec{}, err
	}
	return events.Spec{
		SubjectKind:  domain.SubjectConvergence,
		SubjectID:    workItemID,
		Kind:         domain.EventConvergenceVerdictRecorded,
		Source:       source,
		ActorTokenID: actorTokenID,
		Payload:      payload,
	}, nil
}

// AppendVerdict appends a convergence.verdict_recorded event through the
// normal events writer. It never writes the projection table directly; the
// registered projector derives convergence_verdicts in the same transaction.
func AppendVerdict(ctx context.Context, tx pgx.Tx, writer *events.Writer, source domain.Source, actorTokenID *uuid.UUID, workItemID uuid.UUID, reduction Reduction) (uuid.UUID, bool, error) {
	if writer == nil {
		return uuid.Nil, false, errors.New("convergence: nil event writer")
	}
	spec, err := VerdictEventSpec(source, actorTokenID, workItemID, reduction)
	if err != nil {
		return uuid.Nil, false, err
	}
	return writer.Append(ctx, tx, spec)
}
