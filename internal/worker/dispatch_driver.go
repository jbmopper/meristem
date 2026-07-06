package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/registry"
)

const defaultDispatchCultivarName = "checklist-worker"

const dispatchReasonAgentAttentionRequested = "agent_attention_requested"

type dispatchPassResult struct {
	DispatchCandidatesScanned        int
	DispatchesRequested              int
	DispatchesAlreadyRequested       int
	DispatchesSkippedMissingCultivar int
}

type dispatchCandidate struct {
	ID             uuid.UUID
	State          domain.WorkItemState
	StateEnteredAt time.Time
	Cultivar       string
}

func (w *Worker) scanDispatch(ctx context.Context) (dispatchPassResult, error) {
	candidates, err := w.dispatchCandidates(ctx)
	if err != nil {
		return dispatchPassResult{}, err
	}
	result := dispatchPassResult{DispatchCandidatesScanned: len(candidates)}
	var defaultCultivar string
	for _, candidate := range candidates {
		cultivar := strings.TrimSpace(candidate.Cultivar)
		if cultivar == "" {
			if defaultCultivar == "" {
				defaultCultivar, err = w.resolveDispatchCultivar(ctx)
				if err != nil {
					result.DispatchesSkippedMissingCultivar++
					return result, err
				}
			}
			cultivar = defaultCultivar
		}
		fresh, err := w.appendDispatch(ctx, candidate, cultivar, dispatchReasonAgentAttentionRequested)
		if err != nil {
			return result, err
		}
		if fresh {
			result.DispatchesRequested++
		} else {
			result.DispatchesAlreadyRequested++
		}
	}
	return result, nil
}

func (w *Worker) resolveDispatchCultivar(ctx context.Context) (string, error) {
	item, err := registry.NewService(w.pool, nil).GetCultivar(ctx, defaultDispatchCultivarName)
	if err != nil {
		if errors.Is(err, registry.ErrUnknownCultivar) {
			return "", err
		}
		return "", fmt.Errorf("resolve dispatch cultivar: %w", err)
	}
	return fmt.Sprintf("%s@%d", item.Name, item.Version), nil
}

func (w *Worker) dispatchCandidates(ctx context.Context) ([]dispatchCandidate, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT wi.id, wi.state, wi.state_entered_at, created.payload
		FROM work_items wi
		LEFT JOIN events created
			ON created.subject_kind = $1
			AND created.subject_id = wi.id
			AND created.kind = $2
		WHERE wi.state = ANY($3::text[])
			AND wi.human_review_status <> $4
			AND jsonb_array_length(wi.suggested_convergence_checks) > 0
		ORDER BY wi.updated_at ASC
	`, domain.SubjectWorkItem, domain.EventWorkItemCreated,
		[]string{string(domain.WorkItemCaptured), string(domain.WorkItemTriaged), string(domain.WorkItemPlanned)},
		string(domain.HumanReviewBlocked))
	if err != nil {
		return nil, fmt.Errorf("query dispatch candidates: %w", err)
	}
	defer rows.Close()

	var out []dispatchCandidate
	for rows.Next() {
		var c dispatchCandidate
		var state string
		var createdPayload []byte
		if err := rows.Scan(&c.ID, &state, &c.StateEnteredAt, &createdPayload); err != nil {
			return nil, fmt.Errorf("scan dispatch candidate: %w", err)
		}
		c.State = domain.WorkItemState(state)
		c.Cultivar = dispatchCultivarFromCreatedPayload(createdPayload)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dispatch candidates: %w", err)
	}
	return out, nil
}

func dispatchCultivarFromCreatedPayload(raw []byte) string {
	var payload struct {
		Cultivar string `json:"cultivar"`
	}
	if len(raw) == 0 {
		return ""
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.Cultivar)
}

func (w *Worker) appendDispatch(ctx context.Context, candidate dispatchCandidate, cultivar string, reason string) (bool, error) {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, fresh, err := w.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    candidate.ID,
		Kind:         domain.EventDispatchRequested,
		Source:       domain.SourceSystem,
		ActorTokenID: w.actor,
		Payload: map[string]any{
			"work_item_id":           candidate.ID,
			"state":                  candidate.State,
			"state_entered_at_unix":  candidate.StateEnteredAt.Unix(),
			"cultivar":               cultivar,
			"reason":                 reason,
			"source_reconciler_pass": "dispatch",
		},
	})
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fresh, nil
}

func shouldRoutePatienceBreachToDispatch(b Breach) bool {
	if !preClaimDispatchState(b.Candidate.State) {
		return false
	}
	cultivar := strings.TrimSpace(b.Candidate.Cultivar)
	if cultivar == "" {
		return false
	}
	return !isHumanAttentionCultivarRef(cultivar)
}

func preClaimDispatchState(state domain.WorkItemState) bool {
	switch state {
	case domain.WorkItemCaptured, domain.WorkItemTriaged, domain.WorkItemPlanned:
		return true
	default:
		return false
	}
}

func isHumanAttentionCultivarRef(ref string) bool {
	name := strings.TrimSpace(ref)
	if before, _, ok := strings.Cut(name, "@"); ok {
		name = before
	}
	return name == "human-attention"
}

func (w *Worker) dispatchPatienceBreach(ctx context.Context, b Breach) (bool, error) {
	cultivar := strings.TrimSpace(b.Candidate.Cultivar)
	if cultivar == "" {
		return false, errors.New("dispatch patience breach: cultivar is required")
	}
	return w.appendDispatch(ctx, dispatchCandidate{
		ID:             b.Candidate.ID,
		State:          b.Candidate.State,
		StateEnteredAt: b.Candidate.StateEnteredAt,
		Cultivar:       cultivar,
	}, cultivar, dispatchReasonAgentAttentionRequested)
}
