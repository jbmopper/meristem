package worker

import (
	"bytes"
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
	"github.com/jbmopper/meristem/internal/jobqueue"
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
	StateEventID   uuid.UUID
	Cultivar       string
}

type dispatchRoute struct {
	Cultivar   string
	Capability string
}

func (w *Worker) scanDispatch(ctx context.Context) (dispatchPassResult, error) {
	candidates, err := w.dispatchCandidates(ctx)
	if err != nil {
		return dispatchPassResult{}, err
	}
	result := dispatchPassResult{DispatchCandidatesScanned: len(candidates)}
	routes := make(map[string]dispatchRoute)
	for _, candidate := range candidates {
		ref := strings.TrimSpace(candidate.Cultivar)
		explicit := ref != ""
		if ref == "" {
			ref = defaultDispatchCultivarName
		}
		route, ok := routes[ref]
		if !ok {
			route, err = w.resolveDispatchRoute(ctx, ref)
			if err != nil {
				result.DispatchesSkippedMissingCultivar++
				if !explicit {
					return result, err
				}
				continue
			}
			routes[ref] = route
		}
		fresh, err := w.appendDispatch(ctx, candidate, route, dispatchReasonAgentAttentionRequested)
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

func (w *Worker) resolveDispatchRoute(ctx context.Context, ref string) (dispatchRoute, error) {
	item, err := registry.NewService(w.pool, nil).GetCultivarRef(ctx, ref)
	if err != nil {
		if errors.Is(err, registry.ErrUnknownCultivar) {
			return dispatchRoute{}, err
		}
		return dispatchRoute{}, fmt.Errorf("resolve dispatch cultivar: %w", err)
	}
	capability := strings.TrimSpace(item.Profile.DispatchCapability)
	if capability == "" {
		return dispatchRoute{}, fmt.Errorf("resolve dispatch cultivar: %s@%d has no dispatch capability", item.Name, item.Version)
	}
	return dispatchRoute{
		Cultivar:   fmt.Sprintf("%s@%d", item.Name, item.Version),
		Capability: capability,
	}, nil
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
	rows.Close()

	// Do not issue nested pool queries while rows owns a connection: the
	// safety profile permits pool_max_conns=1. Resolve immutable log identity
	// only after the candidate cursor has released its connection.
	for i := range out {
		entry, err := jobqueue.ResolveCurrentStateEntry(ctx, w.pool, out[i].ID)
		if err != nil {
			return nil, fmt.Errorf("resolve dispatch candidate %s state entry: %w", out[i].ID, err)
		}
		if entry.State != out[i].State || !entry.OccurredAt.Equal(out[i].StateEnteredAt) {
			return nil, fmt.Errorf("dispatch candidate %s lifecycle projection disagrees with event log: projection=%s/%s event=%s/%s", out[i].ID, out[i].State, out[i].StateEnteredAt.UTC().Format(time.RFC3339Nano), entry.State, entry.OccurredAt.UTC().Format(time.RFC3339Nano))
		}
		out[i].StateEventID = entry.ID
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

func (w *Worker) appendDispatch(ctx context.Context, candidate dispatchCandidate, route dispatchRoute, reason string) (bool, error) {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Fence the optimistic scan against lifecycle mutation. Work-item
	// transitions take this same row lock before their event append and
	// synchronous projection update, so either this dispatch is durably
	// recorded for the scanned epoch first or the transition wins and this
	// stale candidate becomes an idempotent no-op. ClaimDemand uses the same
	// lock before validating the demand and appending an assignment.
	var currentState string
	var currentEnteredAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT state, state_entered_at
		FROM work_items
		WHERE id = $1
		FOR UPDATE`, candidate.ID).Scan(&currentState, &currentEnteredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock dispatch candidate: %w", err)
	}
	currentEntry, err := jobqueue.ResolveCurrentStateEntry(ctx, tx, candidate.ID)
	if err != nil {
		return false, fmt.Errorf("resolve locked dispatch candidate %s state entry: %w", candidate.ID, err)
	}
	if currentEntry.State != domain.WorkItemState(currentState) || !currentEntry.OccurredAt.Equal(currentEnteredAt) {
		return false, fmt.Errorf("dispatch candidate %s lifecycle projection disagrees with event log: projection=%s/%s event=%s/%s", candidate.ID, currentState, currentEnteredAt.UTC().Format(time.RFC3339Nano), currentEntry.State, currentEntry.OccurredAt.UTC().Format(time.RFC3339Nano))
	}
	if domain.WorkItemState(currentState) != candidate.State ||
		!currentEnteredAt.Equal(candidate.StateEnteredAt) ||
		currentEntry.ID != candidate.StateEventID {
		return false, nil
	}

	// Keep the global assignment lock order used by ClaimInTx: work_items
	// first, then the permanent assignment-state row. A non-null holder is
	// durable ownership even when its wall-clock lease has elapsed but the
	// assignment reducer has not yet appended the expiry release. The existing
	// demand becomes claimable again after that release; minting another
	// same-epoch dispatch while ownership is projected would only create a
	// second generation racing the held assignment.
	var assignmentHeld bool
	err = tx.QueryRow(ctx, `
		SELECT holder_token_id IS NOT NULL
		FROM work_item_assignment_state
		WHERE work_item_id = $1
		FOR UPDATE`, candidate.ID).Scan(&assignmentHeld)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("lock dispatch assignment state: work item %s has no permanent assignment-state projection", candidate.ID)
	}
	if err != nil {
		return false, fmt.Errorf("lock dispatch assignment state: %w", err)
	}
	if assignmentHeld {
		return false, nil
	}

	// Routing metadata for the listener demand envelope: the demanded
	// semantic capability (declared by the cultivar profile) and the
	// ORIGINATING principal — the last non-system principal that advanced
	// this item, falling back to its creator. Listener resolution reads these
	// from the durable event and never trusts caller-supplied routing fields,
	// so "listen to Fable" matches Fable-originated demand even though this
	// event itself is system-authored.
	payload := map[string]any{
		"work_item_id":           candidate.ID,
		"state":                  candidate.State,
		"state_event_id":         candidate.StateEventID,
		"state_entered_at_unix":  candidate.StateEnteredAt.Unix(),
		"cultivar":               route.Cultivar,
		"capability":             route.Capability,
		"reason":                 reason,
		"source_reconciler_pass": "dispatch",
	}
	origin, err := dispatchOrigin(ctx, tx, candidate.ID)
	if err != nil {
		return false, err
	}
	if origin != uuid.Nil {
		payload["origin_token_id"] = origin
	}
	// Dispatch generations form a causal chain. The desired payload without its
	// causal predecessor is the stable value we reconcile. If it already equals
	// the latest valid generation, this pass is an idempotent no-op. Otherwise
	// link the new generation to the latest raw fact. The link distinguishes
	// legitimate A -> B -> A routing cycles, and also gives a malformed latest
	// immutable demand a deterministic repair generation instead of colliding
	// with an older payload-only event id.
	var (
		latestRawID      uuid.UUID
		latestRawPayload []byte
	)
	err = tx.QueryRow(ctx, `
		SELECT id, payload
		FROM events
		WHERE subject_kind = $1 AND subject_id = $2 AND kind = $3
		ORDER BY seq DESC
		LIMIT 1
	`, domain.SubjectWorkItem, candidate.ID, domain.EventDispatchRequested).Scan(&latestRawID, &latestRawPayload)
	if err == nil {
		_, identityErr := jobqueue.ResolveDispatchIdentity(ctx, tx, latestRawID)
		if identityErr != nil && !errors.Is(identityErr, jobqueue.ErrInvalidDispatchDemand) {
			return false, fmt.Errorf("validate latest dispatch before repair: %w", identityErr)
		}
		if identityErr == nil {
			var latestBase map[string]any
			decoder := json.NewDecoder(bytes.NewReader(latestRawPayload))
			decoder.UseNumber()
			if err := decoder.Decode(&latestBase); err != nil {
				return false, fmt.Errorf("decode latest dispatch generation: %w", err)
			}
			delete(latestBase, "supersedes_dispatch_event_id")
			latestCanonical, err := events.CanonicalJSON(latestBase)
			if err != nil {
				return false, fmt.Errorf("canonicalize latest dispatch generation: %w", err)
			}
			desiredCanonical, err := events.CanonicalJSON(payload)
			if err != nil {
				return false, fmt.Errorf("canonicalize desired dispatch generation: %w", err)
			}
			if bytes.Equal(latestCanonical, desiredCanonical) {
				return false, nil
			}
		}
		payload["supersedes_dispatch_event_id"] = latestRawID
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("resolve latest dispatch before append: %w", err)
	}
	_, fresh, err := w.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    candidate.ID,
		Kind:         domain.EventDispatchRequested,
		Source:       domain.SourceSystem,
		ActorTokenID: w.actor,
		Payload:      payload,
	})
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fresh, nil
}

// dispatchOrigin reduces the demand's originating principal from the event
// log: the most recent human- or agent-authored event on the item (its
// current effective author), else the item's creator. uuid.Nil when neither
// exists; listener resolution fails closed on such demand.
func dispatchOrigin(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID) (uuid.UUID, error) {
	var origin *uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT actor_token_id FROM events
			 WHERE subject_kind = $1 AND subject_id = $2
			   AND source <> $3 AND actor_token_id IS NOT NULL
			 ORDER BY seq DESC LIMIT 1),
			(SELECT created_by FROM work_items WHERE id = $2)
		)`, domain.SubjectWorkItem, workItemID, domain.SourceSystem).Scan(&origin)
	if err != nil {
		return uuid.Nil, fmt.Errorf("resolve dispatch origin: %w", err)
	}
	if origin == nil {
		return uuid.Nil, nil
	}
	return *origin, nil
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
	route, err := w.resolveDispatchRoute(ctx, cultivar)
	if err != nil {
		return false, err
	}
	stateEventID, err := dispatchStateEntryID(ctx, w.pool, b.Candidate.ID)
	if err != nil {
		return false, err
	}
	return w.appendDispatch(ctx, dispatchCandidate{
		ID:             b.Candidate.ID,
		State:          b.Candidate.State,
		StateEnteredAt: b.Candidate.StateEnteredAt,
		StateEventID:   stateEventID,
		Cultivar:       route.Cultivar,
	}, route, dispatchReasonAgentAttentionRequested)
}

type dispatchStateEntryQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func dispatchStateEntryID(ctx context.Context, q dispatchStateEntryQuerier, workItemID uuid.UUID) (uuid.UUID, error) {
	var id uuid.UUID
	err := q.QueryRow(ctx, `
		SELECT lifecycle.id
		FROM events lifecycle
		WHERE lifecycle.subject_kind = $2
		  AND lifecycle.subject_id = $1
		  AND lifecycle.kind IN ($3, $4)
		  AND (
		    lifecycle.kind = $3
		    OR (
		      NULLIF(lifecycle.payload->>'to', '') IS NOT NULL
		      AND NULLIF(lifecycle.payload->>'to', '') IS DISTINCT FROM (
		        SELECT CASE
		                 WHEN prior.kind = $3
		                 THEN COALESCE(NULLIF(prior.payload->>'state', ''), 'captured')
		                 ELSE NULLIF(prior.payload->>'to', '')
		               END
		        FROM events prior
		        WHERE prior.subject_kind = lifecycle.subject_kind
		          AND prior.subject_id = lifecycle.subject_id
		          AND prior.kind IN ($3, $4)
		          AND prior.seq < lifecycle.seq
		          AND jsonb_typeof(prior.payload) = 'object'
		        ORDER BY prior.seq DESC
		        LIMIT 1
		      )
		    )
		  )
		ORDER BY lifecycle.seq DESC
		LIMIT 1
	`, workItemID, domain.SubjectWorkItem, domain.EventWorkItemCreated, domain.EventWorkItemTransitioned).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("resolve dispatch state entry for %s: %w", workItemID, err)
	}
	return id, nil
}
