package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/convergence"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/workitems"
)

const scribeCultivarName = "convergence-scribe"

type scribePassResult struct {
	ScribeCandidatesScanned      int
	ScribeChildrenSpawned        int
	ScribeChildrenAlreadyPresent int
}

type scribeCandidate struct {
	ID    uuid.UUID
	Title string
	Body  string
	State domain.WorkItemState
}

func (w *Worker) scanScribes(ctx context.Context) (scribePassResult, error) {
	candidates, err := w.scribeCandidates(ctx)
	if err != nil {
		return scribePassResult{}, err
	}
	result := scribePassResult{ScribeCandidatesScanned: len(candidates)}
	if len(candidates) == 0 {
		return result, nil
	}
	pending := make([]scribeCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		childID := convergence.ScribeChildID(candidate.ID)
		exists, err := w.scribeChildExists(ctx, candidate.ID, childID)
		if err != nil {
			return result, err
		}
		if exists {
			result.ScribeChildrenAlreadyPresent++
			continue
		}
		pending = append(pending, candidate)
	}
	if len(pending) == 0 {
		return result, nil
	}
	cultivar, err := w.resolveScribeCultivar(ctx)
	if err != nil {
		return result, err
	}
	service := workitems.NewService(w.pool, w.writer)
	actor := domain.Token{Source: domain.SourceSystem}
	if w.actor != nil {
		actor.ID = *w.actor
	}

	for _, candidate := range pending {
		childID := convergence.ScribeChildID(candidate.ID)
		_, fresh, err := service.SpawnChildWithID(ctx, candidate.ID, childID, workitems.CreateInput{
			Title:                      scribeChildTitle(candidate.Title),
			Body:                       scribeChildBody(candidate, cultivar),
			State:                      domain.WorkItemTriaged,
			SuggestedConvergenceChecks: []string{convergence.ScribeChildCheck},
			HumanReviewStatus:          domain.HumanReviewWavedThrough,
			Cultivar:                   cultivar,
			Actor:                      actor,
		})
		if err != nil {
			return result, err
		}
		if fresh {
			result.ScribeChildrenSpawned++
		} else {
			result.ScribeChildrenAlreadyPresent++
		}
	}
	return result, nil
}

func (w *Worker) resolveScribeCultivar(ctx context.Context) (string, error) {
	item, err := registry.NewService(w.pool, nil).GetCultivar(ctx, scribeCultivarName)
	if err != nil {
		if errors.Is(err, registry.ErrUnknownCultivar) {
			return "", err
		}
		return "", fmt.Errorf("resolve scribe cultivar: %w", err)
	}
	return fmt.Sprintf("%s@%d", item.Name, item.Version), nil
}

func (w *Worker) scribeCandidates(ctx context.Context) ([]scribeCandidate, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT id, title, body, state
		FROM work_items
		WHERE state = ANY($1::text[])
			AND human_review_status <> $2
			AND jsonb_array_length(suggested_convergence_checks) = 0
		ORDER BY updated_at ASC
	`, []string{string(domain.WorkItemCaptured), string(domain.WorkItemTriaged)}, string(domain.HumanReviewBlocked))
	if err != nil {
		return nil, fmt.Errorf("query scribe candidates: %w", err)
	}
	defer rows.Close()

	var out []scribeCandidate
	for rows.Next() {
		var c scribeCandidate
		var state string
		if err := rows.Scan(&c.ID, &c.Title, &c.Body, &state); err != nil {
			return nil, fmt.Errorf("scan scribe candidate: %w", err)
		}
		c.State = domain.WorkItemState(state)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scribe candidates: %w", err)
	}
	return out, nil
}

func (w *Worker) scribeChildExists(ctx context.Context, parentID, childID uuid.UUID) (bool, error) {
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM work_item_relations
			WHERE parent_id = $1 AND child_id = $2
		)
	`, parentID, childID).Scan(&exists)
	return exists, err
}

func scribeChildTitle(parentTitle string) string {
	const maxTitle = 120
	title := "Define convergence for: " + strings.TrimSpace(parentTitle)
	if len(title) <= maxTitle {
		return title
	}
	return strings.TrimSpace(title[:maxTitle-3]) + "..."
}

func scribeChildBody(candidate scribeCandidate, cultivar string) string {
	body := strings.TrimSpace(candidate.Body)
	if len(body) > 1200 {
		body = strings.TrimSpace(body[:1200]) + "..."
	}
	contract := map[string]any{
		"event":       domain.EventConvergenceChecksProposed,
		"endpoint":    "/v1/work-items/" + candidate.ID.String() + "/convergence-proposal",
		"proposal_of": convergence.ScribeChildID(candidate.ID),
		"checks":      []string{"cmd:<deterministic command>", "event:<event kind>", convergence.ScribeChildCheck, "human-ack:<owner decision>"},
		"classes":     []string{"machine", "human"},
		"cultivar":    cultivar,
	}
	encoded, _ := json.Marshal(contract)
	return fmt.Sprintf("Parent work_item: %s\nParent state: %s\nParent title: %s\nParent body excerpt:\n%s\n\nProposal contract: %s",
		candidate.ID,
		candidate.State,
		strings.TrimSpace(candidate.Title),
		body,
		string(encoded),
	)
}
