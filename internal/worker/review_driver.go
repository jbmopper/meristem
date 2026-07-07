package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/workitems"
)

const (
	reviewerCultivarName = "reviewer"
	reviewChildCheck     = "event:review.verdict_recorded"
)

type reviewPassResult struct {
	ReviewCandidatesScanned      int
	ReviewChildrenSpawned        int
	ReviewChildrenAlreadyPresent int
}

type reviewCandidate struct {
	ID    uuid.UUID
	Title string
	Body  string
	State domain.WorkItemState
}

type reviewEvidence struct {
	ImplementationReady bool
	Commits             []string
}

func (w *Worker) scanReviews(ctx context.Context) (reviewPassResult, error) {
	candidates, err := w.reviewCandidates(ctx)
	if err != nil {
		return reviewPassResult{}, err
	}
	result := reviewPassResult{ReviewCandidatesScanned: len(candidates)}
	if len(candidates) == 0 {
		return result, nil
	}
	pending := make([]reviewCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		childID := reviewChildID(candidate.ID)
		exists, err := w.reviewChildExists(ctx, candidate.ID, childID)
		if err != nil {
			return result, err
		}
		if exists {
			result.ReviewChildrenAlreadyPresent++
			continue
		}
		pending = append(pending, candidate)
	}
	if len(pending) == 0 {
		return result, nil
	}
	cultivar, err := w.resolveReviewerCultivar(ctx)
	if err != nil {
		return result, err
	}
	service := workitems.NewService(w.pool, w.writer)
	actor := domain.Token{Source: domain.SourceSystem}
	if w.actor != nil {
		actor.ID = *w.actor
	}

	for _, candidate := range pending {
		evidence, err := w.reviewEvidence(ctx, candidate.ID)
		if err != nil {
			return result, err
		}
		childID := reviewChildID(candidate.ID)
		_, fresh, err := service.SpawnChildWithID(ctx, candidate.ID, childID, workitems.CreateInput{
			Title:                      reviewChildTitle(candidate.Title),
			Body:                       reviewChildBody(candidate, evidence, cultivar),
			State:                      domain.WorkItemTriaged,
			SuggestedConvergenceChecks: []string{reviewChildCheck},
			HumanReviewStatus:          domain.HumanReviewWavedThrough,
			Cultivar:                   cultivar,
			Actor:                      actor,
		})
		if err != nil {
			return result, err
		}
		if fresh {
			result.ReviewChildrenSpawned++
		} else {
			result.ReviewChildrenAlreadyPresent++
		}
	}
	return result, nil
}

func reviewChildID(parentID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(parentID.String()+"|reviewer|v1"))
}

func (w *Worker) resolveReviewerCultivar(ctx context.Context) (string, error) {
	item, err := registry.NewService(w.pool, nil).GetCultivar(ctx, reviewerCultivarName)
	if err != nil {
		if errors.Is(err, registry.ErrUnknownCultivar) {
			return "", err
		}
		return "", fmt.Errorf("resolve reviewer cultivar: %w", err)
	}
	return fmt.Sprintf("%s@%d", item.Name, item.Version), nil
}

func (w *Worker) reviewCandidates(ctx context.Context) ([]reviewCandidate, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT wi.id, wi.title, wi.body, wi.state
		FROM work_items wi
		WHERE wi.state = $1
			AND wi.human_review_status <> $2
			AND EXISTS (
				SELECT 1
				FROM events marker
				WHERE marker.subject_kind = $3
					AND marker.subject_id = wi.id
					AND marker.kind = $4
					AND (
						marker.payload->>'inner_kind' = $5
						OR jsonb_typeof(marker.payload->'inner'->'commits') = 'array'
						OR jsonb_typeof(marker.payload->'commits') = 'array'
					)
			)
			AND NOT EXISTS (
				SELECT 1
				FROM events created
				WHERE created.subject_kind = $3
					AND created.subject_id = wi.id
					AND created.kind = $6
					AND split_part(created.payload->>'cultivar', '@', 1) = $7
			)
		ORDER BY wi.updated_at ASC
	`, string(domain.WorkItemDone), string(domain.HumanReviewBlocked),
		domain.SubjectWorkItem, domain.EventWorkItemEventAppended,
		"coordination.implementation_ready", domain.EventWorkItemCreated, reviewerCultivarName)
	if err != nil {
		return nil, fmt.Errorf("query review candidates: %w", err)
	}
	defer rows.Close()

	var out []reviewCandidate
	for rows.Next() {
		var c reviewCandidate
		var state string
		if err := rows.Scan(&c.ID, &c.Title, &c.Body, &state); err != nil {
			return nil, fmt.Errorf("scan review candidate: %w", err)
		}
		c.State = domain.WorkItemState(state)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate review candidates: %w", err)
	}
	return out, nil
}

func (w *Worker) reviewChildExists(ctx context.Context, parentID, childID uuid.UUID) (bool, error) {
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

func (w *Worker) reviewEvidence(ctx context.Context, parentID uuid.UUID) (reviewEvidence, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT payload
		FROM events
		WHERE subject_kind = $1
			AND subject_id = $2
			AND kind = $3
			AND (
				payload->>'inner_kind' = $4
				OR jsonb_typeof(payload->'inner'->'commits') = 'array'
				OR jsonb_typeof(payload->'commits') = 'array'
			)
		ORDER BY occurred_at ASC
	`, domain.SubjectWorkItem, parentID, domain.EventWorkItemEventAppended, "coordination.implementation_ready")
	if err != nil {
		return reviewEvidence{}, fmt.Errorf("query review evidence for %s: %w", parentID, err)
	}
	defer rows.Close()

	var out reviewEvidence
	seen := map[string]bool{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return reviewEvidence{}, fmt.Errorf("scan review evidence for %s: %w", parentID, err)
		}
		var payload struct {
			InnerKind string          `json:"inner_kind"`
			Inner     json.RawMessage `json:"inner"`
			Commit    string          `json:"commit"`
			Commits   json.RawMessage `json:"commits"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			return reviewEvidence{}, fmt.Errorf("decode review evidence for %s: %w", parentID, err)
		}
		if payload.InnerKind == "coordination.implementation_ready" {
			out.ImplementationReady = true
		}
		for _, commit := range commitRefsFromRaw(payload.Commits) {
			if !seen[commit] {
				seen[commit] = true
				out.Commits = append(out.Commits, commit)
			}
		}
		if commit := strings.TrimSpace(payload.Commit); commit != "" && !seen[commit] {
			seen[commit] = true
			out.Commits = append(out.Commits, commit)
		}
		if len(payload.Inner) > 0 {
			var inner struct {
				Commit  string          `json:"commit"`
				Commits json.RawMessage `json:"commits"`
			}
			if err := json.Unmarshal(payload.Inner, &inner); err == nil {
				for _, commit := range commitRefsFromRaw(inner.Commits) {
					if !seen[commit] {
						seen[commit] = true
						out.Commits = append(out.Commits, commit)
					}
				}
				if commit := strings.TrimSpace(inner.Commit); commit != "" && !seen[commit] {
					seen[commit] = true
					out.Commits = append(out.Commits, commit)
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return reviewEvidence{}, fmt.Errorf("iterate review evidence for %s: %w", parentID, err)
	}
	return out, nil
}

func commitRefsFromRaw(raw json.RawMessage) []string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err == nil {
		return compactCommitRefs(values)
	}
	var anys []any
	if err := json.Unmarshal(raw, &anys); err != nil {
		return nil
	}
	out := make([]string, 0, len(anys))
	for _, item := range anys {
		switch v := item.(type) {
		case string:
			out = append(out, v)
		case map[string]any:
			for _, key := range []string{"sha", "commit", "hash", "ref"} {
				if raw, ok := v[key].(string); ok {
					out = append(out, raw)
					break
				}
			}
		}
	}
	return compactCommitRefs(out)
}

func compactCommitRefs(in []string) []string {
	out := make([]string, 0, len(in))
	for _, item := range in {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func reviewChildTitle(parentTitle string) string {
	const maxTitle = 120
	title := "Review implementation: " + strings.TrimSpace(parentTitle)
	if len(title) <= maxTitle {
		return title
	}
	return strings.TrimSpace(truncateRuneSafe(title, maxTitle-3)) + "..."
}

// truncateRuneSafe cuts s to at most max bytes without splitting a UTF-8
// rune: a raw byte slice can leave an invalid-UTF-8 tail when a multibyte
// rune straddles the boundary.
func truncateRuneSafe(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

func reviewChildBody(candidate reviewCandidate, evidence reviewEvidence, cultivar string) string {
	body := strings.TrimSpace(candidate.Body)
	if len(body) > 1200 {
		body = strings.TrimSpace(truncateRuneSafe(body, 1200)) + "..."
	}
	commits := evidence.Commits
	if len(commits) == 0 {
		commits = []string{"<not provided; inspect implementation_ready evidence>"}
	}
	contract := map[string]any{
		"parent_work_item_id": candidate.ID,
		"review_child_id":     reviewChildID(candidate.ID),
		"verdict_inner_kind":  "review.verdict_recorded",
		"check_signal_kind":   "checklist.item:" + reviewChildCheck,
		"check_signal":        map[string]any{"pass": true},
		"verdicts":            []string{"accepted", "accepted_with_finding", "blocking_finding"},
		"cultivar":            cultivar,
	}
	encoded, _ := json.Marshal(contract)
	return fmt.Sprintf("Parent work_item: %s\nParent state: %s\nParent title: %s\nImplementation marker present: %t\nCommit refs: %s\nParent body excerpt:\n%s\n\nReview contract: %s",
		candidate.ID,
		candidate.State,
		strings.TrimSpace(candidate.Title),
		evidence.ImplementationReady,
		strings.Join(commits, ", "),
		body,
		string(encoded),
	)
}
