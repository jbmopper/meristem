package workitems

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/idempotency"
)

var (
	ErrNotFound = errors.New("workitems: not found")
	// ErrRelationCycle is returned when adding the parent->child edge
	// would close a cycle in the work_item DAG. The migration's CHECK
	// constraint blocks the self-loop case at the row level; everything
	// deeper has to be enforced here, before the relation event is
	// appended, because the projector trusts the writer.
	ErrRelationCycle = errors.New("workitems: relation would create a cycle")
)

type Service struct {
	pool   *pgxpool.Pool
	writer *events.Writer
}

func NewService(pool *pgxpool.Pool, writer *events.Writer) *Service {
	return &Service{pool: pool, writer: writer}
}

type CreateInput struct {
	Title                      string
	Body                       string
	Actor                      domain.Token
	State                      domain.WorkItemState
	SuggestedConvergenceChecks []string
	HumanReviewStatus          domain.HumanReviewStatus
}

type UpdateMetadataInput struct {
	SuggestedConvergenceChecks []string
	HumanReviewStatus          domain.HumanReviewStatus
	Actor                      domain.Token
}

func (s *Service) Create(ctx context.Context, in CreateInput) (domain.WorkItem, error) {
	if strings.TrimSpace(in.Title) == "" {
		return domain.WorkItem{}, fmt.Errorf("workitems: title is required")
	}
	state := in.State
	if state == "" {
		state = domain.WorkItemCaptured
	}
	if !state.Valid() {
		return domain.WorkItem{}, fmt.Errorf("workitems: invalid state %q", state)
	}
	checks, err := normalizeSuggestedConvergenceChecks(in.SuggestedConvergenceChecks)
	if err != nil {
		return domain.WorkItem{}, err
	}
	humanReview, err := normalizeHumanReviewStatus(in.HumanReviewStatus)
	if err != nil {
		return domain.WorkItem{}, err
	}
	id := newSubjectID(ctx, "work_item")
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItem{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    id,
		Kind:         domain.EventWorkItemCreated,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload: map[string]any{
			"title":                        in.Title,
			"body":                         in.Body,
			"state":                        state,
			"suggested_convergence_checks": checks,
			"human_review_status":          humanReview,
		},
	}); err != nil {
		return domain.WorkItem{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItem{}, err
	}
	return s.Get(ctx, id)
}

func (s *Service) SpawnChild(ctx context.Context, parentID uuid.UUID, in CreateInput) (domain.WorkItem, error) {
	if strings.TrimSpace(in.Title) == "" {
		return domain.WorkItem{}, fmt.Errorf("workitems: title is required")
	}
	state := in.State
	if state == "" {
		state = domain.WorkItemCaptured
	}
	if !state.Valid() {
		return domain.WorkItem{}, fmt.Errorf("workitems: invalid state %q", state)
	}
	checks, err := normalizeSuggestedConvergenceChecks(in.SuggestedConvergenceChecks)
	if err != nil {
		return domain.WorkItem{}, err
	}
	humanReview, err := normalizeHumanReviewStatus(in.HumanReviewStatus)
	if err != nil {
		return domain.WorkItem{}, err
	}
	childID := newSubjectID(ctx, "child_work_item")
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItem{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanWorkItem(ctx, tx, parentID); err != nil {
		return domain.WorkItem{}, err
	}
	// Self-loops are blocked by the work_item_relations CHECK
	// constraint, but check explicitly here for a clearer error than the
	// generic constraint violation pgx would surface.
	if childID == parentID {
		return domain.WorkItem{}, ErrRelationCycle
	}
	cycle, err := childIsAncestorOf(ctx, tx, childID, parentID)
	if err != nil {
		return domain.WorkItem{}, err
	}
	if cycle {
		return domain.WorkItem{}, ErrRelationCycle
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    childID,
		Kind:         domain.EventWorkItemCreated,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload: map[string]any{
			"title":                        in.Title,
			"body":                         in.Body,
			"state":                        state,
			"suggested_convergence_checks": checks,
			"human_review_status":          humanReview,
		},
	}); err != nil {
		return domain.WorkItem{}, err
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    parentID,
		Kind:         domain.EventWorkItemRelationAdded,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload: map[string]any{
			"parent_id": parentID,
			"child_id":  childID,
		},
	}); err != nil {
		return domain.WorkItem{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItem{}, err
	}
	return s.Get(ctx, childID)
}

func (s *Service) UpdateMetadata(ctx context.Context, id uuid.UUID, in UpdateMetadataInput) (domain.WorkItem, error) {
	checks, err := normalizeSuggestedConvergenceChecks(in.SuggestedConvergenceChecks)
	if err != nil {
		return domain.WorkItem{}, err
	}
	humanReview, err := normalizeHumanReviewStatus(in.HumanReviewStatus)
	if err != nil {
		return domain.WorkItem{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItem{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, err := scanWorkItem(ctx, tx, id)
	if err != nil {
		return domain.WorkItem{}, err
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     id,
		Kind:          domain.EventWorkItemMetadataUpdated,
		Source:        sourceForActor(in.Actor),
		ActorTokenID:  &in.Actor.ID,
		Discriminator: eventDiscriminator(ctx),
		Payload: map[string]any{
			"from": map[string]any{
				"suggested_convergence_checks": current.SuggestedConvergenceChecks,
				"human_review_status":          current.HumanReviewStatus,
			},
			"to": map[string]any{
				"suggested_convergence_checks": checks,
				"human_review_status":          humanReview,
			},
		},
	}); err != nil {
		return domain.WorkItem{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItem{}, err
	}
	return s.Get(ctx, id)
}

func (s *Service) Transition(ctx context.Context, id uuid.UUID, to domain.WorkItemState, reason string, actor domain.Token) (domain.WorkItem, error) {
	if !to.Valid() {
		return domain.WorkItem{}, fmt.Errorf("workitems: invalid state %q", to)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItem{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, err := scanWorkItem(ctx, tx, id)
	if err != nil {
		return domain.WorkItem{}, err
	}
	if !domain.CanTransition(current.State, to) {
		return domain.WorkItem{}, fmt.Errorf("workitems: invalid transition from %s to %s", current.State, to)
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     id,
		Kind:          domain.EventWorkItemTransitioned,
		Source:        sourceForActor(actor),
		ActorTokenID:  &actor.ID,
		Discriminator: eventDiscriminator(ctx),
		Payload: map[string]any{
			"from":   current.State,
			"to":     to,
			"reason": reason,
		},
	}); err != nil {
		return domain.WorkItem{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItem{}, err
	}
	return s.Get(ctx, id)
}

func (s *Service) AppendEvent(ctx context.Context, id uuid.UUID, innerKind string, payload any, actor domain.Token) error {
	if strings.TrimSpace(innerKind) == "" {
		return fmt.Errorf("workitems: event kind is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := scanWorkItem(ctx, tx, id); err != nil {
		return err
	}
	_, _, err = s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     id,
		Kind:          domain.EventWorkItemEventAppended,
		Source:        sourceForActor(actor),
		ActorTokenID:  &actor.ID,
		Discriminator: eventDiscriminator(ctx),
		Payload: map[string]any{
			"inner_kind": innerKind,
			"inner":      payload,
		},
	})
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// childIsAncestorOf reports whether `child` already appears as an
// ancestor of `parent` in the existing work_item_relations graph. If it
// does, adding the parent->child edge would close a cycle.
//
// The walk runs inside the calling transaction so it sees uncommitted
// edges from the same SpawnChild call (defense against pathological
// reentrancy) and so it observes a consistent ancestry under serializable
// or repeatable-read isolation. The recursive CTE is bounded by the
// finite size of the DAG; a runaway is impossible because each step
// follows a foreign-keyed edge that can't form a Postgres-level cycle
// (we're the ones blocking that).
func childIsAncestorOf(ctx context.Context, tx pgx.Tx, child, parent uuid.UUID) (bool, error) {
	const q = `
WITH RECURSIVE ancestry(id) AS (
    SELECT parent_id FROM work_item_relations WHERE child_id = $1
    UNION
    SELECT r.parent_id FROM work_item_relations r JOIN ancestry a ON r.child_id = a.id
)
SELECT EXISTS (SELECT 1 FROM ancestry WHERE id = $2)`
	var found bool
	if err := tx.QueryRow(ctx, q, parent, child).Scan(&found); err != nil {
		return false, fmt.Errorf("workitems: ancestry walk: %w", err)
	}
	return found, nil
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}

func normalizeSuggestedConvergenceChecks(in []string) ([]string, error) {
	out := make([]string, 0, len(in))
	for i, check := range in {
		trimmed := strings.TrimSpace(check)
		if trimmed == "" {
			return nil, fmt.Errorf("workitems: suggested_convergence_checks[%d] is blank", i)
		}
		out = append(out, trimmed)
	}
	if out == nil {
		return []string{}, nil
	}
	return out, nil
}

func marshalSuggestedConvergenceChecks(checks []string) (string, error) {
	encoded, err := json.Marshal(checks)
	if err != nil {
		return "", fmt.Errorf("workitems: encode suggested_convergence_checks: %w", err)
	}
	return string(encoded), nil
}

func normalizeHumanReviewStatus(status domain.HumanReviewStatus) (domain.HumanReviewStatus, error) {
	if status == "" {
		return domain.HumanReviewWavedThrough, nil
	}
	if !status.Valid() {
		return "", fmt.Errorf("workitems: invalid human_review_status %q", status)
	}
	return status, nil
}

// eventDiscriminator distinguishes distinct logical actions whose event
// payloads can legitimately repeat (transition cycles, duplicate progress
// payloads, metadata toggles). Under the idempotency contract it is the
// caller's (token, scope, key) identity: stable across retries, distinct
// across actions. Callers outside that contract (seed, internal system
// writes) get payload-only identity, matching pre-discriminator behavior.
func eventDiscriminator(ctx context.Context) string {
	disc, _ := idempotency.EventDiscriminator(ctx)
	return disc
}

func newSubjectID(ctx context.Context, label string) uuid.UUID {
	if id, ok := idempotency.SubjectID(ctx, label); ok {
		return id
	}
	return uuid.New()
}

func (s *Service) List(ctx context.Context, state string, limit int) ([]domain.WorkItem, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	args := []any{limit}
	query := `SELECT id, title, body, state, state_reason, suggested_convergence_checks, human_review_status, created_by, created_at, updated_at FROM work_items`
	if state != "" {
		query += ` WHERE state = $2`
		args = append(args, state)
	}
	query += ` ORDER BY updated_at DESC LIMIT $1`
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.WorkItem
	for rows.Next() {
		item, err := scanWorkItemRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.WorkItem, error) {
	return scanWorkItem(ctx, s.pool, id)
}

type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanWorkItem(ctx context.Context, q queryer, id uuid.UUID) (domain.WorkItem, error) {
	row := q.QueryRow(ctx, `SELECT id, title, body, state, state_reason, suggested_convergence_checks, human_review_status, created_by, created_at, updated_at FROM work_items WHERE id = $1`, id)
	return scanWorkItemRow(row)
}

func scanWorkItemRow(row rowScanner) (domain.WorkItem, error) {
	var item domain.WorkItem
	var state string
	var humanReview string
	var checksJSON []byte
	if err := row.Scan(&item.ID, &item.Title, &item.Body, &state, &item.StateReason, &checksJSON, &humanReview, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.WorkItem{}, ErrNotFound
		}
		return domain.WorkItem{}, err
	}
	if len(checksJSON) > 0 {
		if err := json.Unmarshal(checksJSON, &item.SuggestedConvergenceChecks); err != nil {
			return domain.WorkItem{}, fmt.Errorf("workitems: decode suggested_convergence_checks: %w", err)
		}
	}
	if item.SuggestedConvergenceChecks == nil {
		item.SuggestedConvergenceChecks = []string{}
	}
	item.State = domain.WorkItemState(state)
	item.HumanReviewStatus = domain.HumanReviewStatus(humanReview)
	return item, nil
}
