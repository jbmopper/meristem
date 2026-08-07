package workitems

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/events"
	"github.com/jbmopper/meristem/internal/feed"
	"github.com/jbmopper/meristem/internal/idempotency"
	"github.com/jbmopper/meristem/internal/registry"
	"github.com/jbmopper/meristem/internal/safety"
)

var (
	ErrNotFound = errors.New("workitems: not found")
	// ErrInvalidRequest is returned for malformed work_item declarations that
	// are deterministically rejected before any event is appended.
	ErrInvalidRequest = errors.New("workitems: invalid request")
	// ErrInvalidState is returned when a caller names a state outside the
	// lifecycle enum.
	ErrInvalidState = errors.New("workitems: invalid state")
	// ErrInvalidTransition is returned when a valid state is not reachable
	// from the work item's current lifecycle state.
	ErrInvalidTransition = errors.New("workitems: invalid transition")
	// ErrRelationCycle is returned when adding the parent->child edge
	// would close a cycle in the work_item DAG. The migration's CHECK
	// constraint blocks the self-loop case at the row level; everything
	// deeper has to be enforced here, before the relation event is
	// appended, because the projector trusts the writer.
	ErrRelationCycle = errors.New("workitems: relation would create a cycle")
	// ErrConvergenceChecksRequired is returned when an item with no declared
	// convergence checks tries to move into execution states. The scribe worker
	// is the deterministic path for filling those checks.
	ErrConvergenceChecksRequired = errors.New("workitems: convergence checks required")
	// ErrHumanReviewDecisionDenied is a pure pre-append refusal: clearing a
	// human-review block or asserting approved review requires one explicit
	// non-root human authority.
	ErrHumanReviewDecisionDenied = errors.New("workitems: human review decision requires an active non-root human token with work_items.review_decide")
	// ErrHumanReviewBlocked prevents completed creation or a completion claim
	// from terminalizing an item that still waits on its human-review gate.
	ErrHumanReviewBlocked = errors.New("workitems: human review is blocked")
	// ErrUnexpectedEventDedupe is returned when a first-attempt mutation's
	// event collides with an existing row while NOT running under the
	// idempotency contract. Without a discriminator, fresh=false here means
	// the action was silently swallowed (the original 2026-07-03 live bug);
	// failing loudly turns silent state desync into a reportable error.
	ErrUnexpectedEventDedupe = errors.New("workitems: unexpected_event_dedupe: identical event already exists; if this is a distinct action, retry through the idempotency contract")
	// ErrXylemBudgetExhausted is returned after the service has recorded a
	// structured xylem.exhausted event and routed the affected item through
	// its escalation rule. The requested over-budget action is not appended.
	ErrXylemBudgetExhausted = errors.New("workitems: xylem budget exhausted")
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
	Cultivar                   string
	PatienceBudgetSeconds      int
	EscalationRule             domain.EscalationRule
}

type UpdateMetadataInput struct {
	SuggestedConvergenceChecks []string
	HumanReviewStatus          domain.HumanReviewStatus
	Actor                      domain.Token
}

func (s *Service) Create(ctx context.Context, in CreateInput) (domain.WorkItem, error) {
	if strings.TrimSpace(in.Title) == "" {
		return domain.WorkItem{}, fmt.Errorf("%w: title is required", ErrInvalidRequest)
	}
	state := in.State
	if state == "" {
		state = domain.WorkItemCaptured
	}
	if !state.Valid() {
		return domain.WorkItem{}, fmt.Errorf("%w: %q", ErrInvalidState, state)
	}
	checks, err := normalizeSuggestedConvergenceChecks(in.SuggestedConvergenceChecks)
	if err != nil {
		return domain.WorkItem{}, err
	}
	humanReview, err := normalizeHumanReviewStatus(in.HumanReviewStatus)
	if err != nil {
		return domain.WorkItem{}, err
	}
	if state == domain.WorkItemDone && humanReview == domain.HumanReviewBlocked {
		return domain.WorkItem{}, ErrHumanReviewBlocked
	}
	if humanReview == domain.HumanReviewApproved && !access.CanDecideHumanReview(in.Actor) {
		return domain.WorkItem{}, ErrHumanReviewDecisionDenied
	}
	createdPayload, err := buildCreatedPayload(ctx, s.pool, in, state, checks, humanReview)
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
		Payload:      createdPayload,
	}); err != nil {
		return domain.WorkItem{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItem{}, err
	}
	return s.Get(ctx, id)
}

func (s *Service) SpawnChild(ctx context.Context, parentID uuid.UUID, in CreateInput) (domain.WorkItem, error) {
	childID := newSubjectID(ctx, "child_work_item")
	item, _, err := s.SpawnChildWithID(ctx, parentID, childID, in)
	return item, err
}

// SpawnChildWithID creates a parent->child edge using a caller-provided child
// id. Reconciler-owned children use this to make "one child per parent ever"
// an identity property instead of a race against process memory.
func (s *Service) SpawnChildWithID(ctx context.Context, parentID, childID uuid.UUID, in CreateInput) (domain.WorkItem, bool, error) {
	if strings.TrimSpace(in.Title) == "" {
		return domain.WorkItem{}, false, fmt.Errorf("%w: title is required", ErrInvalidRequest)
	}
	state := in.State
	if state == "" {
		state = domain.WorkItemCaptured
	}
	if !state.Valid() {
		return domain.WorkItem{}, false, fmt.Errorf("%w: %q", ErrInvalidState, state)
	}
	checks, err := normalizeSuggestedConvergenceChecks(in.SuggestedConvergenceChecks)
	if err != nil {
		return domain.WorkItem{}, false, err
	}
	humanReview, err := normalizeHumanReviewStatus(in.HumanReviewStatus)
	if err != nil {
		return domain.WorkItem{}, false, err
	}
	if state == domain.WorkItemDone && humanReview == domain.HumanReviewBlocked {
		return domain.WorkItem{}, false, ErrHumanReviewBlocked
	}
	if humanReview == domain.HumanReviewApproved && !access.CanDecideHumanReview(in.Actor) {
		return domain.WorkItem{}, false, ErrHumanReviewDecisionDenied
	}
	createdPayload, err := buildCreatedPayload(ctx, s.pool, in, state, checks, humanReview)
	if err != nil {
		return domain.WorkItem{}, false, err
	}
	if childID == uuid.Nil {
		return domain.WorkItem{}, false, fmt.Errorf("%w: child id is required", ErrInvalidRequest)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItem{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	parent, err := scanWorkItemForUpdate(ctx, tx, parentID)
	if err != nil {
		return domain.WorkItem{}, false, err
	}
	// Self-loops are blocked by the work_item_relations CHECK
	// constraint, but check explicitly here for a clearer error than the
	// generic constraint violation pgx would surface.
	if childID == parentID {
		return domain.WorkItem{}, false, ErrRelationCycle
	}
	cycle, err := childIsAncestorOf(ctx, tx, childID, parentID)
	if err != nil {
		return domain.WorkItem{}, false, err
	}
	if cycle {
		return domain.WorkItem{}, false, ErrRelationCycle
	}
	related, err := childRelationExists(ctx, tx, parentID, childID)
	if err != nil {
		return domain.WorkItem{}, false, err
	}
	if !related {
		exhausted, budgetErr, err := s.enforceChildCountBudget(ctx, tx, parent, childID, in)
		if err != nil {
			return domain.WorkItem{}, false, err
		}
		if exhausted {
			if err := tx.Commit(ctx); err != nil {
				return domain.WorkItem{}, false, err
			}
			return domain.WorkItem{}, false, budgetErr
		}
	}
	_, createdFresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    childID,
		Kind:         domain.EventWorkItemCreated,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload:      createdPayload,
	})
	if err != nil {
		return domain.WorkItem{}, false, err
	}
	_, relationFresh, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    parentID,
		Kind:         domain.EventWorkItemRelationAdded,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload: map[string]any{
			"parent_id": parentID,
			"child_id":  childID,
		},
	})
	if err != nil {
		return domain.WorkItem{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItem{}, false, err
	}
	item, err := s.Get(ctx, childID)
	return item, createdFresh || relationFresh, err
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
	current, err := scanWorkItemForUpdate(ctx, tx, id)
	if err != nil {
		return domain.WorkItem{}, err
	}
	if humanReviewDecisionRequired(current.HumanReviewStatus, humanReview) && !access.CanDecideHumanReview(in.Actor) {
		return domain.WorkItem{}, ErrHumanReviewDecisionDenied
	}
	spec := events.Spec{
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
	}
	exhausted, budgetErr, err := s.appendWorkItemEventWithRateBudget(ctx, tx, current, spec, "", in.Actor)
	if err != nil {
		return domain.WorkItem{}, err
	}
	if exhausted {
		if err := tx.Commit(ctx); err != nil {
			return domain.WorkItem{}, err
		}
		return domain.WorkItem{}, budgetErr
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItem{}, err
	}
	return s.Get(ctx, id)
}

func (s *Service) Transition(ctx context.Context, id uuid.UUID, to domain.WorkItemState, reason string, actor domain.Token) (domain.WorkItem, error) {
	if !to.Valid() {
		return domain.WorkItem{}, fmt.Errorf("%w: %q", ErrInvalidState, to)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkItem{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, err := scanWorkItemForUpdate(ctx, tx, id)
	if err != nil {
		return domain.WorkItem{}, err
	}
	if to == domain.WorkItemDone && current.HumanReviewStatus == domain.HumanReviewBlocked {
		return domain.WorkItem{}, ErrHumanReviewBlocked
	}
	if !domain.CanTransition(current.State, to) {
		return domain.WorkItem{}, fmt.Errorf("%w: from %s to %s", ErrInvalidTransition, current.State, to)
	}
	if convergenceChecksRequired(current, to) {
		return domain.WorkItem{}, fmt.Errorf("%w: item %s in %s needs a convergence-scribe child to define suggested_convergence_checks before moving to %s", ErrConvergenceChecksRequired, id, current.State, to)
	}
	if to == domain.WorkItemRunning && current.State != domain.WorkItemRunning {
		exhausted, budgetErr, err := s.enforceConcurrentRunningBudget(ctx, tx, current, actor)
		if err != nil {
			return domain.WorkItem{}, err
		}
		if exhausted {
			if err := tx.Commit(ctx); err != nil {
				return domain.WorkItem{}, err
			}
			return domain.WorkItem{}, budgetErr
		}
	}
	spec := events.Spec{
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
	}
	exhausted, budgetErr, err := s.appendWorkItemEventWithRateBudget(ctx, tx, current, spec, "", actor)
	if err != nil {
		return domain.WorkItem{}, err
	}
	if exhausted {
		if err := tx.Commit(ctx); err != nil {
			return domain.WorkItem{}, err
		}
		return domain.WorkItem{}, budgetErr
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkItem{}, err
	}
	return s.Get(ctx, id)
}

func convergenceChecksRequired(current domain.WorkItem, to domain.WorkItemState) bool {
	if len(current.SuggestedConvergenceChecks) > 0 {
		return false
	}
	switch current.State {
	case domain.WorkItemCaptured, domain.WorkItemTriaged:
	default:
		return false
	}
	switch to {
	case domain.WorkItemCaptured, domain.WorkItemTriaged, domain.WorkItemBlocked, domain.WorkItemFailed, domain.WorkItemCanceled:
		return false
	default:
		return true
	}
}

// validateAppendPayloadShape enforces the object contract for appended event
// payloads at the one seam REST and MCP share. It also rejects an already
// wrapped {inner_kind, inner} envelope: AppendEvent owns that outer shape.
// Write-side rejection keeps client-side marshaling bugs from minting
// malformed non-signals;
// historical string inners stay readable through the reducer's legacy
// recovery, which tolerates exactly what this boundary no longer admits.
// The string-of-JSON case gets its own message because it is always a
// double-encoding bug, never intent.
func validateAppendPayloadShape(payload any) error {
	switch v := payload.(type) {
	case nil:
		return nil
	case map[string]any:
		if len(v) == 2 {
			_, hasInnerKind := v["inner_kind"]
			_, hasInner := v["inner"]
			if hasInnerKind && hasInner {
				return fmt.Errorf("%w: payload is already a work_item.event_appended envelope; send the logical event kind and its payload object without the inner_kind/inner wrapper", ErrInvalidRequest)
			}
		}
		return nil
	case string:
		trimmed := strings.TrimSpace(v)
		if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			var parsed any
			if json.Unmarshal([]byte(trimmed), &parsed) == nil {
				return fmt.Errorf("%w: payload arrived as JSON-encoded text (double-encoded); send the JSON object itself, not its string form", ErrInvalidRequest)
			}
		}
		return fmt.Errorf("%w: payload must be a JSON object when present; got a string", ErrInvalidRequest)
	default:
		return fmt.Errorf("%w: payload must be a JSON object when present; got %T", ErrInvalidRequest, payload)
	}
}

func (s *Service) AppendEvent(ctx context.Context, id uuid.UUID, innerKind string, payload any, actor domain.Token) error {
	innerKind = strings.TrimSpace(innerKind)
	if innerKind == "" {
		return fmt.Errorf("%w: event kind is required", ErrInvalidRequest)
	}
	if innerKind == domain.EventWorkItemEventAppended {
		return fmt.Errorf("%w: event kind %q is the transport envelope; send the logical inner event kind instead", ErrInvalidRequest, innerKind)
	}
	if err := validateAppendPayloadShape(payload); err != nil {
		return err
	}
	if innerKind == ReviewVerdictCheckKind {
		return fmt.Errorf("%w: event kind %q is reserved; append %q and let the deterministic reducer derive the check", ErrInvalidRequest, innerKind, ReviewVerdictInnerKind)
	}
	if innerKind == ReviewVerdictInnerKind {
		if _, err := ParseReviewVerdict(payload); err != nil {
			return err
		}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, err := scanWorkItemForUpdate(ctx, tx, id)
	if err != nil {
		return err
	}
	eventPayload := map[string]any{
		"inner_kind": innerKind,
		"inner":      payload,
	}
	spec := events.Spec{
		SubjectKind:   domain.SubjectWorkItem,
		SubjectID:     id,
		Kind:          domain.EventWorkItemEventAppended,
		Source:        sourceForActor(actor),
		ActorTokenID:  &actor.ID,
		Discriminator: eventDiscriminator(ctx),
		Payload:       eventPayload,
	}
	exhausted, budgetErr, err := s.appendWorkItemEventWithRateBudget(ctx, tx, current, spec, innerKind, actor)
	if err != nil {
		return err
	}
	if exhausted {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return budgetErr
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

func childRelationExists(ctx context.Context, tx pgx.Tx, parent, child uuid.UUID) (bool, error) {
	var found bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM work_item_relations
			WHERE parent_id = $1 AND child_id = $2
		)
	`, parent, child).Scan(&found); err != nil {
		return false, fmt.Errorf("workitems: child relation exists: %w", err)
	}
	return found, nil
}

type childCountBudget struct {
	Max            int
	Source         string
	Cultivar       string
	EscalationRule domain.EscalationRule
}

type concurrentRunningBudget struct {
	Max            int
	Source         string
	Cultivar       string
	EscalationRule domain.EscalationRule
}

type eventRateBudget struct {
	Max            int
	Source         string
	Cultivar       string
	Class          string
	EscalationRule domain.EscalationRule
}

type createdLaunchMetadata struct {
	Cultivar       string                `json:"cultivar"`
	EscalationRule domain.EscalationRule `json:"escalation_rule"`
}

func (s *Service) enforceChildCountBudget(ctx context.Context, tx pgx.Tx, parent domain.WorkItem, childID uuid.UUID, in CreateInput) (bool, error, error) {
	budget, err := s.resolveChildCountBudget(ctx, tx, parent.ID)
	if err != nil {
		return false, nil, err
	}
	current, err := countedChildCount(ctx, tx, parent.ID)
	if err != nil {
		return false, nil, err
	}
	if current < budget.Max {
		return false, nil, nil
	}
	payload := map[string]any{
		"budget":                "max_children_per_item",
		"current_children":      current,
		"max_children_per_item": budget.Max,
		"budget_source":         budget.Source,
		"cultivar":              budget.Cultivar,
		"escalation_rule":       string(budget.EscalationRule),
		"attempted_child_id":    childID,
		"attempted_child_title": in.Title,
		"parent_state":          parent.State,
		"count_excludes":        []string{"human_attention_escalation_children"},
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    parent.ID,
		Kind:         domain.EventXylemExhausted,
		Source:       sourceForActor(in.Actor),
		ActorTokenID: &in.Actor.ID,
		Payload:      payload,
	}); err != nil {
		return false, nil, err
	}
	switch budget.EscalationRule {
	case domain.EscalationRuleHandToHuman:
	default:
		return false, nil, fmt.Errorf("workitems: unknown escalation rule %q", budget.EscalationRule)
	}
	reason := childCountBudgetEscalationReason()
	summary := childCountBudgetEscalationSummary(parent, current, budget)
	if err := s.requestXylemEscalationInTx(ctx, tx, parent, reason, summary, in.Actor); err != nil {
		return false, nil, err
	}
	return true, fmt.Errorf("%w: max_children_per_item exhausted for parent %s: current_children=%d max=%d source=%s", ErrXylemBudgetExhausted, parent.ID, current, budget.Max, budget.Source), nil
}

func (s *Service) resolveChildCountBudget(ctx context.Context, tx pgx.Tx, parentID uuid.UUID) (childCountBudget, error) {
	meta, err := workItemLaunchMetadata(ctx, tx, parentID)
	if err != nil {
		return childCountBudget{}, err
	}
	rule := meta.EscalationRule
	if rule == "" {
		rule = domain.EscalationRuleHandToHuman
	}
	if !rule.Valid() {
		return childCountBudget{}, fmt.Errorf("%w: invalid escalation_rule %q", ErrInvalidRequest, rule)
	}
	budget := childCountBudget{
		Max:            safety.DefaultPolicy().MaxChildrenPerItem,
		Source:         "safety_policy",
		EscalationRule: rule,
	}
	cultivarRef := strings.TrimSpace(meta.Cultivar)
	if cultivarRef == "" {
		return budget, nil
	}
	item, err := registry.NewService(s.pool, nil).GetCultivarRef(ctx, cultivarRef)
	if err != nil {
		return childCountBudget{}, err
	}
	budget.Cultivar = fmt.Sprintf("%s@%d", item.Name, item.Version)
	if item.Xylem.MaxChildrenPerItem > 0 {
		budget.Max = item.Xylem.MaxChildrenPerItem
		budget.Source = "cultivar:" + budget.Cultivar
	}
	return budget, nil
}

func (s *Service) enforceConcurrentRunningBudget(ctx context.Context, tx pgx.Tx, item domain.WorkItem, actor domain.Token) (bool, error, error) {
	if err := lockActorToken(ctx, tx, actor.ID); err != nil {
		return false, nil, err
	}
	budget, err := s.resolveConcurrentRunningBudget(ctx, tx, item.ID)
	if err != nil {
		return false, nil, err
	}
	current, err := runningCountForActor(ctx, tx, item.ID, actor.ID)
	if err != nil {
		return false, nil, err
	}
	if current < budget.Max {
		return false, nil, nil
	}
	payload := map[string]any{
		"budget":                                 "max_concurrent_running_items_per_token",
		"current_running":                        current,
		"max_concurrent_running_items_per_token": budget.Max,
		"budget_source":                          budget.Source,
		"cultivar":                               budget.Cultivar,
		"escalation_rule":                        string(budget.EscalationRule),
		"actor_token_id":                         actor.ID,
		"attempted_state":                        domain.WorkItemRunning,
		"work_item_state":                        item.State,
		"count_scope":                            "same_actor_token_current_running_epoch",
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    item.ID,
		Kind:         domain.EventXylemExhausted,
		Source:       sourceForActor(actor),
		ActorTokenID: &actor.ID,
		Payload:      payload,
	}); err != nil {
		return false, nil, err
	}
	switch budget.EscalationRule {
	case domain.EscalationRuleHandToHuman:
	default:
		return false, nil, fmt.Errorf("workitems: unknown escalation rule %q", budget.EscalationRule)
	}
	reason := concurrentRunningBudgetEscalationReason()
	summary := concurrentRunningBudgetEscalationSummary(item, current, budget, actor.ID)
	if err := s.requestXylemEscalationInTx(ctx, tx, item, reason, summary, actor); err != nil {
		return false, nil, err
	}
	return true, fmt.Errorf("%w: max_concurrent_running_items_per_token exhausted for actor token %s: current_running=%d max=%d source=%s", ErrXylemBudgetExhausted, actor.ID, current, budget.Max, budget.Source), nil
}

func (s *Service) resolveConcurrentRunningBudget(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID) (concurrentRunningBudget, error) {
	meta, err := workItemLaunchMetadata(ctx, tx, workItemID)
	if err != nil {
		return concurrentRunningBudget{}, err
	}
	rule := meta.EscalationRule
	if rule == "" {
		rule = domain.EscalationRuleHandToHuman
	}
	if !rule.Valid() {
		return concurrentRunningBudget{}, fmt.Errorf("%w: invalid escalation_rule %q", ErrInvalidRequest, rule)
	}
	budget := concurrentRunningBudget{
		Max:            safety.DefaultPolicy().MaxConcurrentRunningPerToken,
		Source:         "safety_policy",
		EscalationRule: rule,
	}
	cultivarRef := strings.TrimSpace(meta.Cultivar)
	if cultivarRef == "" {
		return budget, nil
	}
	item, err := registry.NewService(s.pool, nil).GetCultivarRef(ctx, cultivarRef)
	if err != nil {
		return concurrentRunningBudget{}, err
	}
	budget.Cultivar = fmt.Sprintf("%s@%d", item.Name, item.Version)
	if item.Xylem.MaxConcurrentRunningPerToken > 0 {
		budget.Max = item.Xylem.MaxConcurrentRunningPerToken
		budget.Source = "cultivar:" + budget.Cultivar
	}
	return budget, nil
}

func (s *Service) appendWorkItemEventWithRateBudget(ctx context.Context, tx pgx.Tx, item domain.WorkItem, spec events.Spec, attemptedInnerKind string, actor domain.Token) (bool, error, error) {
	eventID, err := events.DeterministicID(spec)
	if err != nil {
		return false, nil, err
	}
	exists, err := eventExistsByID(ctx, tx, eventID)
	if err != nil {
		return false, nil, err
	}
	if exists {
		if spec.Discriminator == "" {
			// A first-attempt append outside the idempotency contract just
			// collided with an existing event: without a discriminator this
			// is the silent-swallow bug class (2026-07-03), not a replay.
			// Fail loudly so callers surface it instead of desyncing.
			return false, nil, fmt.Errorf("%w: kind=%s subject=%s", ErrUnexpectedEventDedupe, spec.Kind, spec.SubjectID)
		}
		return false, nil, nil
	}
	rawPayload, err := json.Marshal(spec.Payload)
	if err != nil {
		return false, nil, fmt.Errorf("workitems: marshal event kind %q for xylem budget: %w", spec.Kind, err)
	}
	class, ok := workItemEventBudgetClass(spec.Kind, rawPayload)
	if !ok {
		return false, nil, fmt.Errorf("workitems: classify event kind %q for xylem budget", spec.Kind)
	}
	exhausted, budgetErr, err := s.enforceEventRateBudget(ctx, tx, item, eventID, spec.Kind, attemptedInnerKind, class, actor)
	if err != nil {
		return false, nil, err
	}
	if exhausted {
		return true, budgetErr, nil
	}
	if _, _, err := s.writer.Append(ctx, tx, spec); err != nil {
		return false, nil, err
	}
	return false, nil, nil
}

// workItemEventBudgetClass is the single xylem accounting classifier for both
// attempted and persisted events. Assignment lifecycle remains admin and
// non-projectable in the feed until Assigned Lane ships, but it must consume
// the lifecycle event budget here so claim/yield churn is bounded.
func workItemEventBudgetClass(kind string, payload json.RawMessage) (string, bool) {
	switch kind {
	case domain.EventWorkItemAssigned, domain.EventWorkItemAssignmentReleased:
		return feed.KindClassLifecycle, true
	default:
		return feed.ClassifyItem(feed.Item{Kind: kind, Payload: payload})
	}
}

func (s *Service) enforceEventRateBudget(ctx context.Context, tx pgx.Tx, item domain.WorkItem, attemptedEventID uuid.UUID, attemptedKind string, attemptedInnerKind string, class string, actor domain.Token) (bool, error, error) {
	budget, err := s.resolveEventRateBudget(ctx, tx, item.ID, class)
	if err != nil {
		return false, nil, err
	}
	current, err := countedEventsInLastHourByClass(ctx, tx, item.ID, class)
	if err != nil {
		return false, nil, err
	}
	if current < budget.Max {
		return false, nil, nil
	}
	payload := map[string]any{
		"budget":                       "max_events_per_item_per_hour_by_class",
		"taxonomy_class":               class,
		"current_events":               current,
		"max_events_per_item_per_hour": budget.Max,
		"window_seconds":               3600,
		"budget_source":                budget.Source,
		"cultivar":                     budget.Cultivar,
		"escalation_rule":              string(budget.EscalationRule),
		"attempted_event_id":           attemptedEventID,
		"attempted_event_kind":         attemptedKind,
		"attempted_inner_kind":         attemptedInnerKind,
		"actor_token_id":               actor.ID,
		"work_item_state":              item.State,
		"count_scope":                  "same_work_item_same_taxonomy_class_last_hour",
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    item.ID,
		Kind:         domain.EventXylemExhausted,
		Source:       sourceForActor(actor),
		ActorTokenID: &actor.ID,
		Payload:      payload,
	}); err != nil {
		return false, nil, err
	}
	switch budget.EscalationRule {
	case domain.EscalationRuleHandToHuman:
	default:
		return false, nil, fmt.Errorf("workitems: unknown escalation rule %q", budget.EscalationRule)
	}
	reason := eventRateBudgetEscalationReason(class)
	summary := eventRateBudgetEscalationSummary(item, current, budget)
	if err := s.requestXylemEscalationInTx(ctx, tx, item, reason, summary, actor); err != nil {
		return false, nil, err
	}
	return true, fmt.Errorf("%w: max_events_per_item_per_hour_by_class exhausted for work item %s class %s: current_events=%d max=%d source=%s", ErrXylemBudgetExhausted, item.ID, class, current, budget.Max, budget.Source), nil
}

func (s *Service) resolveEventRateBudget(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID, class string) (eventRateBudget, error) {
	meta, err := workItemLaunchMetadata(ctx, tx, workItemID)
	if err != nil {
		return eventRateBudget{}, err
	}
	rule := meta.EscalationRule
	if rule == "" {
		rule = domain.EscalationRuleHandToHuman
	}
	if !rule.Valid() {
		return eventRateBudget{}, fmt.Errorf("%w: invalid escalation_rule %q", ErrInvalidRequest, rule)
	}
	defaults := safety.DefaultPolicy().MaxEventsPerItemPerHourByClass
	max := defaults[class]
	if max <= 0 {
		return eventRateBudget{}, fmt.Errorf("workitems: missing safety fallback for taxonomy class %q", class)
	}
	budget := eventRateBudget{
		Max:            max,
		Source:         "safety_policy",
		Class:          class,
		EscalationRule: rule,
	}
	cultivarRef := strings.TrimSpace(meta.Cultivar)
	if cultivarRef == "" {
		return budget, nil
	}
	xylem, resolvedRef, err := cultivarXylemForRefInTx(ctx, tx, cultivarRef)
	if err != nil {
		return eventRateBudget{}, err
	}
	budget.Cultivar = resolvedRef
	if xylem.MaxEventsPerItemPerHourByClass[class] > 0 {
		budget.Max = xylem.MaxEventsPerItemPerHourByClass[class]
		budget.Source = "cultivar:" + budget.Cultivar
	}
	return budget, nil
}

func workItemLaunchMetadata(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID) (createdLaunchMetadata, error) {
	var raw []byte
	err := tx.QueryRow(ctx, `
		SELECT payload
		FROM events
		WHERE subject_kind = $1
		  AND subject_id = $2
		  AND kind = $3
		ORDER BY occurred_at ASC
		LIMIT 1
	`, domain.SubjectWorkItem, workItemID, domain.EventWorkItemCreated).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return createdLaunchMetadata{}, nil
		}
		return createdLaunchMetadata{}, fmt.Errorf("workitems: load work item launch metadata: %w", err)
	}
	var meta createdLaunchMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return createdLaunchMetadata{}, fmt.Errorf("workitems: decode work item launch metadata: %w", err)
	}
	meta.Cultivar = strings.TrimSpace(meta.Cultivar)
	return meta, nil
}

func countedChildCount(ctx context.Context, tx pgx.Tx, parentID uuid.UUID) (int, error) {
	var count int
	err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM work_item_relations wir
		WHERE wir.parent_id = $1
		  AND NOT EXISTS (
			SELECT 1
			FROM events e
			WHERE e.kind = $2
			  AND e.payload->>'work_item_id' = $1::uuid::text
			  AND e.payload->>'human_work_item_id' = wir.child_id::text
		  )
	`, parentID, domain.EventEscalationRequested).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("workitems: count children for xylem budget: %w", err)
	}
	return count, nil
}

func countedEventsInLastHourByClass(ctx context.Context, tx pgx.Tx, workItemID uuid.UUID, class string) (int, error) {
	rows, err := tx.Query(ctx, `
		SELECT kind, payload
		FROM events
		WHERE subject_kind = $1
		  AND subject_id = $2
		  AND occurred_at >= now() - interval '1 hour'
	`, domain.SubjectWorkItem, workItemID)
	if err != nil {
		return 0, fmt.Errorf("workitems: count events for xylem budget: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var item feed.Item
		if err := rows.Scan(&item.Kind, &item.Payload); err != nil {
			return 0, fmt.Errorf("workitems: scan event for xylem budget: %w", err)
		}
		got, ok := workItemEventBudgetClass(item.Kind, item.Payload)
		if ok && got == class {
			count++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("workitems: count events for xylem budget: %w", err)
	}
	return count, nil
}

func childCountBudgetEscalationReason() string {
	return "xylem budget exhausted: max_children_per_item"
}

func childCountBudgetEscalationSummary(parent domain.WorkItem, current int, budget childCountBudget) string {
	return fmt.Sprintf(
		"Work item %s (%s) exhausted max_children_per_item: current_children=%d max=%d source=%s",
		parent.ID,
		parent.Title,
		current,
		budget.Max,
		budget.Source,
	)
}

func lockActorToken(ctx context.Context, tx pgx.Tx, actorID uuid.UUID) error {
	if actorID == uuid.Nil {
		return fmt.Errorf("workitems: actor token id is required for max_concurrent_running_items_per_token")
	}
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM tokens WHERE id = $1 FOR UPDATE`, actorID).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("workitems: actor token %s not found for max_concurrent_running_items_per_token", actorID)
		}
		return fmt.Errorf("workitems: lock actor token for running budget: %w", err)
	}
	return nil
}

func runningCountForActor(ctx context.Context, tx pgx.Tx, excludingWorkItemID uuid.UUID, actorID uuid.UUID) (int, error) {
	var count int
	err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM work_items wi
		JOIN LATERAL (
			SELECT e.actor_token_id
			FROM events e
			WHERE e.subject_kind = $1
			  AND e.subject_id = wi.id
			  AND e.occurred_at = wi.state_entered_at
			  AND (
				(e.kind = $2 AND COALESCE(e.payload->>'state', $5::text) = $4::text)
				OR (e.kind = $3 AND e.payload->>'to' = $4::text)
			  )
			LIMIT 1
		) entered_running ON true
		WHERE wi.state = $4::text
		  AND wi.id <> $6
		  AND entered_running.actor_token_id = $7
	`, domain.SubjectWorkItem, domain.EventWorkItemCreated, domain.EventWorkItemTransitioned, domain.WorkItemRunning, domain.WorkItemCaptured, excludingWorkItemID, actorID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("workitems: count running items for xylem budget: %w", err)
	}
	return count, nil
}

func concurrentRunningBudgetEscalationReason() string {
	return "xylem budget exhausted: max_concurrent_running_items_per_token"
}

func concurrentRunningBudgetEscalationSummary(item domain.WorkItem, current int, budget concurrentRunningBudget, actorID uuid.UUID) string {
	return fmt.Sprintf(
		"Work item %s (%s) exhausted max_concurrent_running_items_per_token for actor token %s: current_running=%d max=%d source=%s",
		item.ID,
		item.Title,
		actorID,
		current,
		budget.Max,
		budget.Source,
	)
}

func eventRateBudgetEscalationReason(class string) string {
	return "xylem budget exhausted: max_events_per_item_per_hour_by_class:" + class
}

func eventRateBudgetEscalationSummary(item domain.WorkItem, current int, budget eventRateBudget) string {
	return fmt.Sprintf(
		"Work item %s (%s) exhausted max_events_per_item_per_hour_by_class for class %s: current_events=%d max=%d window_seconds=3600 source=%s",
		item.ID,
		item.Title,
		budget.Class,
		current,
		budget.Max,
		budget.Source,
	)
}

func eventExistsByID(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (bool, error) {
	var ok bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, eventID).Scan(&ok); err != nil {
		return false, fmt.Errorf("workitems: check existing event: %w", err)
	}
	return ok, nil
}

func (s *Service) requestXylemEscalationInTx(ctx context.Context, tx pgx.Tx, parent domain.WorkItem, reason string, summary string, actor domain.Token) error {
	escalationID := deterministicEscalationID(parent.ID, reason, summary)
	humanWorkItemID := deterministicHumanWorkItemID(escalationID)
	if ok, err := escalationExists(ctx, tx, escalationID); err != nil {
		return err
	} else if ok {
		return nil
	}
	actorID := &actor.ID
	source := sourceForActor(actor)
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectEscalation,
		SubjectID:    escalationID,
		Kind:         domain.EventEscalationRequested,
		Source:       source,
		ActorTokenID: actorID,
		Payload: map[string]any{
			"work_item_id":        parent.ID,
			"human_work_item_id":  humanWorkItemID,
			"reason":              reason,
			"summary":             summary,
			"origin_state":        parent.State,
			"origin_state_reason": parent.StateReason,
		},
	}); err != nil {
		return err
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    humanWorkItemID,
		Kind:         domain.EventWorkItemCreated,
		Source:       source,
		ActorTokenID: actorID,
		Payload: map[string]any{
			"title":                        "Human attention: " + parent.Title,
			"body":                         humanWorkItemBody(parent, reason, summary),
			"state":                        domain.WorkItemCaptured,
			"suggested_convergence_checks": []string{"human_response_recorded"},
			"human_review_status":          domain.HumanReviewBlocked,
		},
	}); err != nil {
		return err
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    parent.ID,
		Kind:         domain.EventWorkItemRelationAdded,
		Source:       source,
		ActorTokenID: actorID,
		Payload: map[string]any{
			"parent_id": parent.ID,
			"child_id":  humanWorkItemID,
		},
	}); err != nil {
		return err
	}
	if _, _, err := s.writer.Append(ctx, tx, events.Spec{
		SubjectKind:  domain.SubjectWorkItem,
		SubjectID:    parent.ID,
		Kind:         domain.EventWorkItemMetadataUpdated,
		Source:       source,
		ActorTokenID: actorID,
		Payload: map[string]any{
			"from": map[string]any{
				"suggested_convergence_checks": parent.SuggestedConvergenceChecks,
				"human_review_status":          parent.HumanReviewStatus,
			},
			"to": map[string]any{
				"suggested_convergence_checks": parent.SuggestedConvergenceChecks,
				"human_review_status":          domain.HumanReviewBlocked,
			},
		},
	}); err != nil {
		return err
	}
	if parent.State != domain.WorkItemBlocked {
		if _, _, err := s.writer.Append(ctx, tx, events.Spec{
			SubjectKind:  domain.SubjectWorkItem,
			SubjectID:    parent.ID,
			Kind:         domain.EventWorkItemTransitioned,
			Source:       source,
			ActorTokenID: actorID,
			Payload: map[string]any{
				"from":   parent.State,
				"to":     domain.WorkItemBlocked,
				"reason": "human escalation requested: " + reason,
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func escalationExists(ctx context.Context, tx pgx.Tx, escalationID uuid.UUID) (bool, error) {
	var ok bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events
			WHERE subject_kind = $1 AND subject_id = $2 AND kind = $3
		)
	`, domain.SubjectEscalation, escalationID, domain.EventEscalationRequested).Scan(&ok); err != nil {
		return false, fmt.Errorf("workitems: check existing escalation: %w", err)
	}
	return ok, nil
}

func deterministicEscalationID(workItemID uuid.UUID, reason string, summary string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"meristem",
		"escalation",
		workItemID.String(),
		reason,
		summary,
	}, "\x00")))
}

func deterministicHumanWorkItemID(escalationID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"meristem",
		"escalation",
		"human-work-item",
		escalationID.String(),
	}, "\x00")))
}

func humanWorkItemBody(parent domain.WorkItem, reason string, summary string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Escalation requested for work_item %s.\n\n", parent.ID)
	fmt.Fprintf(&b, "Reason: %s\n\n", reason)
	if summary != reason {
		fmt.Fprintf(&b, "Summary: %s\n\n", summary)
	}
	fmt.Fprintf(&b, "Respond by appending a human decision or by moving the original work_item out of blocked once resolved.")
	return b.String()
}

func sourceForActor(actor domain.Token) domain.Source {
	if actor.Source.Valid() {
		return actor.Source
	}
	return domain.SourceHuman
}

func buildCreatedPayload(ctx context.Context, pool *pgxpool.Pool, in CreateInput, state domain.WorkItemState, checks []string, humanReview domain.HumanReviewStatus) (map[string]any, error) {
	payload := map[string]any{
		"title":                        in.Title,
		"body":                         in.Body,
		"state":                        state,
		"suggested_convergence_checks": checks,
		"human_review_status":          humanReview,
	}
	cultivarRef := strings.TrimSpace(in.Cultivar)
	if cultivarRef != "" {
		item, err := registry.NewService(pool, nil).GetCultivarRef(ctx, cultivarRef)
		if err != nil {
			return nil, err
		}
		payload["cultivar"] = fmt.Sprintf("%s@%d", item.Name, item.Version)
	}
	if in.PatienceBudgetSeconds < 0 {
		return nil, fmt.Errorf("%w: patience_budget_seconds must be >= 0", ErrInvalidRequest)
	}
	if in.PatienceBudgetSeconds > 0 {
		if int64(in.PatienceBudgetSeconds) > int64(safety.MaxPatienceBudget/time.Second) {
			return nil, fmt.Errorf("%w: patience_budget_seconds exceeds the %s finite cap; bounded patience admits no effectively-infinite budget", ErrInvalidRequest, safety.MaxPatienceBudget)
		}
		payload["patience_budget_seconds"] = in.PatienceBudgetSeconds
	}
	rule, err := normalizeEscalationRule(in.EscalationRule)
	if err != nil {
		return nil, err
	}
	if rule != "" {
		payload["escalation_rule"] = string(rule)
	}
	return payload, nil
}

func normalizeSuggestedConvergenceChecks(in []string) ([]string, error) {
	out := make([]string, 0, len(in))
	for i, check := range in {
		trimmed := strings.TrimSpace(check)
		if trimmed == "" {
			return nil, fmt.Errorf("%w: suggested_convergence_checks[%d] is blank", ErrInvalidRequest, i)
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
		return "", fmt.Errorf("%w: invalid human_review_status %q", ErrInvalidRequest, status)
	}
	return status, nil
}

// humanReviewDecisionRequired identifies changes that confer or retain human
// clearance. Ordinary actors may always move toward the conservative blocked
// state and may update checklists on merely waved-through work, but only the
// owner authority may clear a block or assert/retain approved review.
func humanReviewDecisionRequired(from, to domain.HumanReviewStatus) bool {
	return to == domain.HumanReviewApproved ||
		(from == domain.HumanReviewBlocked && to == domain.HumanReviewWavedThrough)
}

func normalizeEscalationRule(rule domain.EscalationRule) (domain.EscalationRule, error) {
	if rule == "" {
		return "", nil
	}
	if !rule.Valid() {
		return "", fmt.Errorf("%w: invalid escalation_rule %q", ErrInvalidRequest, rule)
	}
	return rule, nil
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
	return s.list(ctx, state, limit)
}

// ListAll returns the complete work_items projection, newest first. It is
// intentionally separate from List so ordinary list surfaces retain their
// bounded response contract while aggregate views can compute honest totals.
func (s *Service) ListAll(ctx context.Context, state string) ([]domain.WorkItem, error) {
	return s.list(ctx, state, 0)
}

func (s *Service) list(ctx context.Context, state string, limit int) ([]domain.WorkItem, error) {
	args := []any{}
	query := `SELECT id, title, body, state, state_reason, suggested_convergence_checks, human_review_status, created_by, created_at, state_entered_at, updated_at FROM work_items`
	if state != "" {
		query += ` WHERE state = $1`
		args = append(args, state)
	}
	query += ` ORDER BY updated_at DESC`
	if limit > 0 {
		query += fmt.Sprintf(` LIMIT $%d`, len(args)+1)
		args = append(args, limit)
	}
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
	row := q.QueryRow(ctx, `SELECT id, title, body, state, state_reason, suggested_convergence_checks, human_review_status, created_by, created_at, state_entered_at, updated_at FROM work_items WHERE id = $1`, id)
	return scanWorkItemRow(row)
}

func scanWorkItemForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (domain.WorkItem, error) {
	row := tx.QueryRow(ctx, `SELECT id, title, body, state, state_reason, suggested_convergence_checks, human_review_status, created_by, created_at, state_entered_at, updated_at FROM work_items WHERE id = $1 FOR UPDATE`, id)
	return scanWorkItemRow(row)
}

func scanWorkItemRow(row rowScanner) (domain.WorkItem, error) {
	var item domain.WorkItem
	var state string
	var humanReview string
	var checksJSON []byte
	if err := row.Scan(&item.ID, &item.Title, &item.Body, &state, &item.StateReason, &checksJSON, &humanReview, &item.CreatedBy, &item.CreatedAt, &item.StateEnteredAt, &item.UpdatedAt); err != nil {
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
