// Package access contains deterministic read/action policy reducers.
//
// The reducers operate over canonical projections (tokens, work_items,
// relations, events) and return allow/deny or filtered read models. They are
// transport-independent: MCP, REST, CLI, and future provider context exports
// should call here instead of each inventing local visibility rules.
package access

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
)

const (
	ScopeWorkItemsRead     = "work_items.read"
	ScopeWorkItemsReadAll  = "work_items.read_all"
	ScopeWorkItemsWrite    = "work_items.write"
	ScopeWorkItemsWriteAll = "work_items.write_all"
	ScopeWorkItemsCreate   = "work_items.create"
	ScopeFeedRead          = "feed.read"
	ScopeFeedReadAssigned  = "feed.read_assigned"
	ScopeInboxCapture      = "inbox.capture"

	scopeWorkItemsTreePrefix = "work_items.tree:"
)

var ErrDenied = errors.New("access: denied")

// Service evaluates access decisions that need projection reads.
type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// ToolVisible reports whether a canonical MCP tool should be advertised to
// actor. It is a coarse capability filter; object-level tools still need
// per-object checks when called.
func ToolVisible(actor domain.Token, canonicalTool string) bool {
	if actor.IsRoot || legacyUnscoped(actor) {
		return true
	}
	scopes := scopeSet(actor.Scopes)
	switch canonicalTool {
	case "inbox.capture":
		return actor.Source == domain.SourceHuman && scopes[ScopeInboxCapture]
	case "feed.read":
		return scopes[ScopeFeedRead] || (scopes[ScopeFeedReadAssigned] && hasWorkItemTreeScope(actor))
	case "deterministic_errors.list", "deterministic_errors.get":
		return hasAny(scopes, "logs.read", "logs.read_details", "logs.read_restricted", "logs.read_masked", "logs.read_all")
	case "work_items.list", "work_items.get":
		return canReadWorkItems(scopes) && (scopes[ScopeWorkItemsReadAll] || scopes[ScopeWorkItemsWriteAll] || hasWorkItemTreeScope(actor))
	case "work_items.create":
		return scopes[ScopeWorkItemsCreate] || scopes[ScopeWorkItemsWriteAll]
	case "work_items.spawn_child", "work_items.append_event", "work_items.update_metadata", "work_items.transition":
		return scopes[ScopeWorkItemsWriteAll] || (scopes[ScopeWorkItemsWrite] && hasWorkItemTreeScope(actor))
	default:
		return false
	}
}

func RequiresScopedPolicy(actor domain.Token) bool {
	return !actor.IsRoot && !legacyUnscoped(actor)
}

func (s *Service) FilterWorkItems(ctx context.Context, actor domain.Token, items []domain.WorkItem) ([]domain.WorkItem, error) {
	if actor.IsRoot || legacyUnscoped(actor) || hasScope(actor, ScopeWorkItemsReadAll) || hasScope(actor, ScopeWorkItemsWriteAll) {
		return items, nil
	}
	if !canReadWorkItems(scopeSet(actor.Scopes)) {
		return nil, ErrDenied
	}
	out := make([]domain.WorkItem, 0, len(items))
	for _, item := range items {
		ok, err := s.workItemInAnyTree(ctx, actor, item.ID)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *Service) CanReadWorkItem(ctx context.Context, actor domain.Token, id uuid.UUID) error {
	if actor.IsRoot || legacyUnscoped(actor) || hasScope(actor, ScopeWorkItemsReadAll) || hasScope(actor, ScopeWorkItemsWriteAll) {
		return nil
	}
	if !canReadWorkItems(scopeSet(actor.Scopes)) {
		return ErrDenied
	}
	ok, err := s.workItemInAnyTree(ctx, actor, id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrDenied
	}
	return nil
}

func (s *Service) CanCreateWorkItem(_ context.Context, actor domain.Token) error {
	if actor.IsRoot || legacyUnscoped(actor) || hasScope(actor, ScopeWorkItemsCreate) || hasScope(actor, ScopeWorkItemsWriteAll) {
		return nil
	}
	return ErrDenied
}

func (s *Service) CanWriteWorkItem(ctx context.Context, actor domain.Token, id uuid.UUID) error {
	if actor.IsRoot || legacyUnscoped(actor) || hasScope(actor, ScopeWorkItemsWriteAll) {
		return nil
	}
	if !canWriteWorkItems(scopeSet(actor.Scopes)) {
		return ErrDenied
	}
	ok, err := s.workItemInAnyTree(ctx, actor, id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrDenied
	}
	return nil
}

func (s *Service) FilterFeedItems(ctx context.Context, actor domain.Token, items []feed.Item) ([]feed.Item, error) {
	if actor.IsRoot || legacyUnscoped(actor) || hasScope(actor, ScopeFeedRead) {
		return items, nil
	}
	if !hasScope(actor, ScopeFeedReadAssigned) {
		return nil, ErrDenied
	}
	out := make([]feed.Item, 0, len(items))
	for _, item := range items {
		ok, err := s.feedItemVisible(ctx, actor, item)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *Service) FilterFeedPage(ctx context.Context, actor domain.Token, page feed.Page) (feed.Page, error) {
	items, err := s.FilterFeedItems(ctx, actor, page.Items)
	if err != nil {
		return feed.Page{}, err
	}
	page.Items = items
	return page, nil
}

func (s *Service) feedItemVisible(ctx context.Context, actor domain.Token, item feed.Item) (bool, error) {
	if item.SubjectKind != domain.SubjectWorkItem {
		return false, nil
	}
	if item.Kind == domain.EventWorkItemRelationAdded {
		ids := relationIDs(item)
		for _, id := range ids {
			ok, err := s.workItemInAnyTree(ctx, actor, id)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	return s.workItemInAnyTree(ctx, actor, item.SubjectID)
}

func relationIDs(item feed.Item) []uuid.UUID {
	ids := []uuid.UUID{item.SubjectID}
	var payload struct {
		ParentID uuid.UUID `json:"parent_id"`
		ChildID  uuid.UUID `json:"child_id"`
	}
	if err := json.Unmarshal(item.Payload, &payload); err == nil {
		if payload.ParentID != uuid.Nil {
			ids = append(ids, payload.ParentID)
		}
		if payload.ChildID != uuid.Nil {
			ids = append(ids, payload.ChildID)
		}
	}
	return ids
}

func (s *Service) workItemInAnyTree(ctx context.Context, actor domain.Token, id uuid.UUID) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("access: work_item tree policy requires database")
	}
	roots := workItemTreeRoots(actor)
	if len(roots) == 0 {
		return false, nil
	}
	for _, root := range roots {
		ok, err := s.workItemInTree(ctx, root, id)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) workItemInTree(ctx context.Context, root, target uuid.UUID) (bool, error) {
	row := s.pool.QueryRow(ctx, `
		WITH RECURSIVE subtree(id) AS (
			SELECT $1::uuid
			UNION
			SELECT wir.child_id
			FROM work_item_relations wir
			JOIN subtree s ON wir.parent_id = s.id
		)
		SELECT EXISTS (SELECT 1 FROM subtree WHERE id = $2)
	`, root, target)
	var ok bool
	if err := row.Scan(&ok); err != nil {
		return false, err
	}
	return ok, nil
}

func workItemTreeRoots(actor domain.Token) []uuid.UUID {
	var roots []uuid.UUID
	for _, scope := range actor.Scopes {
		raw := strings.TrimSpace(scope)
		if !strings.HasPrefix(raw, scopeWorkItemsTreePrefix) {
			continue
		}
		id, err := uuid.Parse(strings.TrimPrefix(raw, scopeWorkItemsTreePrefix))
		if err == nil && id != uuid.Nil {
			roots = append(roots, id)
		}
	}
	return roots
}

func legacyUnscoped(actor domain.Token) bool {
	for _, scope := range actor.Scopes {
		if strings.TrimSpace(scope) != "" {
			return false
		}
	}
	return true
}

func hasWorkItemTreeScope(actor domain.Token) bool {
	return len(workItemTreeRoots(actor)) > 0
}

func canReadWorkItems(scopes map[string]bool) bool {
	return scopes[ScopeWorkItemsRead] || scopes[ScopeWorkItemsReadAll] || scopes[ScopeWorkItemsWrite] || scopes[ScopeWorkItemsWriteAll]
}

func canWriteWorkItems(scopes map[string]bool) bool {
	return scopes[ScopeWorkItemsWrite] || scopes[ScopeWorkItemsWriteAll]
}

func hasScope(actor domain.Token, scope string) bool {
	for _, candidate := range actor.Scopes {
		if strings.TrimSpace(candidate) == scope {
			return true
		}
	}
	return false
}

func hasAny(scopes map[string]bool, candidates ...string) bool {
	for _, candidate := range candidates {
		if scopes[candidate] {
			return true
		}
	}
	return false
}

func scopeSet(scopes []string) map[string]bool {
	out := make(map[string]bool, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope != "" {
			out[scope] = true
		}
	}
	return out
}
