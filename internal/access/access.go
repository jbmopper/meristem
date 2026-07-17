// Package access contains deterministic read/action policy reducers.
//
// The reducers operate over canonical projections (tokens, work_items,
// relations, events) and return allow/deny or filtered read models. They are
// transport-independent: MCP, REST, CLI, and future provider context exports
// should call here instead of each inventing local visibility rules.
package access

import (
	"context"
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
	// Tracker-write scopes grant only the four ordinary coordination
	// mutations (spawn child, append event, update metadata, transition).
	// They deliberately do not imply approvals, connectors, convergence,
	// registry, policy, or execution authority.
	ScopeWorkItemsTrackerWrite    = "work_items.tracker_write"
	ScopeWorkItemsTrackerWriteAll = "work_items.tracker_write_all"
	ScopeWorkItemsCreate          = "work_items.create"
	ScopeFeedRead                 = "feed.read"
	ScopeFeedReadAssigned         = "feed.read_assigned"
	ScopeInboxCapture             = "inbox.capture"
	ScopePolicyProfileSwitch      = "policy_profile.switch"
	ScopeRegistryWrite            = "registry.write"
	ScopeApprovalsDecide          = "approvals.decide"
	// OAuth-client administration is deliberately separate from token
	// administration. The root credential only mints and revokes tokens; a
	// scoped, non-root human client reviews provider metadata and binds or
	// revokes public OAuth clients.
	ScopeOAuthClientsBind   = "oauth_clients.bind"
	ScopeOAuthClientsRevoke = "oauth_clients.revoke"

	scopeWorkItemsTreePrefix = "work_items.tree:"
)

var ErrDenied = errors.New("access: denied")

// CanBindOAuthClient and CanRevokeOAuthClient are intentionally strict: the
// legacy unscoped-token compatibility path does not apply to provider client
// administration. These operations require an explicitly scoped human
// credential and reject the root credential and all agent/system actors.
func CanBindOAuthClient(actor domain.Token) bool {
	return canAdminOAuthClient(actor, ScopeOAuthClientsBind)
}

func CanRevokeOAuthClient(actor domain.Token) bool {
	return canAdminOAuthClient(actor, ScopeOAuthClientsRevoke)
}

func canAdminOAuthClient(actor domain.Token, scope string) bool {
	return actor.ID != uuid.Nil && !actor.IsRoot && actor.RevokedAt == nil && actor.Source == domain.SourceHuman && hasScope(actor, scope)
}

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
	if canonicalTool == "oauth_clients.bind_actor" {
		return CanBindOAuthClient(actor)
	}
	// A grant is a strict subset of its client's authority, so grant revocation
	// reuses the client revoke scope rather than provisioning a new one.
	if canonicalTool == "oauth_clients.revoke" || canonicalTool == "oauth_grants.revoke" {
		return CanRevokeOAuthClient(actor)
	}
	if canonicalTool == "policy_profile.switch" {
		// Evaluated before the root/legacy shortcut: the switch tool is
		// human-and-non-root regardless of scope breadth.
		if actor.Source != domain.SourceHuman || actor.IsRoot {
			return false
		}
		if legacyUnscoped(actor) {
			return true
		}
		return hasScope(actor, ScopePolicyProfileSwitch)
	}
	if canonicalTool == "registry.define_tropism" || canonicalTool == "registry.define_cultivar" || canonicalTool == "projections.define" {
		if actor.IsRoot {
			return false
		}
		if legacyUnscoped(actor) {
			return true
		}
		return hasScope(actor, ScopeRegistryWrite)
	}
	if canonicalTool == "approvals.decide" {
		if actor.Source != domain.SourceHuman || actor.IsRoot {
			return false
		}
		if legacyUnscoped(actor) {
			return true
		}
		return hasScope(actor, ScopeApprovalsDecide)
	}
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
	case "backlog.readiness":
		return canReadWorkItems(scopes) && (scopes[ScopeWorkItemsReadAll] || scopes[ScopeWorkItemsWriteAll] || hasWorkItemTreeScope(actor))
	case "registry.list", "registry.get", "projections.list", "projections.get":
		return canReadWorkItems(scopes) && (scopes[ScopeWorkItemsReadAll] || scopes[ScopeWorkItemsWriteAll] || hasWorkItemTreeScope(actor))
	case "work_items.list", "work_items.get":
		return canReadWorkItems(scopes) && (hasPortfolioWorkItemAccess(scopes) || hasWorkItemTreeScope(actor))
	case "approvals.get", "approvals.list_for_work_item":
		return canReadWorkItems(scopes) && (scopes[ScopeWorkItemsReadAll] || scopes[ScopeWorkItemsWriteAll] || hasWorkItemTreeScope(actor))
	case "work_items.create":
		return scopes[ScopeWorkItemsCreate] || scopes[ScopeWorkItemsWriteAll] || scopes[ScopeWorkItemsTrackerWriteAll]
	case "work_items.spawn_child", "work_items.append_event", "work_items.update_metadata", "work_items.transition":
		return scopes[ScopeWorkItemsWriteAll] || scopes[ScopeWorkItemsTrackerWriteAll] ||
			((scopes[ScopeWorkItemsWrite] || scopes[ScopeWorkItemsTrackerWrite]) && hasWorkItemTreeScope(actor))
	case "convergence.propose_checks", "registry.activate_cultivar", "approvals.request", "connectors.http_request":
		return scopes[ScopeWorkItemsWriteAll] || (scopes[ScopeWorkItemsWrite] && hasWorkItemTreeScope(actor))
	default:
		return false
	}
}

func RequiresScopedPolicy(actor domain.Token) bool {
	return !actor.IsRoot && !legacyUnscoped(actor)
}

// CanReadAssignedFeed authorizes the reducing assigned/addressed preset. A
// full-feed reader may always request a narrower view; an assigned-only reader
// must retain the preexisting tree scope requirement.
func CanReadAssignedFeed(actor domain.Token) bool {
	if actor.IsRoot || legacyUnscoped(actor) {
		return true
	}
	scopes := scopeSet(actor.Scopes)
	return scopes[ScopeFeedRead] || (scopes[ScopeFeedReadAssigned] && hasWorkItemTreeScope(actor))
}

// RequiresAssignedFeed reports whether the assigned/addressed preset is the
// actor's only feed authority. REST uses this to fail closed by normalization:
// omitting scope=assigned cannot expose the legacy broad feed.
func RequiresAssignedFeed(actor domain.Token) bool {
	if actor.IsRoot || legacyUnscoped(actor) {
		return false
	}
	scopes := scopeSet(actor.Scopes)
	return !scopes[ScopeFeedRead] && scopes[ScopeFeedReadAssigned]
}

func (s *Service) FilterWorkItems(ctx context.Context, actor domain.Token, items []domain.WorkItem) ([]domain.WorkItem, error) {
	if actor.IsRoot || legacyUnscoped(actor) || hasPortfolioWorkItemAccess(scopeSet(actor.Scopes)) {
		return items, nil
	}
	if !canReadWorkItems(scopeSet(actor.Scopes)) {
		return nil, ErrDenied
	}
	candidates := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		candidates = append(candidates, item.ID)
	}
	visible, err := s.workItemsInAnyTree(ctx, actor, candidates)
	if err != nil {
		return nil, err
	}
	out := make([]domain.WorkItem, 0, len(items))
	for _, item := range items {
		if visible[item.ID] {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *Service) CanReadWorkItem(ctx context.Context, actor domain.Token, id uuid.UUID) error {
	if actor.IsRoot || legacyUnscoped(actor) || hasPortfolioWorkItemAccess(scopeSet(actor.Scopes)) {
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
	if actor.IsRoot || legacyUnscoped(actor) || hasScope(actor, ScopeWorkItemsCreate) || hasScope(actor, ScopeWorkItemsWriteAll) || hasScope(actor, ScopeWorkItemsTrackerWriteAll) {
		return nil
	}
	return ErrDenied
}

func (s *Service) CanWriteWorkItem(ctx context.Context, actor domain.Token, id uuid.UUID) error {
	if actor.IsRoot || legacyUnscoped(actor) || hasScope(actor, ScopeWorkItemsWriteAll) || hasScope(actor, ScopeWorkItemsTrackerWriteAll) {
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
	// Gather every work_item id any feed item's visibility can hinge on,
	// resolve tree membership for all of them in one query, then filter in
	// memory. An item is visible when any of its anchors is in-tree;
	// anchor-less items are dropped from tree-scoped feeds.
	anchorsByIndex := make([][]uuid.UUID, len(items))
	var candidates []uuid.UUID
	for i, item := range items {
		anchorsByIndex[i] = feed.WorkItemAnchors(item)
		candidates = append(candidates, anchorsByIndex[i]...)
	}
	visible, err := s.workItemsInAnyTree(ctx, actor, candidates)
	if err != nil {
		return nil, err
	}
	out := make([]feed.Item, 0, len(items))
	for i, item := range items {
		for _, id := range anchorsByIndex[i] {
			if visible[id] {
				out = append(out, item)
				break
			}
		}
	}
	return out, nil
}

// feedItemAnchors maps one feed item to the work_item ids its tree-scoped
// visibility hinges on. This is the single place that knows how each
// feed-included event kind relates to a work_item; feed.IncludedKinds and
// this mapping must stay in sync (enforced by TestFeedItemAnchorsCoverIncludedKinds).
//
//   - work_item-subject events anchor on their subject; relation events
//     anchor on both endpoints so either side's tree sees the edge.
//     convergence.checks_proposed is first-class, but still subject-anchored
//     on the parent work_item whose checks are being proposed.
//   - convergence.verdict_recorded uses subject_kind "convergence" but its
//     subject_id *is* the judged work_item (see convergence.VerdictEventSpec).
//   - message.captured, signal.received, escalation.requested, and the
//     approval.*, subactor_grant.*, and cultivar_activation.* families
//     anchor through the work_item_id (and, where present, human_work_item_id)
//     fields their writers put in the payload.
//   - policy_profile.switched has no work-item anchor on purpose: profile
//     switches are system-wide owner posture, not tree content. Tree-scoped
//     workers learn the active envelope from /readyz (or their launcher);
//     feed.read holders see switches on the unscoped feed.
//   - tropism.defined, cultivar.defined, and projection.defined are global
//     registry writes, not tree content. Tree-scoped feeds drop them; registry
//     and projection read tools expose the current projections directly.
//   - deterministic_error.* events return no anchor on purpose: they are
//     governed by logs.* scopes, and a tree-scoped feed deliberately drops
//     them rather than inventing a work_item relationship they do not have.
func feedItemAnchors(item feed.Item) []uuid.UUID {
	return feed.WorkItemAnchors(item)
}

func (s *Service) FilterFeedPage(ctx context.Context, actor domain.Token, page feed.Page) (feed.Page, error) {
	items, err := s.FilterFeedItems(ctx, actor, page.Items)
	if err != nil {
		return feed.Page{}, err
	}
	page.Items = items
	return page, nil
}

func (s *Service) workItemInAnyTree(ctx context.Context, actor domain.Token, id uuid.UUID) (bool, error) {
	visible, err := s.workItemsInAnyTree(ctx, actor, []uuid.UUID{id})
	if err != nil {
		return false, err
	}
	return visible[id], nil
}

// workItemsInAnyTree resolves tree membership for every candidate id in one
// recursive walk over all the actor's tree roots, rather than one query per
// (candidate, root) pair. Filtering a page of N items costs one round trip
// regardless of N or the number of tree scopes.
func (s *Service) workItemsInAnyTree(ctx context.Context, actor domain.Token, ids []uuid.UUID) (map[uuid.UUID]bool, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("access: work_item tree policy requires database")
	}
	roots := workItemTreeRoots(actor)
	visible := make(map[uuid.UUID]bool, len(ids))
	if len(roots) == 0 || len(ids) == 0 {
		return visible, nil
	}
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE subtree(id) AS (
			SELECT unnest($1::uuid[])
			UNION
			SELECT wir.child_id
			FROM work_item_relations wir
			JOIN subtree s ON wir.parent_id = s.id
		)
		SELECT DISTINCT id FROM subtree WHERE id = ANY($2::uuid[])
	`, roots, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		visible[id] = true
	}
	return visible, rows.Err()
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
	return scopes[ScopeWorkItemsRead] || scopes[ScopeWorkItemsReadAll] || scopes[ScopeWorkItemsWrite] || scopes[ScopeWorkItemsWriteAll] ||
		scopes[ScopeWorkItemsTrackerWrite] || scopes[ScopeWorkItemsTrackerWriteAll]
}

func canWriteWorkItems(scopes map[string]bool) bool {
	return scopes[ScopeWorkItemsWrite] || scopes[ScopeWorkItemsWriteAll] ||
		scopes[ScopeWorkItemsTrackerWrite] || scopes[ScopeWorkItemsTrackerWriteAll]
}

func hasPortfolioWorkItemAccess(scopes map[string]bool) bool {
	return scopes[ScopeWorkItemsReadAll] || scopes[ScopeWorkItemsWriteAll] || scopes[ScopeWorkItemsTrackerWriteAll]
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
