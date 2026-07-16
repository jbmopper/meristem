package mcp

import (
	"time"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/backlog"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
)

// providerSafeIdempotencyContract versions the response-data contract folded
// into provider-facing mutation request hashes. Bump this only when a cached
// provider mutation response from the previous version is no longer safe to
// replay verbatim. The token/scope/key identity remains unchanged, so a legacy
// row produces an idempotency conflict rather than re-running the mutation.
const providerSafeIdempotencyContract = "provider_safe_mcp_response.v1"

// Provider-safe response rendering is a fail-closed contract enforced at the
// HTTP profile/tool boundary rather than inside each handler.
//
// The security problem this solves: a provider-facing tools/call (opts.Profile
// != nil or opts.AllowedTools != nil) must never serialize an ordinary operator
// DTO. Historically each handler opted in to a reduced shape by branching on
// isProviderSafeContext. That is fail-open: a future tool added to a provider
// profile that forgets the branch leaks the raw event/work-item DTO.
//
// The structural fix: every tool reachable on a provider profile MUST have a
// registered renderer here, keyed on the canonical tool name. Under a
// provider-safe context the boundary hands the handler's typed carrier to that
// renderer, and the renderer alone produces the wire shape. The handlers no
// longer build the reduced shape; they tag their raw domain data with a carrier
// type. Two failure modes both fail CLOSED:
//
//   - A tool allowed on a provider profile with no registered renderer is
//     rejected before dispatch (checkHTTPToolAllowed), returning a deterministic
//     error and no body instead of falling through to the raw handler result.
//   - A registered renderer that receives an unexpected carrier type (a handler
//     that forgot to tag its provider-safe result) errors rather than emitting
//     the value, so an ordinary DTO can never reach the wire.
//
// This is deliberately not free-form JSON key redaction. Each renderer is typed
// to its tool's carrier and rebuilds an explicit allowlisted shape.

// providerSafeRenderer is the per-tool provider-safe rendering contract. Every
// reachable tool has a non-nil renderer and a typed carrier; there are no raw
// pass-through exceptions. An unexpected carrier must fail closed.
type providerSafeRenderer func(result any) (any, error)

// providerSafeRenderers maps canonical tool names to their provider-safe
// rendering contract. Every tool in ProviderSafeReadHTTPProfile and
// ProviderTrackerHTTPProfile (and ReadOnlyHTTPTools) must appear here; a
// provider-safe-guarding table test enforces that.
var providerSafeRenderers = map[string]providerSafeRenderer{
	"feed.read":         renderProviderSafeFeed,
	"backlog.readiness": renderProviderSafeReadiness,
	"work_items.list":   renderProviderSafeWorkItemList,
	"work_items.get":    renderProviderSafeWorkItem,
	// Tracker mutations echo the item as the caller just wrote it. Even so, the
	// ordinary work-item DTO carries created_by and the free-form state_reason,
	// which a provider must never receive, so create/spawn_child/update_metadata/
	// transition are reduced to provider_safe_work_items.v1 like the reads.
	"work_items.create":          renderProviderSafeWorkItem,
	"work_items.spawn_child":     renderProviderSafeWorkItem,
	"work_items.update_metadata": renderProviderSafeWorkItem,
	"work_items.transition":      renderProviderSafeWorkItem,
	"work_items.append_event":    renderProviderSafeAppendEvent,
}

// providerSafeRendererFor looks up the renderer for a canonical tool name.
func providerSafeRendererFor(tool string) (providerSafeRenderer, bool) {
	renderer, ok := providerSafeRenderers[tool]
	return renderer, ok && renderer != nil
}

// providerSafeRenderRegistered reports whether the tool has any provider-safe
// rendering contract. checkHTTPToolAllowed uses this as the fail-closed
// pre-dispatch gate: a provider-allowed tool with no entry is rejected before
// its handler can run.
func providerSafeRenderRegistered(tool string) bool {
	renderer, ok := providerSafeRenderers[tool]
	if renderer == nil {
		return false
	}
	return ok
}

// renderProviderSafeResult reduces a provider-safe handler result for the given
// canonical tool. An unregistered tool or an unexpected carrier fails closed.
func renderProviderSafeResult(tool string, result any) (any, *rpcError) {
	renderer, ok := providerSafeRendererFor(tool)
	if !ok {
		return nil, rpcErrorf(errCodeMethodNotFound, "provider-safe response rendering not registered for tool: "+tool)
	}
	rendered, err := renderer(result)
	if err != nil {
		return nil, rpcErrorf(errCodeInternal, err.Error())
	}
	return rendered, nil
}

// Carrier types tag a handler's raw, unreduced provider-safe result. They hold
// domain data only; the renderer above owns the reduction. Because they are
// distinct types, a handler that forgets to tag its provider-safe result (and
// returns an ordinary DTO map instead) trips the renderer's type check and fails
// closed.

type providerSafeFeedSnapshot struct {
	items []feed.Item
}

type providerSafeFeedPage struct {
	page feed.Page
}

type providerSafeWorkItemsResult struct {
	items []domain.WorkItem
}

// providerSafeWorkItemResult carries one work item plus the id echoes a
// particular read or mutation includes in its envelope.
type providerSafeWorkItemResult struct {
	item     domain.WorkItem
	echoID   bool       // create/spawn_child echo work_item_id
	parentID *uuid.UUID // spawn_child echoes parent_id
}

type providerSafeAppendEventResult struct {
	workItemID uuid.UUID
	appended   bool
}

type providerSafeReadinessResult struct {
	summary backlog.Summary
}

// These readiness DTOs deliberately mirror the provider-approved fields one by
// one. They must not embed backlog.Summary, Groups, or Item: those ordinary
// domain DTOs are designed to grow, and a new field must not silently become a
// provider-facing field merely because encoding/json discovers it.
type providerSafeReadinessDTO struct {
	Contract            string                         `json:"contract"`
	Source              string                         `json:"source"`
	Limit               int                            `json:"limit"`
	AsOf                time.Time                      `json:"as_of"`
	Totals              providerSafeReadinessTotalsDTO `json:"totals"`
	StateCounts         map[domain.WorkItemState]int   `json:"state_counts"`
	Groups              providerSafeReadinessGroupsDTO `json:"groups"`
	SpecSeedDrift       []string                       `json:"spec_seed_drift"`
	ClassificationRules []string                       `json:"classification_rules"`
}

type providerSafeReadinessTotalsDTO struct {
	Visible     int `json:"visible"`
	Terminal    int `json:"terminal"`
	NonTerminal int `json:"non_terminal"`
}

type providerSafeReadinessGroupsDTO struct {
	V1Substrate []providerSafeReadinessItemDTO `json:"v1_substrate"`
	ReadyNext   []providerSafeReadinessItemDTO `json:"ready_next"`
	Blockers    []providerSafeReadinessItemDTO `json:"blockers"`
	Running     []providerSafeReadinessItemDTO `json:"running"`
	StaleNoise  []providerSafeReadinessItemDTO `json:"stale_noise"`
}

type providerSafeReadinessItemDTO struct {
	ID                         uuid.UUID                `json:"id"`
	Title                      string                   `json:"title"`
	State                      domain.WorkItemState     `json:"state"`
	HumanReviewStatus          domain.HumanReviewStatus `json:"human_review_status"`
	SuggestedConvergenceChecks []string                 `json:"suggested_convergence_checks,omitempty"`
	StateEnteredAt             time.Time                `json:"state_entered_at"`
	UpdatedAt                  time.Time                `json:"updated_at"`
	Tags                       []string                 `json:"tags,omitempty"`
}

func renderProviderSafeFeed(result any) (any, error) {
	switch r := result.(type) {
	case providerSafeFeedSnapshot:
		return map[string]any{
			"contract": feed.ProviderSafeContract,
			"items":    feed.ProjectProviderSafeItems(r.items),
		}, nil
	case providerSafeFeedPage:
		return map[string]any{
			"contract":    feed.ProviderSafeContract,
			"items":       feed.ProjectProviderSafeItems(r.page.Items),
			"next_cursor": r.page.NextCursor,
			"has_more":    r.page.HasMore,
		}, nil
	default:
		return nil, providerSafeRenderMismatch("feed.read", result)
	}
}

func renderProviderSafeWorkItemList(result any) (any, error) {
	r, ok := result.(providerSafeWorkItemsResult)
	if !ok {
		return nil, providerSafeRenderMismatch("work_items.list", result)
	}
	safe := make([]providerSafeWorkItemDTO, 0, len(r.items))
	for _, item := range r.items {
		safe = append(safe, toProviderSafeWorkItemDTO(item))
	}
	return map[string]any{
		"contract": ProviderSafeWorkItemsContract,
		"items":    safe,
	}, nil
}

func renderProviderSafeWorkItem(result any) (any, error) {
	r, ok := result.(providerSafeWorkItemResult)
	if !ok {
		return nil, providerSafeRenderMismatch("work_items.*", result)
	}
	out := map[string]any{
		"contract":  ProviderSafeWorkItemsContract,
		"work_item": toProviderSafeWorkItemDTO(r.item),
	}
	if r.echoID {
		out["work_item_id"] = r.item.ID
	}
	if r.parentID != nil {
		out["parent_id"] = *r.parentID
	}
	return out, nil
}

func renderProviderSafeAppendEvent(result any) (any, error) {
	r, ok := result.(providerSafeAppendEventResult)
	if !ok {
		return nil, providerSafeRenderMismatch("work_items.append_event", result)
	}
	return map[string]any{
		"work_item_id": r.workItemID,
		"appended":     r.appended,
	}, nil
}

func renderProviderSafeReadiness(result any) (any, error) {
	r, ok := result.(providerSafeReadinessResult)
	if !ok {
		return nil, providerSafeRenderMismatch("backlog.readiness", result)
	}
	return toProviderSafeReadinessDTO(r.summary), nil
}

func toProviderSafeReadinessDTO(summary backlog.Summary) providerSafeReadinessDTO {
	stateCounts := make(map[domain.WorkItemState]int, len(summary.StateCounts))
	for state, count := range summary.StateCounts {
		stateCounts[state] = count
	}
	return providerSafeReadinessDTO{
		Contract: summary.Contract,
		Source:   summary.Source,
		Limit:    summary.Limit,
		AsOf:     summary.AsOf,
		Totals: providerSafeReadinessTotalsDTO{
			Visible:     summary.Totals.Visible,
			Terminal:    summary.Totals.Terminal,
			NonTerminal: summary.Totals.NonTerminal,
		},
		StateCounts: stateCounts,
		Groups: providerSafeReadinessGroupsDTO{
			V1Substrate: projectProviderSafeReadinessItems(summary.Groups.V1Substrate),
			ReadyNext:   projectProviderSafeReadinessItems(summary.Groups.ReadyNext),
			Blockers:    projectProviderSafeReadinessItems(summary.Groups.Blockers),
			Running:     projectProviderSafeReadinessItems(summary.Groups.Running),
			StaleNoise:  projectProviderSafeReadinessItems(summary.Groups.StaleNoise),
		},
		SpecSeedDrift:       append([]string(nil), summary.SpecSeedDrift...),
		ClassificationRules: append([]string(nil), summary.ClassificationRules...),
	}
}

func projectProviderSafeReadinessItems(items []backlog.Item) []providerSafeReadinessItemDTO {
	out := make([]providerSafeReadinessItemDTO, 0, len(items))
	for _, item := range items {
		out = append(out, providerSafeReadinessItemDTO{
			ID:                         item.ID,
			Title:                      item.Title,
			State:                      item.State,
			HumanReviewStatus:          item.HumanReviewStatus,
			SuggestedConvergenceChecks: append([]string(nil), item.SuggestedConvergenceChecks...),
			StateEnteredAt:             item.StateEnteredAt,
			UpdatedAt:                  item.UpdatedAt,
			Tags:                       append([]string(nil), item.Tags...),
		})
	}
	return out
}

func providerSafeRenderMismatch(tool string, result any) error {
	return &providerSafeRenderError{tool: tool, got: result}
}

type providerSafeRenderError struct {
	tool string
	got  any
}

func (e *providerSafeRenderError) Error() string {
	return "provider-safe renderer for " + e.tool + " received an unrendered result; refusing to emit an ordinary response shape"
}
