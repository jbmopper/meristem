package mcp

import (
	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/backlog"
	"github.com/jbmopper/meristem/internal/domain"
	"github.com/jbmopper/meristem/internal/feed"
)

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

// providerSafeRenderer is the per-tool provider-safe rendering contract. Exactly
// one of render or noReductionJustification is set.
//
//   - render reduces the handler's typed carrier into the provider-safe wire
//     shape. It must fail closed (return an error) on any unexpected carrier
//     type.
//   - noReductionJustification marks a tool whose ordinary response carries no
//     private DTO fields, so its handler result is emitted unchanged. This is a
//     deliberate, reviewed exception and is documented per tool; a canary test
//     guards each such response against regressions.
type providerSafeRenderer struct {
	render                   func(result any) (any, error)
	noReductionJustification string
}

// providerSafeRenderable reports whether a renderer transforms the result (true)
// or passes it through under a documented no-reduction exception (false).
func (r providerSafeRenderer) providerSafeRenderable() bool {
	return r.render != nil
}

// apply runs the renderer, failing closed on an unexpected carrier type.
func (r providerSafeRenderer) apply(result any) (any, error) {
	if r.render == nil {
		return result, nil
	}
	return r.render(result)
}

// providerSafeRenderers maps canonical tool names to their provider-safe
// rendering contract. Every tool in ProviderSafeReadHTTPProfile and
// ProviderTrackerHTTPProfile (and ReadOnlyHTTPTools) must appear here; a
// provider-safe-guarding table test enforces that.
var providerSafeRenderers = map[string]providerSafeRenderer{
	"feed.read":         {render: renderProviderSafeFeed},
	"backlog.readiness": {render: renderProviderSafeReadiness},
	"work_items.list":   {render: renderProviderSafeWorkItemList},
	"work_items.get":    {render: renderProviderSafeWorkItem},
	// Tracker mutations echo the item as the caller just wrote it. Even so, the
	// ordinary work-item DTO carries created_by and the free-form state_reason,
	// which a provider must never receive, so create/spawn_child/update_metadata/
	// transition are reduced to provider_safe_work_items.v1 like the reads.
	"work_items.create":          {render: renderProviderSafeWorkItem},
	"work_items.spawn_child":     {render: renderProviderSafeWorkItem},
	"work_items.update_metadata": {render: renderProviderSafeWorkItem},
	"work_items.transition":      {render: renderProviderSafeWorkItem},
	// work_items.append_event returns only {work_item_id, appended}. It joins no
	// DTO and echoes no free-form field, so it needs no reduction. The
	// TestProviderSafeAppendEventResponseCarriesNoDTO canary keeps that true.
	"work_items.append_event": {noReductionJustification: "acknowledgement echoes only work_item_id and a boolean; no DTO or free-form field is serialized"},
}

// providerSafeRendererFor looks up the renderer for a canonical tool name.
func providerSafeRendererFor(tool string) (providerSafeRenderer, bool) {
	renderer, ok := providerSafeRenderers[tool]
	return renderer, ok
}

// providerSafeRenderRegistered reports whether the tool has any provider-safe
// rendering contract. checkHTTPToolAllowed uses this as the fail-closed
// pre-dispatch gate: a provider-allowed tool with no entry is rejected before
// its handler can run.
func providerSafeRenderRegistered(tool string) bool {
	_, ok := providerSafeRenderers[tool]
	return ok
}

// renderProviderSafeResult reduces a provider-safe handler result for the given
// canonical tool. An unregistered tool or an unexpected carrier fails closed.
func renderProviderSafeResult(tool string, result any) (any, *rpcError) {
	renderer, ok := providerSafeRendererFor(tool)
	if !ok {
		return nil, rpcErrorf(errCodeMethodNotFound, "provider-safe response rendering not registered for tool: "+tool)
	}
	rendered, err := renderer.apply(result)
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

type providerSafeReadinessResult struct {
	summary backlog.Summary
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

func renderProviderSafeReadiness(result any) (any, error) {
	r, ok := result.(providerSafeReadinessResult)
	if !ok {
		return nil, providerSafeRenderMismatch("backlog.readiness", result)
	}
	return providerSafeReadinessSummary(r.summary), nil
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
