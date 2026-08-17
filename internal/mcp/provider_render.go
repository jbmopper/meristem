package mcp

import (
	"context"
	"fmt"

	"github.com/jbmopper/meristem/internal/backlog"
	"github.com/jbmopper/meristem/internal/feed"
)

// providerSafeRenderer reduces one tool's ordinary handler payload to its
// registered provider-safe response shape. Renderers are keyed by canonical
// tool name in providerSafeRenderers and applied centrally by renderProviderSafe
// at the HTTP provider boundary, so provider-facing reduction is a structural
// property of the tool registry rather than a check each handler has to
// remember to perform.
type providerSafeRenderer func(tool string, payload any) (any, error)

// providerSafeRenderers is the fail-closed provider response contract. A tool
// may be advertised or executed on a provider-facing HTTP profile only if it
// appears here (checkHTTPToolAllowed and handleListToolsFiltered enforce it),
// and renderProviderSafe rewrites every provider-facing response through the
// registered entry. A tool missing from this map fails closed rather than
// serializing its ordinary operator DTO. Keep this in lockstep with the
// provider profiles in http_profile.go: every tool ProviderSafeReadHTTPProfile
// or ProviderTrackerHTTPProfile allowlists needs an entry here.
var providerSafeRenderers = map[string]providerSafeRenderer{
	"feed.read":                  renderProviderSafeFeed,
	"backlog.readiness":          renderProviderSafeReadiness,
	"work_items.list":            renderProviderSafeWorkItemList,
	"work_items.get":             renderProviderSafeWorkItemGet,
	"work_items.create":          renderProviderSafeWorkItemMutation,
	"work_items.spawn_child":     renderProviderSafeWorkItemMutation,
	"work_items.update_metadata": renderProviderSafeWorkItemMutation,
	"work_items.transition":      renderProviderSafeWorkItemMutation,
	"work_items.append_event":    renderProviderSafeAck,
}

// hasProviderSafeRenderer reports whether tool has a registered provider-safe
// renderer. The HTTP boundary uses it to fail closed: a provider-facing profile
// cannot advertise or call a tool that lacks one.
func hasProviderSafeRenderer(tool string) bool {
	_, ok := providerSafeRenderers[tool]
	return ok
}

// renderProviderSafe applies the registered provider-safe renderer to a tool's
// result. Outside the provider HTTP context it returns the payload unchanged, so
// stdio and unrestricted in-process callers keep the ordinary operator DTOs.
// Inside it, a tool with no registered renderer fails closed instead of
// returning its ordinary payload. checkHTTPToolAllowed already rejects such a
// call before dispatch; this is the response-side seam that keeps the guarantee
// true regardless of which handler produced the payload.
func renderProviderSafe(ctx context.Context, tool string, payload any) (any, error) {
	if !isProviderSafeContext(ctx) {
		return payload, nil
	}
	renderer, ok := providerSafeRenderers[tool]
	if !ok {
		return nil, fmt.Errorf("provider_safe_render_unregistered: %s has no provider-safe response renderer", tool)
	}
	return renderer(tool, payload)
}

func renderProviderSafeFeed(tool string, payload any) (any, error) {
	fields, err := providerSafePayloadFields(tool, payload)
	if err != nil {
		return nil, err
	}
	items, ok := fields["items"].([]feed.Item)
	if !ok {
		return nil, providerSafeRenderMismatch(tool, "items")
	}
	fields["items"] = feed.ProjectProviderSafeItems(items)
	fields["contract"] = feed.ProviderSafeContract
	return fields, nil
}

func renderProviderSafeReadiness(tool string, payload any) (any, error) {
	summary, ok := payload.(backlog.Summary)
	if !ok {
		return nil, providerSafeRenderMismatch(tool, "summary")
	}
	return providerSafeReadinessSummary(summary), nil
}

func renderProviderSafeWorkItemList(tool string, payload any) (any, error) {
	fields, err := providerSafePayloadFields(tool, payload)
	if err != nil {
		return nil, err
	}
	items, ok := fields["items"].([]workItemDTO)
	if !ok {
		return nil, providerSafeRenderMismatch(tool, "items")
	}
	safe := make([]providerSafeWorkItemDTO, 0, len(items))
	for _, item := range items {
		safe = append(safe, reduceWorkItemDTO(item))
	}
	return map[string]any{
		"contract": ProviderSafeWorkItemsContract,
		"items":    safe,
	}, nil
}

func renderProviderSafeWorkItemGet(tool string, payload any) (any, error) {
	fields, err := providerSafePayloadFields(tool, payload)
	if err != nil {
		return nil, err
	}
	item, ok := fields["work_item"].(workItemDTO)
	if !ok {
		return nil, providerSafeRenderMismatch(tool, "work_item")
	}
	return map[string]any{
		"contract":  ProviderSafeWorkItemsContract,
		"work_item": reduceWorkItemDTO(item),
	}, nil
}

// renderProviderSafeWorkItemMutation reduces the embedded work_item DTO of a
// tracker mutation response to the provider-safe shape while preserving the id
// and relation fields the caller needs to keep coordinating. Without it,
// create, spawn_child, update_metadata, and transition would echo the ordinary
// DTO's state_reason and created_by back across the provider boundary.
func renderProviderSafeWorkItemMutation(tool string, payload any) (any, error) {
	fields, err := providerSafePayloadFields(tool, payload)
	if err != nil {
		return nil, err
	}
	raw, ok := fields["work_item"]
	if !ok {
		return nil, providerSafeRenderMismatch(tool, "work_item")
	}
	item, ok := raw.(workItemDTO)
	if !ok {
		return nil, providerSafeRenderMismatch(tool, "work_item")
	}
	fields["work_item"] = reduceWorkItemDTO(item)
	return fields, nil
}

// renderProviderSafeAck passes through a response that carries only ids and
// acknowledgement flags (work_items.append_event). It is registered so the tool
// is allowlist-eligible; the guard keeps it honest if the handler ever starts
// returning richer content that would need its own reducer.
func renderProviderSafeAck(tool string, payload any) (any, error) {
	fields, err := providerSafePayloadFields(tool, payload)
	if err != nil {
		return nil, err
	}
	if _, ok := fields["work_item"]; ok {
		return nil, providerSafeRenderMismatch(tool, "work_item")
	}
	return fields, nil
}

// reduceWorkItemDTO drops the free-form state reason and creator token id from
// an ordinary work-item DTO, yielding the provider_safe_work_items.v1 shape.
// It is the DTO-level twin of the domain-level omission in the safe DTO builder.
func reduceWorkItemDTO(item workItemDTO) providerSafeWorkItemDTO {
	return providerSafeWorkItemDTO{
		ID:                         item.ID,
		Title:                      item.Title,
		Body:                       item.Body,
		State:                      item.State,
		SuggestedConvergenceChecks: item.SuggestedConvergenceChecks,
		HumanReviewStatus:          item.HumanReviewStatus,
		CreatedAt:                  item.CreatedAt,
		StateEnteredAt:             item.StateEnteredAt,
		UpdatedAt:                  item.UpdatedAt,
	}
}

// providerSafeReadinessSummary strips the free-form state reason from every
// classified item. backlog.readiness emits backlog.Item rather than the
// provider-safe work-item DTO, so the state_reason omission the other read
// tools get from reduceWorkItemDTO must be applied here explicitly.
func providerSafeReadinessSummary(summary backlog.Summary) backlog.Summary {
	for _, group := range []*[]backlog.Item{
		&summary.Groups.V1Substrate,
		&summary.Groups.ReadyNext,
		&summary.Groups.Blockers,
		&summary.Groups.Running,
		&summary.Groups.StaleNoise,
	} {
		for i := range *group {
			(*group)[i].StateReason = nil
		}
	}
	return summary
}

// providerSafePayloadFields returns a shallow copy of a map-shaped handler
// payload so a renderer can rewrite fields without mutating the value the
// handler returned. A non-map payload means the handler's shape drifted from
// what its renderer expects, which fails closed.
func providerSafePayloadFields(tool string, payload any) (map[string]any, error) {
	fields, ok := payload.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("provider_safe_render_shape: %s returned an unexpected response shape", tool)
	}
	out := make(map[string]any, len(fields)+1)
	for name, value := range fields {
		out[name] = value
	}
	return out, nil
}

func providerSafeRenderMismatch(tool, field string) error {
	return fmt.Errorf("provider_safe_render_shape: %s response field %q is not provider-safe renderable", tool, field)
}
