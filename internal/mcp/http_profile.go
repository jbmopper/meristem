package mcp

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/jbmopper/meristem/internal/access"
	"github.com/jbmopper/meristem/internal/domain"
)

// HTTPToolProfile separates tool admission from response reduction. Provider
// profiles enable both; the explicit local-agent profile enables neither and
// therefore retains ordinary scope-derived tools and DTOs.
type HTTPToolProfile struct {
	name                  string
	restrictTools         bool
	allowedTools          map[string]bool
	providerSafeResponses bool
	validateCall          func(tool string, arguments json.RawMessage) error
}

// ProviderSafeReadHTTPProfile exposes only tracker-shaped reads. feed.read is
// safe here because the provider HTTP context selects provider_safe_feed.v1;
// registry, approval, policy, inbox, connector, convergence, and
// deterministic-error surfaces remain excluded.
func ProviderSafeReadHTTPProfile() *HTTPToolProfile {
	return &HTTPToolProfile{
		name:                  "provider-safe-read",
		restrictTools:         true,
		providerSafeResponses: true,
		allowedTools: toolSet(
			"feed.read",
			"backlog.readiness",
			"work_items.list",
			"work_items.get",
		),
	}
}

// LocalAgentHTTPProfile identifies the ordinary local-agent presentation
// boundary. It grants no authority: access.ToolVisible and object-level
// reducers continue to enforce the token's explicit business scopes.
func LocalAgentHTTPProfile() *HTTPToolProfile {
	return &HTTPToolProfile{name: "local-agent-v1"}
}

// mcpProfileForActor maps exact provider and local markers to their shared MCP
// tool/data boundary. Doing this inside the dispatcher makes both marker kinds
// transport-independent, so a marked credential cannot regain a different
// stdio surface.
//
// A marker-bearing credential must be one non-root agent identity with exactly
// the scopes produced by access.ReduceProviderAuthority. Malformed or
// hand-expanded markers fail closed instead of falling back to ordinary token
// scope filtering.
func mcpProfileForActor(actor domain.Token) (*HTTPToolProfile, bool, error) {
	if marked, err := access.LocalAgentMCPProfileFromActor(actor); marked {
		if err != nil {
			return nil, true, err
		}
		return LocalAgentHTTPProfile(), true, nil
	}
	if !access.HasProviderAuthorityMarker(actor.Scopes) {
		return nil, false, nil
	}
	if actor.ID == uuid.Nil || actor.IsRoot || actor.Source != domain.SourceAgent || actor.RevokedAt != nil {
		return nil, true, access.ErrInvalidProviderAuthority
	}
	profile, err := access.ProviderAuthorityProfileFromScopes(actor.Scopes)
	if err != nil {
		return nil, true, err
	}
	switch profile {
	case access.ProviderOwnerTrackerReadV1, access.ProviderDelegatedTreeReadV1:
		return ProviderSafeReadHTTPProfile(), true, nil
	case access.ProviderOwnerTrackerWriteV1, access.ProviderDelegatedTreeWriteV1:
		return ProviderTrackerHTTPProfile(), true, nil
	default:
		return nil, true, access.ErrInvalidProviderAuthority
	}
}

// HTTPProfileForActor is the API route's first profile gate. The shared MCP
// dispatcher calls mcpProfileForActor independently before tools/list and
// tools/call, then requires this route-selected profile to match exactly.
func HTTPProfileForActor(actor domain.Token) (*HTTPToolProfile, error) {
	profile, marked, err := mcpProfileForActor(actor)
	if err != nil {
		return nil, err
	}
	if marked {
		return profile, nil
	}
	// Unmarked static credentials retain the historical provider-safe HTTP
	// fallback. Unmarked stdio credentials remain compatibility-unrestricted.
	return ProviderSafeReadHTTPProfile(), nil
}

// ProviderTrackerHTTPProfile adds the narrow tracker mutation surface to the
// provider-safe reads. These mutations use the ordinary MCP idempotency
// executor. Argument validation keeps this transport from granting latent job
// execution authority: new items must remain human-review-blocked and cannot
// name a cultivar, metadata writes cannot wave items through, and lifecycle
// transitions are limited to blocked or terminal states.
//
// Broader tracker transitions require a durable domain-level non-dispatchable
// marker that every worker reconciler enforces. Until that exists, widening
// this profile is unsafe even while the job executor is paused.
func ProviderTrackerHTTPProfile() *HTTPToolProfile {
	read := ProviderSafeReadHTTPProfile()
	for name := range trackerMutationHTTPTools() {
		read.allowedTools[name] = true
	}
	read.name = "provider-tracker"
	read.validateCall = validateProviderTrackerCall
	return read
}

func trackerMutationHTTPTools() map[string]bool {
	return toolSet(
		"work_items.create",
		"work_items.spawn_child",
		"work_items.append_event",
		"work_items.update_metadata",
		"work_items.transition",
	)
}

func toolSet(names ...string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, name := range names {
		out[name] = true
	}
	return out
}

// allowedTools returns the effective allowlist and whether the options
// restrict tools at all. A set Profile or a non-nil AllowedTools map is
// restricting even when its set is empty: only wholly absent options mean
// unrestricted, so an explicitly empty allowlist or a zero-value profile
// denies every tool instead of failing open.
func (o HTTPOptions) allowedTools() (map[string]bool, bool) {
	if o.Profile != nil {
		return o.Profile.allowedTools, o.Profile.restrictTools
	}
	if o.AllowedTools != nil {
		return o.AllowedTools, true
	}
	return nil, false
}

func (p HTTPToolProfile) Name() string { return p.name }

func (p *HTTPToolProfile) providerSafe() bool {
	return p != nil && p.providerSafeResponses
}

func sameHTTPProfile(a, b *HTTPToolProfile) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.name == "" || a.name != b.name || a.restrictTools != b.restrictTools ||
		a.providerSafeResponses != b.providerSafeResponses || len(a.allowedTools) != len(b.allowedTools) {
		return false
	}
	for name, allowed := range a.allowedTools {
		if b.allowedTools[name] != allowed {
			return false
		}
	}
	return true
}

func validateHTTPActorProfile(actor domain.Token, route *HTTPToolProfile) error {
	want, err := HTTPProfileForActor(actor)
	if err != nil || !sameHTTPProfile(want, route) {
		return fmt.Errorf("MCP actor profile does not match the HTTP route profile")
	}
	return nil
}

// validate applies the profile's optional per-call narrowing. Profiles whose
// allowed set is entirely read-shaped (provider-safe-read) define no
// validateCall; the allowed-set gate has already run, so a nil validator
// means "no further narrowing", never a panic.
func (p *HTTPToolProfile) validate(tool string, arguments json.RawMessage) error {
	if p == nil || p.validateCall == nil {
		return nil
	}
	return p.validateCall(tool, arguments)
}

func validateProviderTrackerCall(tool string, raw json.RawMessage) error {
	switch tool {
	case "feed.read", "backlog.readiness", "work_items.list", "work_items.get":
		return nil
	case "work_items.create", "work_items.spawn_child":
		var args struct {
			HumanReviewStatus string `json:"human_review_status"`
			Cultivar          string `json:"cultivar"`
			State             string `json:"state"`
		}
		if err := decodeHTTPProfileArgs(raw, &args); err != nil {
			return err
		}
		if args.HumanReviewStatus != "blocked" {
			return executionAuthorityDenied(tool + " requires human_review_status=blocked")
		}
		if strings.TrimSpace(args.Cultivar) != "" {
			return executionAuthorityDenied(tool + " cannot assign a cultivar")
		}
		switch args.State {
		case "", "captured", "triaged", "planned", "blocked":
			return nil
		default:
			return executionAuthorityDenied(tool + " cannot create an execution-shaped lifecycle state")
		}

	case "work_items.update_metadata":
		var args struct {
			HumanReviewStatus string `json:"human_review_status"`
		}
		if err := decodeHTTPProfileArgs(raw, &args); err != nil {
			return err
		}
		if args.HumanReviewStatus != "blocked" {
			return executionAuthorityDenied(tool + " cannot wave through or approve work")
		}
		return nil

	case "work_items.transition":
		var args struct {
			To string `json:"to"`
		}
		if err := decodeHTTPProfileArgs(raw, &args); err != nil {
			return err
		}
		switch args.To {
		case "blocked", "done", "failed", "canceled":
			return nil
		default:
			return executionAuthorityDenied(tool + " may only block or terminalize work")
		}

	case "work_items.append_event":
		var args struct {
			Kind    string                     `json:"kind"`
			Payload map[string]json.RawMessage `json:"payload"`
		}
		if err := decodeHTTPProfileArgs(raw, &args); err != nil {
			return err
		}
		switch args.Kind {
		case "provider.note":
			return validateProviderEventPayload(tool, args.Payload, map[string]bool{"note": true, "reference": true})
		case "provider.progress":
			return validateProviderEventPayload(tool, args.Payload, map[string]bool{"summary": true, "percent": true, "reference": true})
		default:
			return executionAuthorityDenied(tool + " may only append provider.note or provider.progress")
		}
	default:
		return fmt.Errorf("tool not enabled on provider-tracker HTTP MCP profile: %s", tool)
	}
}

func validateProviderEventPayload(tool string, payload map[string]json.RawMessage, allowed map[string]bool) error {
	for key := range payload {
		if !allowed[key] {
			return executionAuthorityDenied(tool + " payload field " + key + " is not tracker-safe")
		}
	}
	return nil
}

func decodeHTTPProfileArgs(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("invalid tools/call arguments: %w", err)
	}
	return nil
}

func executionAuthorityDenied(detail string) error {
	return fmt.Errorf("tracker_execution_authority_denied: %s", detail)
}

type httpToolDescriptor struct {
	toolDescriptor
	Annotations httpToolAnnotations `json:"annotations"`
}

type httpToolAnnotations struct {
	ReadOnlyHint    bool `json:"readOnlyHint"`
	DestructiveHint bool `json:"destructiveHint"`
	IdempotentHint  bool `json:"idempotentHint"`
	OpenWorldHint   bool `json:"openWorldHint"`
}

func httpAnnotationsForTool(tool Tool) httpToolAnnotations {
	destructive := false
	if tool.Name == "work_items.update_metadata" || tool.Name == "work_items.transition" {
		destructive = true
	}
	return httpToolAnnotations{
		ReadOnlyHint:    !tool.Mutates,
		DestructiveHint: destructive,
		IdempotentHint:  tool.Mutates,
		OpenWorldHint:   false,
	}
}
