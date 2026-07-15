package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
)

// HTTPToolProfile is a fail-closed provider-facing tool policy. The allowed
// set controls both tools/list and tools/call; validateCall can further narrow
// mutation arguments before any handler, event append, queue write, or outbox
// write is reachable.
type HTTPToolProfile struct {
	name         string
	allowedTools map[string]bool
	validateCall func(tool string, arguments json.RawMessage) error
}

// ProviderSafeReadHTTPProfile exposes only tracker-shaped reads. feed.read is
// safe here because the provider HTTP context selects provider_safe_feed.v1;
// registry, approval, policy, inbox, connector, convergence, and
// deterministic-error surfaces remain excluded.
func ProviderSafeReadHTTPProfile() *HTTPToolProfile {
	return &HTTPToolProfile{
		name: "provider-safe-read",
		allowedTools: toolSet(
			"feed.read",
			"backlog.readiness",
			"work_items.list",
			"work_items.get",
		),
	}
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
		return o.Profile.allowedTools, true
	}
	if o.AllowedTools != nil {
		return o.AllowedTools, true
	}
	return nil, false
}

func (p HTTPToolProfile) Name() string { return p.name }

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
